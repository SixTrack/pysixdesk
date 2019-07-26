#!/usr/bin/env python3
import re
import os
import time
import gzip
import shutil
import getpass
import zipfile
import logging
import configparser
import importlib

from .pysixdb import SixDB
from . import utils
from .resultparser import parse_preprocess, parse_sixtrack

logger = logging.getLogger(__name__)


def run(wu_id, infile):
    cf = configparser.ConfigParser()
    if os.path.isfile(infile):
        cf.read(infile)
        info_sec = cf['info']
        boinc = 'false'
        if str(wu_id) == '1':
            boinc = info_sec['boinc']

        db_info = cf['db_info']
        dbtype = db_info['db_type']
        if dbtype.lower() == 'mysql' and str(boinc).lower() == 'false':
            content = "No need to gather results manually with MySQL!"
            logger.info(content)
            return

        cluster_module = info_sec['cluster_module']  # pysixtrack.submission
        classname = info_sec['cluster_name']  # HTCondor
        try:
            module = importlib.import_module(cluster_module)
            cluster_cls = getattr(module, classname)
            cluster = cluster_cls()
        except ModuleNotFoundError as e:
            content = "Failed to instantiate cluster class %s!" % cluster_module
            logger.error(content)
            raise e
        if str(wu_id) == '0':
            preprocess_results(cf, cluster)
        elif str(wu_id) == '1':
            sixtrack_results(cf, cluster)
        else:
            content = "Unknown task!"
            logger.error(content)
    else:
        content = "The input file %s doesn't exist!" % infile
        logger.error(content)


def preprocess_results(cf, cluster):
    '''Gather the results of madx and oneturn sixtrack jobs and store in
    database
    '''
    info_sec = cf['info']

    preprocess_path = info_sec['path']
    if not os.path.isdir(preprocess_path) or not os.listdir(preprocess_path):
        content = "There isn't result in path %s!" % preprocess_path
        logger.warning(content)
        return
    # contents = os.listdir(preprocess_path)
    set_sec = cf['db_setting']
    db_info = cf['db_info']
    oneturn = cf['oneturn']
    db = SixDB(db_info, settings=set_sec, create=False)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    where = "status='submitted'"
    job_ids = db.select('preprocess_wu', ['wu_id', 'unique_id'], where)
    job_ids = [(str(j), str(i)) for i, j in job_ids]
    job_index = dict(job_ids)
    studypath = os.path.dirname(preprocess_path)
    unfin = cluster.check_running(studypath)
    running_jobs = [job_index.pop(unid) for unid in unfin]
    if running_jobs:
        content = "The preprocess jobs %s aren't completed yet!" % str(running_jobs)
        logger.warning(content)

    for item in os.listdir(preprocess_path):
        if item not in job_index.values():
            continue
        job_path = os.path.join(preprocess_path, item)
        if not os.listdir(job_path):
            content = "The preprocess job %s is empty!" % item
            logger.warning(content)
            continue
        job_table = {}
        task_table = {}
        oneturn_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            # parse the results
            where = 'wu_id=%s' % item
            task_id = db.select('preprocess_wu', ['task_id'], where)
            task_id = task_id[0][0]
            parse_preprocess(item, job_path, file_list, task_table,
                             oneturn_table, list(oneturn.keys()))
            where = 'task_id=%s' % task_id
            db.update('preprocess_task', task_table, where)
            oneturn_table['task_id'] = task_id
            db.insert('oneturn_sixtrack_result', oneturn_table)
            if task_table['status'] == 'Success':
                job_table['status'] = 'complete'
                job_table['mtime'] = int(time.time() * 1E7)
                where = "wu_id=%s" % item
                db.update('preprocess_wu', job_table, where)
                content = "Preprocess job %s has completed normally!" % item
                logger.info(content)
            else:
                where = "wu_id=%s" % item
                job_table['status'] = 'incomplete'
                db.update('preprocess_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('preprocess_task', task_table)
            content = "This is a failed job!"
            logger.warning(content)
        shutil.rmtree(job_path)
    db.close()


def sixtrack_results(cf, cluster):
    '''Gather the results of sixtrack jobs and store in database'''
    info_sec = cf['info']

    boinc = info_sec['boinc']

    six_path = info_sec['path']
    if not os.path.isdir(six_path) or not os.listdir(six_path):
        content = "There isn't result in path %s!" % six_path
        logger.warning(content)
        return
    set_sec = cf['db_setting']
    f10_sec = cf['f10']
    db_info = cf['db_info']
    db = SixDB(db_info, settings=set_sec, create=False)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    where = "status='submitted'"
    job_ids = db.select('sixtrack_wu', ['wu_id', 'unique_id'], where)
    job_ids = [(str(j), str(i)) for i, j in job_ids]
    job_index = dict(job_ids)
    studypath = os.path.dirname(six_path)
    unfin = cluster.check_running(studypath)
    if unfin is not None:
        running_jobs = [job_index.pop(unid) for unid in unfin if unid in
                        job_index.keys()]
    else:
        content = "Can't get job status, try later!"
        logger.warniing(content)
        return
    if running_jobs:
        content = "Sixtrack jobs %s on HTCondor aren't completed yet!" % str(running_jobs)
        logger.warning(content)
    valid_wu_ids = list(job_index.values())

    # Donwload results from boinc if there is any
    if boinc.lower() == 'true':
        content = "Downloading results from boinc spool!"
        logger.info(content)
        stat, wu_ids = download_from_boinc(info_sec)
        if not stat:
            return
        unfn_wu_ids = [i for i in valid_wu_ids if i not in wu_ids]
        if unfn_wu_ids:
            content = "Sixtrack jobs %s on Boinc aren't completed yet!" % str(unfn_wu_ids)
            logger.warning(content)
        valid_wu_ids = wu_ids
    for item in os.listdir(six_path):
        if item not in valid_wu_ids:
            continue
        job_path = os.path.join(six_path, item)
        if not os.listdir(job_path):
            content = "The sixtrack job %s is empty!" % item
            logger.warning(content)
            continue
        job_table = {}
        task_table = {}
        f10_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            # parse the result
            parse_sixtrack(item, job_path, file_list, task_table, f10_table,
                           list(f10_sec.keys()))
            db.insert('sixtrack_task', task_table)
            where = "mtime=%s and wu_id=%s" % (task_table['mtime'], item)
            task_id = db.select('sixtrack_task', ['task_id'], where)
            task_id = task_id[0][0]
            f10_table['six_input_id'] = [task_id, ] * len(f10_table['mtime'])
            db.insertm('six_results', f10_table)
            if task_table['status'] == 'Success':
                job_table['status'] = 'complete'
                job_table['task_id'] = task_id
                job_table['mtime'] = int(time.time() * 1E7)
                where = "wu_id=%s" % item
                db.update('sixtrack_wu', job_table, where)
                content = "Sixtrack job %s has completed normally!" % item
                logger.info(content)
            else:
                where = "wu_id=%s" % item
                job_table['status'] = 'incomplete'
                db.update('sixtrack_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('sixtrack_task', task_table)
            content = "This is an empty job path!"
            logger.warning(content)
        shutil.rmtree(job_path)
    db.close()


def download_from_boinc(info_sec):
    '''Download results from boinc'''
    wu_ids = []

    six_path = info_sec['path']
    res_path = info_sec['boinc_results']
    st_pre = info_sec['st_pre']
    if not os.path.isdir(res_path):
        content = "There isn't job submitted to boinc!"
        logger.warning(content)
        return 0, []
    contents = os.listdir(res_path)
    if 'processed' in contents:
        contents.remove('processed')
    if not contents:
        content = "No result in boinc spool yet!"
        logger.warning(content)
        return 0, []
    out_path = six_path

    processed_path = os.path.join(res_path, 'processed')
    if not os.path.isdir(processed_path):
        os.mkdir(processed_path)
    username = getpass.getuser()
    tmp_path = os.path.join('/tmp', username, st_pre)
    if not os.path.isdir(tmp_path):
        os.mkdir(tmp_path)
    for res in contents:
        if re.match(r'%s.+\.zip' % st_pre, res):
            try:
                res_file = os.path.join(res_path, res)
                zph = zipfile.ZipFile(res_file, mode='r')
                zph.extractall(tmp_path)
                zph.close()
                shutil.move(res_file, processed_path)
            except Exception as e:
                logging.error(e, exc_info=True)
                zph.close()
                continue
    for f10 in os.listdir(tmp_path):
        if f10[-1] != '0':
            continue
        match = re.search('wu_id', f10)
        if not match:
            content = 'Something wrong with the result %s' % f10
            logger.warning(content)
            continue
        wu_id = f10[match.end() + 1:match.end() + 2]
        job_path = os.path.join(out_path, wu_id)
        if not os.path.isdir(job_path):
            content = "The output path for sixtrack job %s doesn't exist!" % wu_id
            logger.warning(content)
            os.mkdir(job_path)
        out_name = os.path.join(job_path, 'fort.10.gz')
        f10_file = os.path.join(tmp_path, f10)
        with open(f10_file, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        wu_ids.append(wu_id)
    shutil.rmtree(tmp_path)
    return 1, wu_ids
