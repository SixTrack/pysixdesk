#!/usr/bin/env python3
import re
import os
import time
import gzip
import copy
import shutil
import getpass
import zipfile
import logging
import importlib

from .pysixdb import SixDB
from . import utils
from .resultparser import parse_results

logger = logging.getLogger(__name__)

def run(wu_id, info):
    cf = {}
    cf.update(info)
    info_sec = cf['info']
    boinc = False
    if str(wu_id) == '1':
        boinc = info_sec['boinc']

    db_info = cf['db_info']
    dbtype = db_info['db_type']
    if dbtype.lower() == 'mysql' and not boinc:
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
        gather_results('preprocess', cf, cluster)
    elif str(wu_id) == '1':
        gather_results('sixtrack', cf, cluster)
    else:
        content = "Unknown task!"
        logger.error(content)


def gather_results(jobtype, cf, cluster):
    '''Gather the results'''
    info_sec = cf['info']
    type_path = info_sec['path']
    if not os.path.isdir(type_path) or not os.listdir(type_path):
        content = "There isn't result in path %s!" % type_path
        logger.warning(content)
        return
    set_sec = cf['db_setting']
    db_info = cf['db_info']
    db = SixDB(db_info, settings=set_sec, create=False)
    file_list = info_sec['outs']
    where = "status='submitted'"
    job_ids = db.select(f'{jobtype}_wu', ['wu_id', 'unique_id'], where)
    job_ids = [(str(j), str(i)) for i, j in job_ids]
    job_index = dict(job_ids)
    studypath = os.path.dirname(type_path)
    unfin = cluster.check_running(studypath)
    running_jobs = [job_index.pop(unid) for unid in unfin]
    if running_jobs:
        content = f"The {jobtype} jobs {str(running_jobs)} aren't completed yet!"
        logger.warning(content)
    valid_wu_ids = list(job_index.values())

    if ('boinc' in cf['info'].keys()) and cf['info']['boinc']:
        content = "Downloading results from boinc spool!"
        logger.info(content)
        stat, wu_ids = download_from_boinc(info_sec)
        if not stat:
            return
        unfn_wu_ids = [i for i in valid_wu_ids if i not in wu_ids]
        if unfn_wu_ids:
            content = f"{jobtype} jobs {str(unfn_wu_ids)} on Boinc aren't completed yet!"
            logger.warning(content)
        valid_wu_ids = wu_ids

    parent_cf = {}
    for sec in cf:
        parent_cf[sec] = cf[sec]
    for item in os.listdir(type_path):
        if item not in valid_wu_ids:
            continue
        job_path = os.path.join(type_path, item)
        if not os.listdir(job_path):
            content = f"The {jobtype} job {item} is empty!"
            logger.warning(content)
            continue
        result_cf = copy.deepcopy(parent_cf)
        job_table = {}
        task_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            # parse the results
            where = 'wu_id=%s' % item
            task_id = db.select(f'{jobtype}_wu', ['task_id'], where)
            task_id = task_id[0][0]
            parse_results(jobtype, item, job_path, file_list, task_table,
                    result_cf)
            where = 'task_id=%s' % task_id
            db.update(f'{jobtype}_task', task_table, where)
            for sec, vals in result_cf.items():
                vals['task_id'] = [task_id,]*len(vals['mtime'])
                db.insertm(sec, vals)
            if task_table['status'] == 'Success':
                job_table['status'] = 'complete'
                job_table['mtime'] = int(time.time() * 1E7)
                where = "wu_id=%s" % item
                db.update(f'{jobtype}_wu', job_table, where)
                content = f"{jobtype} job {item} has completed normally!"
                logger.info(content)
            else:
                where = "wu_id=%s" % item
                job_table['status'] = 'incomplete'
                db.update(f'{jobtype}_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert(f'{jobtype}_task', task_table)
            content = "This is a failed job!"
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
