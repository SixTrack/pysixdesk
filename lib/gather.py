#!/usr/bin/python3
import os
import re
import sys
import time
import gzip
import shutil
import utils
import traceback
import configparser
import resultparser as rp

from pysixdb import SixDB
from importlib.machinery import SourceFileLoader

def run(wu_id, infile):
    cf = configparser.ConfigParser()
    if os.path.isfile(infile):
        cf.read(infile)
        info_sec = cf['info']
        cluster_module = info_sec['cluster_module']
        classname = info_sec['cluster_name']
        mes_level = int(info_sec['mes_level'])
        log_file = info_sec['log_file']
        if len(log_file) == 0:
            log_file = None
        try:
            module_name = os.path.abspath(cluster_module)
            module_name = module_name.replace('.py', '')
            mod = SourceFileLoader(module_name, cluster_module).load_module()
            cls = getattr(mod, classname)
            cluster = cls(mes_level, log_file)
        except:
            utils.message('Error', traceback.print_exc(), mes_level, log_file)
            content = "Failed to instantiate cluster module %s!"%cluster_module
            utils.message('Error', content, mes_level, log_file)
            return
        if str(wu_id) == '0':
            preprocess_results(cf, cluster)
        elif str(wu_id) == '1':
            sixtrack_results(cf, cluster)
        else:
            content = "Unknown task!"
            utils.message('Error', content, mes_level, log_file)
    else:
        content = "The input file %s doesn't exist!"%infile
        utils.message('Error', content, mes_level, log_file)

def preprocess_results(cf, cluster):
    '''Gather the results of madx and oneturn sixtrack jobs and store in
    database
    '''
    info_sec = cf['info']
    mes_level = int(info_sec['mes_level'])
    log_file = info_sec['log_file']
    if len(log_file) == 0:
        log_file = None
    preprocess_path = info_sec['path']
    if not os.path.isdir(preprocess_path) or not os.listdir(preprocess_path):
        content = "There isn't result in path %s!"%preprocess_path
        utils.message('Warning', content, mes_level, log_file)
        return
    contents = os.listdir(preprocess_path)
    set_sec = cf['db_setting']
    db_info = cf['db_info']
    oneturn = cf['oneturn']
    db = SixDB(db_info, set_sec)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    where = "status='submitted'"
    job_ids = db.select('preprocess_wu', ['wu_id', 'unique_id'], where)
    job_ids = [(str(i), str(j)) for i, j in job_ids]
    job_index = dict(job_ids)

    for item in os.listdir(preprocess_path):
        if not item in job_index.keys():
            content = "Unknown preprocess job id %s!"%item
            utils.message('Error', content, mes_level, log_file)
            continue
        else:
            status = cluster.check_format(job_index[item])
            if status is None:
                continue
            elif status:
                content = "The preprocess job %s isn't completed yet!"%item
                utils.message('Warning', content, mes_level, log_file)
                continue
        job_path = os.path.join(preprocess_path, item)
        job_table = {}
        task_table = {}
        oneturn_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            task_count = db.select('preprocess_task', ['task_id'])
            where = "wu_id=%s"%item
            job_count = db.select('preprocess_task', ['task_id'], where)
            count = len(job_count) + 1
            task_id = len(task_count) + 1
            #parse the results
            rp.parse_preprocess(item, job_path, file_list, count, task_id,
                    task_table, oneturn_table, oneturn.keys(), mes_level,
                    log_file)
            db.insert('preprocess_task', task_table)
            db.insert('oneturn_sixtrack_result', oneturn_table)
            if task_table['status'] == 'Success':
                where = "wu_id=%s"%item
                job_table['status'] = 'complete'
                job_table['task_id'] = task_table['task_id']
                db.update('preprocess_wu', job_table, where)
                content = "Preprocess job %s has completed normally!"%item
                utils.message('Message', content, mes_level, log_file)
            else:
                where = "wu_id=%s"%item
                job_table['status'] = 'incomplete'
                db.update('preprocess_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('preprocess_task', task_table)
            content = "This is a failed job!"
            utils.message('Warning', content, mes_level, log_file)
        shutil.rmtree(job_path)
    db.close()

def sixtrack_results(cf, cluster):
    '''Gather the results of sixtrack jobs and store in database'''
    info_sec = cf['info']
    mes_level = int(info_sec['mes_level'])
    log_file = info_sec['log_file']
    if len(log_file) == 0:
        log_file = None
    six_path = info_sec['path']
    if not os.path.isdir(six_path) or not os.listdir(six_path):
        content = "There isn't result in path %s!"%six_path
        utils.message('Warning', content, mes_level, log_file)
        return
    set_sec = cf['db_setting']
    f10_sec = cf['f10']
    db_info = cf['db_info']
    db = SixDB(db_info, set_sec)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    where = "status='submitted'"
    job_ids = db.select('sixtrack_wu', ['wu_id', 'unique_id'], where)
    job_ids = [(str(i), str(j)) for i, j in job_ids]
    job_index = dict(job_ids)
    for item in os.listdir(six_path):
        if not item in job_index.keys():
            content = "Unknown sixtrack job id %s!"%item
            utils.message('Error', content, mes_level, log_file)
            continue
        else:
            status = cluster.check_format(job_index[item])
            if status is None:
                continue
            elif status:
                content = "The sixtrack job %s isn't completed yet!"%item
                utils.message('Warning', content, mes_level, log_file)
                continue
        job_path = os.path.join(six_path, item)
        job_table = {}
        task_table = {}
        f10_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            task_count = db.select('sixtrack_task', ['task_id'])
            where = "wu_id=%s"%item
            job_count = db.select('sixtrack_task', ['task_id'], where)
            #parse the result
            count = len(job_count) + 1
            task_id = len(task_count) + 1
            rp.parse_sixtrack(item, job_path, file_list, count, task_id,
                    task_table, f10_table, f10_sec.keys(), mes_level, log_file)
            db.insert('sixtrack_task', task_table)
            db.insertm('six_results', f10_table)
            if task_table['status'] == 'Success':
                where = "wu_id=%s"%item
                job_table['status'] = 'complete'
                job_table['task_id'] = task_table['task_id']
                db.update('sixtrack_wu', job_table, where)
                content = "Sixtrack job %s has completed normally!"%item
                utils.message('Message', content, mes_level, log_file)
            else:
                where = "wu_id=%s"%item
                job_table['status'] = 'incomplete'
                db.update('sixtrack_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('sixtrack_task', task_table)
            content = "This is an empty job path!"
            utils.message('Warning', content, mes_level, log_file)
        shutil.rmtree(job_path)
    db.close()

if __name__ == '__main__':
    args = sys.argv
    num = len(args[1:])
    if num == 0 or num == 1:
        print("The input file is missing!")
        sys.exit(1)
    elif num == 2:
        wu_id = args[1]
        in_file = args[2]
        run(wu_id, in_file)
    else:
        print("Too many input arguments!")
        sys.exit(1)
