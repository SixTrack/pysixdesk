#!/usr/bin/python3
import os
import sys
import time
import shutil
import utils
import configparser

from pysixdb import SixDB

def run(wu_id, infile):
    cf = configparser.ConfigParser()
    if os.path.isfile(infile):
        cf.read(infile)
        if str(wu_id) == '0':
            preprocess_results(cf)
        elif str(wu_id) == '1':
            sixtrack_results(cf)
        else:
            print("Unknown task!")
    else:
        print("The input file %s doesn't exist!"%infile)

def preprocess_results(cf):
    '''Gather the results of madx and oneturn sixtrack jobs and store in
    database
    '''
    info_sec = cf['info']
    preprocess_path = info_sec['path']
    if os.path.isdir(preprocess_path) and os.listdir(preprocess_path):
        set_sec = cf['db_setting']
        db_name = info_sec['db']
        db = SixDB(db_name, set_sec)
        file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])

        for item in os.listdir(preprocess_path):
            job_path = os.path.join(preprocess_path, item)
            job_table = {}
            task_table = {}
            task_table['status'] = 'Success'
            if os.path.isdir(job_path) and os.listdir(job_path):
                contents = os.listdir(job_path)
                madx_in = [s for s in contents if 'madx_in' in s]
                if madx_in:
                    madx_in = os.path.join(job_path, madx_in[0])
                    task_table['madx_in'] = utils.evlt(utils.compress_buf,\
                            [madx_in,'gzip'])
                else:
                    print("The madx_in file for job %s dosen't exist! The job failed!"%item)
                    task_table['status'] = 'Failed'
                madx_out = [s for s in contents if 'madx_stdout' in s]
                if madx_out:
                    madx_out = os.path.join(job_path, madx_out[0])
                    task_table['madx_stdout'] = utils.evlt(utils.compress_buf,\
                            [madx_out,'gzip'])
                else:
                    print("The madx_out file for job %s doesn't exist! The job failed!"%item)
                    task_table['status'] = 'Failed'
                for out in file_list.values():
                    out_f = [s for s in contents if out in s]
                    if out_f:
                        out_f = os.path.join(job_path, out_f[0])
                        task_table[out] = utils.evlt(utils.compress_buf,\
                                [out_f,'gzip'])
                    else:
                        task_table['status'] = 'Failed'
                        print("The madx output file %s for job %s doesn't exist! The job failed!"%(out, item))
                task_count = db.select('preprocess_task', ['task_id'])
                where = "wu_id=%s"%item
                job_count = db.select('preprocess_task', ['task_id'], where)
                task_table['count'] = len(job_count) + 1
                task_table['wu_id'] = item
                task_table['task_id'] = len(task_count) + 1
                task_table['task_name'] = ''
                task_table['mtime'] = time.time()
                db.insert('preprocess_task', task_table)
                if task_table['status'] == 'Success':
                    where = "wu_id=%s"%item
                    job_table['status'] = 'complete'
                    job_table['task_id'] = task_table['task_id']
                    db.update('preprocess_wu', job_table, where)
                    print("Successfully update madx job %s!"%item)
            else:
                task_table['status'] = 'Failed'
                db.insert('preprocess_task', task_table)
                print("This is a failed job!")
        db.close()
    else:
        print("The result path %s is invalid!"%preprocess_path)
        sys.exit(0)

def sixtrack_results(cf):
    '''Gather the results of sixtrack jobs and store in database'''
    info_sec = cf['info']
    six_path = info_sec['path']
    if os.path.isdir(six_path) and os.listdir(six_path):
        set_sec = cf['db_setting']
        db_name = info_sec['db']
        db = SixDB(db_name, set_sec)
        file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])

        for item in os.listdir(six_path):
            job_path = os.path.join(six_path, item)
            job_table = {}
            task_table = {}
            task_table['status'] = 'Success'
            if os.path.isdir(job_path) and os.listdir(job_path):
                contents = os.listdir(job_path)
                for out in file_list:
                    out_f = [s for s in contents if out in s]
                    if out_f:
                        out_f = os.path.join(job_path, out_f[0])
                        task_table[out] = utils.evlt(utils.compress_buf,\
                                [out_f,'gzip'])
                    else:
                        task_table['status'] = 'Failed'
                        print("The sixtrack output file %s for job %s doesn't exist! The job failed!"%(out, item))
                task_count = db.select('sixtrack_task', ['task_id'])
                where = "wu_id=%s"%item
                job_count = db.select('sixtrack_task', ['task_id'], where)
                task_table['wu_id'] = item
                task_table['count'] = len(job_count) + 1
                task_table['task_id'] = len(task_count) + 1
                task_table['task_name'] = ''
                task_table['mtime'] = time.time()
                db.insert('sixtrack_task', task_table)
                if task_table['status'] == 'Success':
                    where = "wu_id=%s"%item
                    job_table['status'] = 'complete'
                    job_table['task_id'] = task_table['task_id']
                    db.update('sixtrack_wu', job_table, where)
                    print("Successfully update sixtrack job %s!"%item)
            else:
                task_table['status'] = 'Failed'
                db.insert('sixtrack_task', task_table)
                print("This is a failed job!")
        db.close()
    else:
        print("The result path %s is invalid!"%six_path)
        sys.exit(0)

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
