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

from pysixdb import SixDB
from importlib.machinery import SourceFileLoader

def run(wu_id, infile):
    cf = configparser.ConfigParser()
    if os.path.isfile(infile):
        cf.read(infile)
        info_sec = cf['info']
        cluster_module = info_sec['cluster_module']
        classname = info_sec['cluster_name']
        try:
            module_name = os.path.abspath(cluster_module)
            module_name = module_name.replace('.py', '')
            mod = SourceFileLoader(module_name, cluster_module).load_module()
            cls = getattr(mod, classname)
            cluster = cls()
        except:
            print(traceback.print_exc())
            print("Failed to instantiate cluster object!")
            return
        if str(wu_id) == '0':
            preprocess_results(cf, cluster)
        elif str(wu_id) == '1':
            sixtrack_results(cf, cluster)
        else:
            print("Unknown task!")
    else:
        print("The input file %s doesn't exist!"%infile)

def preprocess_results(cf, cluster):
    '''Gather the results of madx and oneturn sixtrack jobs and store in
    database
    '''
    info_sec = cf['info']
    preprocess_path = info_sec['path']
    if not os.path.isdir(preprocess_path) or not os.listdir(preprocess_path):
        print("The result path %s is invalid!"%preprocess_path)
        sys.exit(0)
    contents = os.listdir(preprocess_path)
    set_sec = cf['db_setting']
    db_name = info_sec['db']
    oneturn = cf['oneturn']
    db = SixDB(db_name, set_sec)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    where = "status='submitted' and jobtype='preprocess'"
    #job_ids = db.select('submit', [batch_name, proc_id, sub_id], where)
    #cluster.check()

    for item in os.listdir(preprocess_path):
        job_path = os.path.join(preprocess_path, item)
        job_table = {}
        task_table = {}
        oneturn_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            task_count = db.select('preprocess_task', ['task_id'])
            where = "wu_id=%s"%item
            job_count = db.select('preprocess_task', ['task_id'], where)
            task_table['count'] = len(job_count) + 1
            task_table['wu_id'] = item
            task_id = len(task_count) + 1
            task_table['task_id'] = task_id
            task_table['task_name'] = 'preprocess_job_%s_%i'%(item, task_id)
            task_table['mtime'] = time.time()

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
            job_stdout = [s for s in contents if re.match('htcondor\..+\.out',s)]
            if job_stdout:
                job_stdout = os.path.join(job_path, job_stdout[0])
                task_table['job_stdout'] = utils.evlt(utils.compress_buf,\
                        [job_stdout])
            job_stderr = [s for s in contents if re.match('htcondor\..+\.err',s)]
            if job_stderr:
                job_stderr = os.path.join(job_path, job_stderr[0])
                task_table['job_stderr'] = utils.evlt(utils.compress_buf,\
                        [job_stderr])
            job_stdlog = [s for s in contents if re.match('htcondor\..+\.log',s)]
            if job_stdlog:
                job_stdlog = os.path.join(job_path, job_stdlog[0])
                task_table['job_stdlog'] = utils.evlt(utils.compress_buf,\
                        [job_stdlog])
            betavalue = [s for s in contents if 'betavalues' in s]
            chrom = [s for s in contents if 'chrom' in s]
            tunes = [s for s in contents if 'sixdesktunes' in s]
            if betavalue and chrom and tunes:
                betavalue = os.path.join(job_path, betavalue[0])
                chrom = os.path.join(job_path, chrom[0])
                tunes = os.path.join(job_path, tunes[0])
                mtime = os.path.getmtime(betavalue)
                with open(betavalue, 'r') as f_in:
                    line = f_in.read()
                    lines_beta = line.split()
                with open(chrom, 'r') as f_in:
                    line = f_in.read()
                    lines_chrom = line.split()
                with open(tunes, 'r') as f_in:
                    line = f_in.read()
                    lines_tunes = line.split()
                lines = lines_beta + lines_chrom + lines_tunes
                if len(lines) != 21:
                    print(lines)
                    print('Error in one turn result of preprocess job %s!'%item)
                    task_table['status'] = 'Failed'
                    data = [task_id, item]+21*['None']+[mtime]
                else:
                    data = [task_id, item]+lines+[mtime]
                oneturn_table = dict(zip(oneturn.keys(), data))
            for out in file_list.values():
                out_f = [s for s in contents if out in s]
                if out_f:
                    out_f = os.path.join(job_path, out_f[0])
                    task_table[out] = utils.evlt(utils.compress_buf,\
                            [out_f,'gzip'])
                else:
                    task_table['status'] = 'Failed'
                    print("The madx output file %s for job %s doesn't exist! The job failed!"%(out, item))
            db.insert('preprocess_task', task_table)
            db.insert('oneturn_sixtrack_result', oneturn_table)
            if task_table['status'] == 'Success':
                where = "wu_id=%s"%item
                job_table['status'] = 'complete'
                job_table['task_id'] = task_table['task_id']
                db.update('preprocess_wu', job_table, where)
                print("Preprocess job %s has completed normally!"%item)
            else:
                where = "wu_id=%s"%item
                job_table['status'] = 'incomplete'
                db.update('preprocess_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('preprocess_task', task_table)
            print("This is a failed job!")
        #shutil.rmtree(job_path)
    db.close()

def sixtrack_results(cf, cluster):
    '''Gather the results of sixtrack jobs and store in database'''
    info_sec = cf['info']
    six_path = info_sec['path']
    if not os.path.isdir(six_path) or not os.listdir(six_path):
        print("The result path %s is invalid!"%six_path)
        sys.exit(0)

    set_sec = cf['db_setting']
    f10_sec = cf['f10']
    db_name = info_sec['db']
    db = SixDB(db_name, set_sec)
    file_list = utils.evlt(utils.decode_strings, [info_sec['outs']])
    for item in os.listdir(six_path):
        job_path = os.path.join(six_path, item)
        job_table = {}
        task_table = {}
        f10_table = {}
        task_table['status'] = 'Success'
        if os.path.isdir(job_path) and os.listdir(job_path):
            contents = os.listdir(job_path)
            task_count = db.select('sixtrack_task', ['task_id'])
            where = "wu_id=%s"%item
            job_count = db.select('sixtrack_task', ['task_id'], where)
            task_table['wu_id'] = item
            task_table['count'] = len(job_count) + 1
            task_id = len(task_count) + 1
            task_table['task_id'] = task_id
            fort3_in = [s for s in contents if 'fort.3' in s]
            if fort3_in:
                fort3_in = os.path.join(job_path, fort3_in[0])
                task_table['fort3'] = utils.evlt(utils.compress_buf,\
                        [fort3_in,'gzip'])
            job_stdout = [s for s in contents if re.match('htcondor\..+\.out',s)]
            if job_stdout:
                job_stdout = os.path.join(job_path, job_stdout[0])
                task_table['job_stdout'] = utils.evlt(utils.compress_buf,\
                        [job_stdout])
            job_stderr = [s for s in contents if re.match('htcondor\..+\.err',s)]
            if job_stderr:
                job_stderr = os.path.join(job_path, job_stderr[0])
                task_table['job_stderr'] = utils.evlt(utils.compress_buf,\
                        [job_stderr])
            job_stdlog = [s for s in contents if re.match('htcondor\..+\.log',s)]
            if job_stdlog:
                job_stdlog = os.path.join(job_path, job_stdlog[0])
                task_table['job_stdlog'] = utils.evlt(utils.compress_buf,\
                        [job_stdlog])
            for out in file_list:
                out_f = [s for s in contents if out in s]
                if out_f:
                    out_f = os.path.join(job_path, out_f[0])
                    if 'fort.10' in out_f:
                        countl = 1
                        try:
                            mtime = os.path.getmtime(out_f)
                            f10_data = []
                            with gzip.open(out_f, 'r') as f_in:
                                for lines in f_in:
                                    line = lines.split()
                                    countl += 1
                                    if len(line)!=60:
                                        print(line)
                                        print('Error in %s'%out_f)
                                        task_table['status'] = 'Failed'
                                        line = [task_id, countl]+60*['None']+[mtime]
                                        f10_data.append(line)
                                    else:
                                        line = [task_id, countl]+line+[mtime]
                                        f10_data.append(line)
                        except:
                            task_table['status'] = 'Failed'
                            print("There is something wrong with the output "
                                    "file %s for job %s!"%(out, item))
                    task_table[out] = utils.evlt(utils.compress_buf,\
                            [out_f, 'gzip'])
                else:
                    task_table['status'] = 'Failed'
                    print("The sixtrack output file %s for job %s doesn't"
                            "exist! The job failed!"%(out, item))
            task_table['task_name'] = ''
            task_table['mtime'] = time.time()
            db.insert('sixtrack_task', task_table)
            f10 = dict(zip(f10_sec.keys(), zip(*f10_data)))
            db.insertm('six_results', f10)
            if task_table['status'] == 'Success':
                where = "wu_id=%s"%item
                job_table['status'] = 'complete'
                job_table['task_id'] = task_table['task_id']
                db.update('sixtrack_wu', job_table, where)
                print("Sixtrack job %s has completed normally!"%item)
            else:
                where = "wu_id=%s"%item
                job_table['status'] = 'incomplete'
                db.update('sixtrack_wu', job_table, where)
        else:
            task_table['status'] = 'Failed'
            db.insert('sixtrack_task', task_table)
            print("This is an empty job path!")
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
