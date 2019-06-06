import os
import re
import sys
import time
import gzip
import utils
import shutil

'''Parse the results of preprocess jobs and sixtrack jobs'''

def parse_preprocess(item, job_path, file_list, task_table, oneturn_table,
        oneturn_param_names, mes_level=1, log_file=None):
    '''Parse the results of preprocess jobs'''
    task_table['wu_id'] = item
    task_table['mtime'] = str(time.time())

    contents = os.listdir(job_path)
    madx_in = [s for s in contents if 'madx_in' in s]
    if madx_in:
        madx_in = os.path.join(job_path, madx_in[0])
        task_table['madx_in'] = utils.evlt(utils.compress_buf,\
                [madx_in,'gzip'])
    else:
        content = "The madx_in file for job %s dosen't exist! The job failed!"%item
        utils.message('Error', content, mes_level, log_file)
        task_table['status'] = 'Failed'
    madx_out = [s for s in contents if 'madx_stdout' in s]
    if madx_out:
        madx_out = os.path.join(job_path, madx_out[0])
        task_table['madx_stdout'] = utils.evlt(utils.compress_buf,\
                [madx_out,'gzip'])
    else:
        content = "The madx_out file for job %s doesn't exist! The job failed!"%item
        utils.message('Error', content, mes_level, log_file)
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
        mtime = str(os.path.getmtime(betavalue))
        with gzip.open(betavalue, 'rt') as f_in:
            line = f_in.read()
            lines_beta = line.split()
        with gzip.open(chrom, 'rt') as f_in:
            line = f_in.read()
            lines_chrom = line.split()
        with gzip.open(tunes, 'rt') as f_in:
            line = f_in.read()
            lines_tunes = line.split()
        lines = lines_beta + lines_chrom + lines_tunes
        if len(lines) != 21:
            utils.message('Message', lines, mes_level, log_file)
            content = 'Error in one turn result of preprocess job %s!'%item
            utils.message('Error', content, mes_level, log_file)
            task_table['status'] = 'Failed'
            #data = [task_id, item]+21*['None']+[mtime]
            data = [item]+21*['None']+[mtime]
        else:
            #data = [task_id, item]+lines+[mtime]
            data = [item]+lines+[mtime]
        oneturn_table.update(dict(zip(oneturn_param_names[1:], data)))
    for out in file_list.values():
        out_f = [s for s in contents if out in s]
        if out_f:
            out_f = os.path.join(job_path, out_f[0])
            task_table[out] = utils.evlt(utils.compress_buf,\
                    [out_f,'gzip'])
        else:
            task_table['status'] = 'Failed'
            content = "The madx output file %s for job %s doesn't exist! The job failed!"%(out, item)
            utils.message('Error', content, mes_level, log_file)

def parse_sixtrack(item, job_path, file_list, task_table, f10_table, f10_names,
        mes_level=1, log_file=None):
    task_table['wu_id'] = item
    task_table['mtime'] = str(time.time())
    contents = os.listdir(job_path)
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
                    mtime = str(os.path.getmtime(out_f))
                    f10_data = []
                    with gzip.open(out_f, 'rt') as f_in:
                        for lines in f_in:
                            line = lines.split()
                            countl += 1
                            if len(line)!=60:
                                utils.message('Message', line, mes_level, log_file)
                                content = 'Error in %s'%out_f
                                utils.message('Warning', content, mes_level, log_file)
                                task_table['status'] = 'Failed'
                                #line = [task_id, countl]+60*['None']+[mtime]
                                line = [countl]+60*['None']+[mtime]
                                f10_data.append(line)
                            else:
                                #line = [task_id, countl]+line+[mtime]
                                line = [countl]+line+[mtime]
                                f10_data.append(line)
                    f10_table.update(dict(zip(f10_names[1:], zip(*f10_data))))
                except:
                    task_table['status'] = 'Failed'
                    content = "There is something wrong with the output "\
                            "file %s for job %s!"%(out, item)
                    utils.message('Error', content, mes_level, log_file)
            task_table[out] = utils.evlt(utils.compress_buf,\
                    [out_f, 'gzip'])
        else:
            task_table['status'] = 'Failed'
            content = "The sixtrack output file %s for job %s doesn't "\
                    "exist! The job failed!"%(out, item)
            utils.message('Warning', content, mes_level, log_file)
