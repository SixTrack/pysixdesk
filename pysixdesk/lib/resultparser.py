import os
import re
import time
import gzip
import logging

from pysixdesk.lib.utils import compress_buf

'''Parse the results of preprocess jobs and sixtrack jobs'''

logger = logging.getLogger(__name__)


def parse_results(jobtype, item, job_path, file_list, task_table, result_cf):
    '''parse the results'''
    task_table['mtime'] = int(time.time() * 1E7)
    contents = []
    for a in os.walk(job_path):
        if a[0] == '.' or str(item) in a[0].split('/'):
            for b in a[2]:
                contents.append(os.path.join(a[0], b))

    def search_store(key, name):
        search_re = [s for s in contents if name in os.path.basename(s)]
        if search_re:
            search_re = search_re[0]
            task_table[key] = compress_buf(search_re, 'gzip')

    if jobtype == 'preprocess':
        search_store('madx_in', 'madx_in')
        search_store('madx_stdout', 'madx_stdout')

    if jobtype == 'sixtrack':
        search_store('fort_3', 'fort.3')
        # The following files are used for checkpoint/restart
        search_store('cr_status', 'cr_status')
        search_store('cr_stdout', 'cr_stdout')
        search_store('cr_stderr', 'cr_stderr')
        search_store('crpoint_pri_bin', 'crpoint_pri')
        search_store('crpoint_sec_bin', 'crpoint_sec')
        search_store('singletrackfile_dat', 'singletrackfile')
        search_store('fort_6', 'fort.6')

    job_stdout = [s for s in contents if (re.match(r'htcondor\..+\.out',
        os.path.basename(s)) or re.match(r'_condor_stdout', os.path.basename(s)))]
    if job_stdout:
        job_stdout = job_stdout[0]
        task_table['job_stdout'] = compress_buf(job_stdout)

    job_stderr = [s for s in contents if (re.match(r'htcondor\..+\.err',
        os.path.basename(s)) or re.match(r'_condor_stderr', os.path.basename(s)))]
    if job_stderr:
        job_stderr = job_stderr[0]
        task_table['job_stderr'] = compress_buf(job_stderr)

    job_stdlog = [s for s in contents if re.match(r'htcondor\..+\.log',
        os.path.basename(s))]
    if job_stdlog:
        job_stdlog = job_stdlog[0]
        task_table['job_stdlog'] = compress_buf(job_stdlog)

    valid_tname = []
    for out, tname in file_list.items():
        out_f = [s for s in contents if out in os.path.basename(s)]
        if out_f:
            out_f = out_f[0]
            if tname is not None:
                try:
                    parse_file(out_f, task_table, result_cf[tname], tname)
                    valid_tname.append(tname)
                except Exception as e:
                    task_table['status'] = 'Failed'
                    content = "There is something wrong with the output "\
                        "file %s for task %s!" % (out, item)
                    logger.error(content)
                    logger.error(e, exc_info=True)
            task_table[out] = compress_buf(out_f, 'gzip')
        else:
            task_table['status'] = 'Failed'
            content = f"The {jobtype} output file {out} for task {item} "\
                      "doesn't exist! The job failed!"
            logger.warning(content)
    # clean the redundant sections
    keys = list(result_cf.keys())
    for tname in keys:
        if tname not in valid_tname:
            result_cf.pop(tname)


def parse_file(out_f, task_table, result_table, tname):
    '''parse the files'''
    countl = 0
    mtime = int(os.path.getmtime(out_f) * 1E7)
    with gzip.open(out_f, 'rt') as f_in:
        raw_lines = f_in.readlines()
    lines = []
    postlines = []
    for lin in raw_lines:
        if lin.lstrip()[0] == '#':
            continue
        lines.append(lin)
    status = globals()[tname](lines, postlines)
    if not status:
        task_table['status'] = 'Failed'
        content = 'Error in %s' % out_f
        logger.warning(content)
    post_data = []
    for line in postlines:
        countl += 1
        line = [countl] + line + [mtime]
        post_data.append(line)
    keys = list(result_table.keys())
    result_table.update(dict(zip(keys[1:], zip(*post_data))))


# The following methods to parse specific files should have the same name with
# the table which they will be stored in
def oneturn_sixtrack_results(lines, postlines):
    '''process the lines of oneturnresult'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 21:
            logger.info(perline)
            line = 21*['None']
            status = False
        postlines.append(line)
    return status


def six_results(lines, postlines):
    '''process the lines of fort.10'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 60:
            logger.info(perline)
            line = 60*['None']
            status = False
        postlines.append(line)
    return status


def init_state(lines, postlines):
    '''process the lines of initial_state.dat'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 12:
            logger.info(perline)
            line = 12*['None']
            status = False
        postlines.append(line)
    return status


def final_sate(lines, postlines):
    '''process the lines of final_state.dat'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 12:
            # logger.info(perline)
            line = 12*['None']
            status = False
        postlines.append(line)
    return status


def aperture_losses(lines, postlines):
    '''process the lines of aperture_losses.dat'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 15:
            logger.info(perline)
            line = 15*['None']
            status = False
        postlines.append(line)
    return status


def collimation_losses(lines, postlines):
    '''process the lines of Coll_Scatter.dat'''
    status = True
    for perline in lines:
        line = perline.split()
        if len(line) != 7:
            logger.info(perline)
            line = 7*['None']
            status = False
        postlines.append(line)
    return status
