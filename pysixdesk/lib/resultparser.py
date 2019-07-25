import os
import re
import time
import gzip
import logging

from pysixdesk.lib.utils import evlt, compress_buf

'''Parse the results of preprocess jobs and sixtrack jobs'''


def parse_preprocess(item, job_path, file_list, task_table, oneturn_table,
                     oneturn_param_names):
    '''Parse the results of preprocess jobs'''
    logger = logging.getLogger(__name__)

    task_table['wu_id'] = item
    task_table['mtime'] = int(time.time() * 1E7)

    contents = os.listdir(job_path)
    madx_in = [s for s in contents if 'madx_in' in s]
    if madx_in:
        madx_in = os.path.join(job_path, madx_in[0])
        task_table['madx_in'] = evlt(compress_buf, [madx_in, 'gzip'])
    else:
        content = "The madx_in file for job %s dosen't exist! The job failed!" % item
        logger.error(content)
        task_table['status'] = 'Failed'
    madx_out = [s for s in contents if 'madx_stdout' in s]
    if madx_out:
        madx_out = os.path.join(job_path, madx_out[0])
        task_table['madx_stdout'] = evlt(compress_buf, [madx_out, 'gzip'])
    else:
        content = "The madx_out file for job %s doesn't exist! The job failed!" % item
        logger.error(content)
        task_table['status'] = 'Failed'
    job_stdout = [s for s in contents if (re.match(r'htcondor\..+\.out', s) or
                                          re.match(r'_condor_stdout', s))]
    if job_stdout:
        job_stdout = os.path.join(job_path, job_stdout[0])
        task_table['job_stdout'] = evlt(compress_buf, [job_stdout])
    job_stderr = [s for s in contents if (re.match(r'htcondor\..+\.err', s) or
                                          re.match(r'_condor_stderr', s))]
    if job_stderr:
        job_stderr = os.path.join(job_path, job_stderr[0])
        task_table['job_stderr'] = evlt(compress_buf, [job_stderr])
    job_stdlog = [s for s in contents if re.match(r'htcondor\..+\.log', s)]
    if job_stdlog:
        job_stdlog = os.path.join(job_path, job_stdlog[0])
        task_table['job_stdlog'] = evlt(compress_buf, [job_stdlog])
    oneturn_result = [s for s in contents if 'oneturnresult' in s]
    # chrom = [s for s in contents if 'chrom' in s]
    # tunes = [s for s in contents if 'sixdesktunes' in s]
    if oneturn_result:
        oneturn_result = os.path.join(job_path, oneturn_result[0])
        # chrom = os.path.join(job_path, chrom[0])
        # tunes = os.path.join(job_path, tunes[0])
        mtime = int(os.path.getmtime(oneturn_result) * 1E7)
        # with gzip.open(betavalue, 'rt') as f_in:
        #     line = f_in.read()
        #     lines_beta = line.split()
        # with gzip.open(chrom, 'rt') as f_in:
        #     line = f_in.read()
        #     lines_chrom = line.split()
        with gzip.open(oneturn_result, 'rt') as f_in:

            line = f_in.read()
            lines = line.split()
        # lines = lines_beta + lines_chrom + lines_tunes
        if len(lines) != 21:
            logger.info(lines)
            content = 'Error in one turn result of preprocess job %s!' % item
            logger.error(content)
            task_table['status'] = 'Failed'
            data = [item] + 21 * ['None'] + [mtime]
        else:
            data = [item] + lines + [mtime]
        oneturn_table.update(dict(zip(oneturn_param_names[1:], data)))
    for out in file_list.values():
        out_f = [s for s in contents if out in s]
        if out_f:
            out_f = os.path.join(job_path, out_f[0])
            task_table[out] = evlt(compress_buf, [out_f, 'gzip'])
        else:
            task_table['status'] = 'Failed'
            content = "The madx output file %s for job %s doesn't exist! The job failed!" % (
                out, item)
            logger.error(content)


def parse_sixtrack(item, job_path, file_list, task_table, f10_table, f10_names):
    '''Parse the results of sixtrack job'''
    logger = logging.getLogger(__name__)

    task_table['wu_id'] = item
    task_table['mtime'] = int(time.time() * 1E7)
    contents = os.listdir(job_path)
    fort3_in = [s for s in contents if 'fort.3' in s]
    if fort3_in:
        fort3_in = os.path.join(job_path, fort3_in[0])
        task_table['fort3'] = evlt(compress_buf, [fort3_in, 'gzip'])
    job_stdout = [s for s in contents if (re.match(r'htcondor\..+\.out', s) or
                                          re.match(r'_condor_stdout', s))]
    if job_stdout:
        job_stdout = os.path.join(job_path, job_stdout[0])
        task_table['job_stdout'] = evlt(compress_buf, [job_stdout])
    job_stderr = [s for s in contents if (re.match(r'htcondor\..+\.err', s) or
                                          re.match(r'_condor_stderr', s))]
    if job_stderr:
        job_stderr = os.path.join(job_path, job_stderr[0])
        task_table['job_stderr'] = evlt(compress_buf, [job_stderr])
    job_stdlog = [s for s in contents if re.match(r'htcondor\..+\.log', s)]
    if job_stdlog:
        job_stdlog = os.path.join(job_path, job_stdlog[0])
        task_table['job_stdlog'] = evlt(compress_buf, [job_stdlog])
    for out in file_list:
        out_f = [s for s in contents if out in s]
        if out_f:
            out_f = os.path.join(job_path, out_f[0])
            if 'fort.10' in out_f:
                countl = 1
                try:
                    mtime = int(os.path.getmtime(out_f) * 1E7)
                    f10_data = []
                    with gzip.open(out_f, 'rt') as f_in:
                        for lines in f_in:
                            line = lines.split()
                            countl += 1
                            if len(line) != 60:
                                logger.info(line)
                                content = 'Error in %s' % out_f
                                logger.warning(content)
                                task_table['status'] = 'Failed'
                                line = [countl] + 60 * ['None'] + [mtime]
                                f10_data.append(line)
                            else:
                                line = [countl] + line + [mtime]
                                f10_data.append(line)
                    f10_table.update(dict(zip(f10_names[1:], zip(*f10_data))))
                except:
                    task_table['status'] = 'Failed'
                    content = "There is something wrong with the output "\
                        "file %s for job %s!" % (out, item)
                    logger.error(content)
            task_table[out] = evlt(compress_buf, [out_f, 'gzip'])
        else:
            task_table['status'] = 'Failed'
            content = "The sixtrack output file %s for job %s doesn't "\
                "exist! The job failed!" % (out, item)
            logger.warning(content)
