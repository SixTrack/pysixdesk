#!/usr/bin/env python3
import os
import re
import sys
import ast
import time
import shutil
import zipfile
import configparser
from subprocess import Popen, PIPE

from pysixdesk.lib.pysixdb import SixDB
from pysixdesk.lib import utils
from pysixdesk.lib.dbtable import Table
from pysixdesk.lib.resultparser import parse_results

logger = utils.condor_logger('sixtrack')


def run(wu_id, input_info):
    cf = configparser.ConfigParser()
    cf.optionxform = str  # preserve case
    cf.read(input_info)
    db_info = {}
    db_info.update(cf['db_info'])
    dbtype = db_info['db_type']
    db = SixDB(db_info)
    wu_id = str(wu_id)
    where = 'wu_id=%s' % wu_id
    outputs = db.select('sixtrack_wu', ['input_file', 'preprocess_id', 'boinc',
                                        'job_name', 'task_id'], where)
    boinc_infos = db.select('env', ['boinc_work', 'boinc_results',
                                    'surv_percent'])
    if not outputs:
        content = "Input file not found for preprocess job %s!" % wu_id
        raise FileNotFoundError(content)

    preprocess_id = outputs[0][1]
    boinc = outputs[0][2]
    input_buf = outputs[0][0]
    job_name = outputs[0][3]
    task_id = outputs[0][4]
    input_file = utils.decompress_buf(input_buf, None, 'buf')
    cf.clear()
    cf.read_string(input_file)
    sixtrack_config = cf['sixtrack']
    inp = sixtrack_config["input_files"]
    input_files = utils.decode_strings(inp)
    where = 'wu_id=%s' % str(preprocess_id)
    pre_task_id = db.select('preprocess_wu', ['task_id'], where)
    if not pre_task_id:
        raise Exception("Can't find the preprocess task_id for this job!")

    inputs = list(input_files.values())
    pre_task_id = pre_task_id[0][0]
    where = 'task_id=%s' % str(pre_task_id)
    input_buf = db.select('preprocess_task', inputs, where)
    db.close()
    if not input_buf:
        raise FileNotFoundError("The required files were not found!")

    for infile in inputs:
        i = inputs.index(infile)
        buf = input_buf[0][i]
        utils.decompress_buf(buf, infile)
    fort3_config = cf['fort3']
    boinc_vars = cf['boinc']
    if not boinc_infos:
        boinc = 'false'
        boinc_work = ''
        boinc_results = ''
        surv_percent = 1
        logger.error("There isn't a valid boinc path to submit this job!")
    else:
        boinc_work = boinc_infos[0][0]
        boinc_results = boinc_infos[0][1]
        surv_percent = boinc_infos[0][2]
    sixtrack_config['boinc'] = boinc
    sixtrack_config['task_id'] = str(task_id)
    sixtrack_config['boinc_work'] = boinc_work
    sixtrack_config['boinc_results'] = boinc_results
    sixtrack_config['surv_percent'] = str(surv_percent)
    sixtrack_config['job_name'] = job_name
    sixtrack_config['wu_id'] = wu_id

    try:
        sixtrackjob(sixtrack_config, fort3_config, boinc_vars)
    except Exception:
        logger.error('sixtrackjob failed!', exc_info=True)

    if dbtype.lower() == 'sql':
        dest_path = sixtrack_config["dest_path"]
    else:
        dest_path = './result'
    if not os.path.isdir(dest_path):
        os.makedirs(dest_path)
    inp = sixtrack_config["output_files"]
    output_files = utils.decode_strings(inp)
    down_list = list(output_files)
    down_list.append('fort.3')

    try:
        utils.download_output(down_list, dest_path)
        logger.info("All requested results have been stored in %s" % dest_path)
    except Exception:
        logger.error("Job failed!", exc_info=True)
    else:
        if boinc.lower() == 'true':
            down_list = ['fort.3']
            dest_path = sixtrack_config["dest_path"]
            utils.download_output(down_list, dest_path)
            return

    if dbtype.lower() == 'sql':
        return

    try:
        # Reconnect after job finished
        db = SixDB(db_info)
        f10_sec = cf['f10']
        job_table = {}
        task_table = {}
        task_table['status'] = 'Success'
        job_path = dest_path
        result_cf = {}
        for sec in cf:
            result_cf[sec] = dict(cf[sec])
        filelist = Table.result_table(output_files.values())
        parse_results(wu_id, job_path, filelist, task_table, result_cf)
        where = 'task_id=%s' % task_id
        db.update('sixtrack_task', task_table, where)
        for sec, vals in result_cf.items():
            vals['task_id'] = [task_id,]*len(vals['mtime'])
            db.insertm(sec, vals)
        if task_table['status'] == 'Success':
            job_table['status'] = 'complete'
            job_table['mtime'] = int(time.time() * 1E7)
            where = "wu_id=%s" % wu_id
            db.update('sixtrack_wu', job_table, where)
            content = "Sixtrack job %s has completed normally!" % wu_id
            logger.info(content)
        else:
            where = "wu_id=%s" % wu_id
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            db.update('sixtrack_wu', job_table, where)
            content = "The sixtrack job failed!"
            logger.warning(content)
    except Exception:
        where = "wu_id=%s" % wu_id
        job_table['status'] = 'incomplete'
        job_table['mtime'] = int(time.time() * 1E7)
        db.update('sixtrack_wu', job_table, where)
        logger.error('Error during reconnection.', exc_info=True)
    finally:
        db.close()


def sixtrackjob(sixtrack_config, config_param, boinc_vars):
    '''The actual sixtrack job'''
    fort3_config = config_param
    real_turn = fort3_config['turnss']
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    source_path = sixtrack_config["source_path"]
    inp = sixtrack_config["temp_files"]
    temp_files = utils.decode_strings(inp)
    inp = sixtrack_config["input_files"]
    input_files = utils.decode_strings(inp)
    inp = sixtrack_config["output_files"]
    output_files = utils.decode_strings(inp)
    add_inputs = []
    if 'additional_input' in sixtrack_config.keys():
        inp = sixtrack_config["additional_input"]
        add_inputs = utils.decode_strings(inp)
    boinc = sixtrack_config["boinc"]
    requires = temp_files + add_inputs
    for infile in requires:
        infi = os.path.join(source_path, infile)
        if os.path.isfile(infi):
            shutil.copy2(infi, infile)
        else:
            raise FileNotFoundError("The required file %s isn't found!" % infile)

    with open('fort.3.aux', 'r') as fc3aux:
        fc3aux_lines = fc3aux.readlines()
    fc3aux_2 = fc3aux_lines[1]
    c = fc3aux_2.split()
    lhc_length = c[4]
    fort3_config['length'] = lhc_length

    # Create a temp folder to excute sixtrack
    if os.path.isdir('junk'):
        shutil.rmtree('junk')
    os.mkdir('junk')
    os.chdir('junk')

    logger.info("Preparing the sixtrack input files!")

    # prepare the other input files
    for key in list(input_files.values())+add_inputs:
        key1 = os.path.join('../', key)
        if os.path.isfile(key1):
            os.symlink(key1, key)
        else:
            raise FileNotFoundError("The required input file %s does not found!" %
                                    key)

    #if os.path.isfile('../fort.2') and os.path.isfile('../fort.16'):
    #    os.symlink('../fort.2', 'fort.2')
    #    if not os.path.isfile('../fort.16'):
    #        os.symlink('../fort.16', 'fort.16')
    #    if not os.path.isfile('../fort.8'):
    #        open('fort.8', 'a').close()
    #    else:
    #        os.symlink('../fort.8', 'fort.8')

    if boinc.lower() == 'true':
        test_turn = sixtrack_config["test_turn"]
        fort3_config['turnss'] = test_turn
    keys = list(fort3_config.keys())
    patterns = ['%' + a for a in keys]
    values = [fort3_config[key] for key in keys]
    output = []
    for s in temp_files:
        dest = s + '.temp'
        source = os.path.join('../', s)
        try:
            utils.replace(patterns, values, source, dest)
        except Exception:
            raise Exception("Failed to generate input file for oneturn sixtrack!")

        output.append(dest)
    temp1 = input_files['fc.3']
    temp1 = os.path.join('../', temp1)
    if os.path.isfile(temp1):
        output.insert(1, temp1)
    else:
        raise FileNotFoundError("The %s file doesn't exist!" % temp1)

    utils.concatenate_files(output, 'fort.3')
    #utils.diff(source, 'fort.3', logger=logger)

    # actually run
    wu_id = sixtrack_config['wu_id']
    logger.info('Sixtrack job %s is running...' % wu_id)
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    output_name = os.path.join('../', 'sixtrack.output')
    with open(output_name, 'w') as six_out:
        six_out.writelines(outputlines)
    #if not os.path.isfile('fort.10'):
    #    content = ("The sixtrack job %s for chromaticity FAILED! "
    #               "Check the file %s which contains the SixTrack output.")
    #    raise Exception(content % (wu_id, output_name))
    #else:
    #    shutil.move('fort.10', '../fort.10')
    #    logger.info('Sixtrack job %s has completed normally!' % wu_id)
    if utils.check(output_files):
        for out in output_files:
            shutil.move(out, os.path.join('../', out))
    os.chdir('../')  # get out of junk folder
    if boinc.lower() != 'true':
        shutil.move('junk/fort.3', 'fort.3')
        if boinc.lower() != 'false':
            logger.warning("Unknown boinc flag %s!" % boinc)
    else:
        surv_per = sixtrack_config['surv_percent']
        surv_per = ast.literal_eval(surv_per)

        if not check_tracking('sixtrack.output', surv_per):
            raise Exception("The job doesn't pass the test!")

        boinc_work = sixtrack_config['boinc_work']
        boinc_results = sixtrack_config['boinc_results']
        job_name = sixtrack_config['job_name']
        task_id = sixtrack_config['task_id']
        st_pre = os.path.basename(os.path.dirname(boinc_work))
        job_name = st_pre + '__' + job_name + '_task_id_' + str(task_id)
        if not os.path.isdir(boinc_work):
            os.makedirs(boinc_work)
        if not os.path.isdir(boinc_results):
            os.makedirs(boinc_results)
        logger.info('The job passes the test and will be sumbitted to BOINC!')
        fort3_config['turnss'] = real_turn
        values = [fort3_config[key] for key in keys]
        output = []
        # recreate the fort.3 file
        for s in temp_files:
            dest = s + '.temp'
            try:
                utils.replace(patterns, values, s, dest)
            except Exception:
                raise Exception("Failed to generate input file for sixtrack!")

            output.append(dest)
        temp1 = input_files['fc.3']
        if os.path.isfile(temp1):
            output.insert(1, temp1)
        else:
            raise FileNotFoundError("The %s file doesn't exist!" % temp1)

        utils.concatenate_files(output, 'fort.3')
        #utils.diff(source, 'fort.3', logger=logger)

        # zip all the input files, e.g. fort.3 fort.2 fort.8 fort.16
        input_zip = job_name + '.zip'
        ziph = zipfile.ZipFile(input_zip, 'w', zipfile.ZIP_DEFLATED)
        inputs = ['fort.2', 'fort.3', 'fort.8', 'fort.16']
        for infile in inputs:
            if infile in os.listdir('.'):
                ziph.write(infile)
            else:
                raise FileNotFoundError("The required file %s isn't found!" % infile)

        ziph.close()

        boinc_config = job_name + '.desc'
        boinc_vars['workunitName'] = job_name
        with open(boinc_config, 'w') as f_out:
            pars = '\n'.join(boinc_vars.values())
            f_out.write(pars)
            f_out.write('\n')
        process = Popen(['cp', input_zip, boinc_config, boinc_work], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            logger.info(stdout)
            logger.error(stderr)
            raise Exception(stderr)
        else:
            logger.info("Submit to %s successfully!" % boinc_work)
            logger.info(stdout)


def check_tracking(filename, surv_percent=1):
    '''Check the tracking result to see how many particles survived'''
    with open(filename, 'r') as f_in:
        lines = f_in.readlines()
    try:
        track_lines = filter(lambda x: re.search(r'TRACKING>', x), lines)
        last_line = list(track_lines)[-1]
        info = re.split(r':|,', last_line)
        turn_info = info[1].split()
        part_info = info[-1].split()
        total_turn = ast.literal_eval(turn_info[-1])
        track_turn = ast.literal_eval(turn_info[1])
        total_part = ast.literal_eval(part_info[-1])
        surv_part = ast.literal_eval(part_info[0])
        if track_turn < total_turn:
            return 0
        else:
            if surv_part / total_part < surv_percent:
                return 0
            else:
                return 1
    except Exception as e:
        logger.error(e)
        return 0


if __name__ == '__main__':

    args = sys.argv
    num = len(args[1:])
    if num == 0 or num == 1:
        logger.error("The input file is missing!")
        sys.exit(1)
    elif num == 2:
        wu_id = args[1]
        db_name = args[2]
        run(wu_id, db_name)
        sys.exit(0)
    else:
        logger.error("Too many input arguments!")
        sys.exit(1)
