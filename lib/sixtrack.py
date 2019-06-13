#!/usr/bin/python3
import os
import sys
import copy
import utils
import time
import shutil
import traceback
import zipfile
import getpass
import configparser
import resultparser as rp

from pysixdb import SixDB
from subprocess import Popen, PIPE

def run(wu_id, input_info):
    cf = configparser.ConfigParser()
    cf.optionxform = str #preserve case
    cf.read(input_info)
    db_info = {}
    db_info.update(cf['db_info'])
    dbtype = db_info['db_type']
    db = SixDB(db_info)
    wu_id = str(wu_id)
    where = 'wu_id=%s'%wu_id
    outputs = db.select('sixtrack_wu', ['input_file', 'preprocess_id', 'boinc',
        'job_name'], where)
    boinc_paths = db.select('env', ['boinc_work', 'boinc_results'])
    if not outputs:
        print("There isn't input file for sixtrack job %s!"%wu_id)
        db.close()
        return 0
    preprocess_id = outputs[0][1]
    boinc = outputs[0][2]
    input_buf = outputs[0][0]
    job_name = outputs[0][3]
    input_file = utils.evlt(utils.decompress_buf, [input_buf, None, 'buf'])
    cf.clear()
    cf.read_string(input_file)
    sixtrack_config = cf['sixtrack']
    inp = sixtrack_config["input_files"]
    input_files = utils.evlt(utils.decode_strings, [inp])
    where = 'wu_id=%s'%str(preprocess_id)
    task_id = db.select('preprocess_wu', ['task_id'], where)
    if not task_id:
        print("Can't find the preprocess task_id for this job!")
        return 0
    inputs = list(input_files.values())
    task_id = task_id[0][0]
    where = 'task_id=%s'%str(task_id)
    input_buf = db.select('preprocess_task', inputs, where)
    db.close()
    if not input_buf:
        print("The required files aren't found!")
        return 0
    for infile in inputs:
        i = inputs.index(infile)
        buf = input_buf[0][i]
        utils.evlt(utils.decompress_buf, [buf, infile])
    fort3_config = cf['fort3']
    boinc_vars = cf['boinc']
    if not boinc_paths:
        boinc = 'false'
        print("There isn't valid boinc path to submit this job!")
    else:
        boinc_work = boinc_paths[0][0]
        boinc_results = boinc_paths[0][1]
    sixtrack_config['boinc'] = boinc
    sixtrack_config['boinc_work'] = boinc_work
    sixtrack_config['boinc_results'] = boinc_results
    sixtrack_config['job_name'] = job_name
    sixtrack_config['wu_id'] = wu_id
    status = sixtrackjob(sixtrack_config, fort3_config, boinc_vars)
    if dbtype.lower() == 'sql':
        dest_path = sixtrack_config["dest_path"]
    else:
        dest_path = './result'
    if not os.path.isdir(dest_path):
        os.makedirs(dest_path)
    inp = sixtrack_config["output_files"]
    output_files = utils.evlt(utils.decode_strings, [inp])
    down_list = list(output_files)
    down_list.append('fort.3')
    status = utils.download_output(down_list, dest_path)
    if status:
        print("All requested results have stored in %s"%dest_path)
    else:
        print("Job failed!")

    if dbtype.lower() == 'sql':
        return status

    if boinc.lower() == 'true':
        return status

    try:
        #Reconnect after job finished
        db = SixDB(db_info)
        f10_sec = cf['f10']
        job_table = {}
        task_table = {}
        f10_table = {}
        task_table['status'] = 'Success'
        job_path = dest_path
        rp.parse_sixtrack(wu_id, job_path, output_files, task_table,
                f10_table, list(f10_sec.keys()))
        db.insert('sixtrack_task', task_table)
        where = "mtime='%s' and wu_id=%s"%(task_table['mtime'], wu_id)
        task_id = db.select('sixtrack_task', ['task_id'], where)
        task_id = task_id[0][0]
        f10_table['six_input_id'] = [task_id,]*len(f10_table['mtime'])
        db.insertm('six_results', f10_table)
        if task_table['status'] == 'Success':
            job_table['status'] = 'complete'
            job_table['task_id'] = task_id
            job_table['mtime'] = str(time.time())
            where = "wu_id=%s"%wu_id
            db.update('sixtrack_wu', job_table, where)
            content = "Sixtrack job %s has completed normally!"%wu_id
            utils.message('Message', content)
        else:
            where = "wu_id=%s"%wu_id
            job_table['status'] = 'incomplete'
            job_table['mtime'] = str(time.time())
            db.update('sixtrack_wu', job_table, where)
            content = "The sixtrack job failed!"
            utils.message('Warning', content)
        return status
    except:
        where = "wu_id=%s"%wu_id
        job_table['status'] = 'incomplete'
        job_table['mtime'] = str(time.time())
        db.update('sixtrack_wu', job_table, where)
        content = traceback.print_exc()
        utils.message('Error', content)
        return False
    finally:
        db.close()

def sixtrackjob(sixtrack_config, config_param, boinc_vars):
    '''The actual sixtrack job'''
    six_status = 1
    fort3_config = config_param
    real_turn = fort3_config['turnss']
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    source_path = sixtrack_config["source_path"]
    dest_path = sixtrack_config["dest_path"]
    inp = sixtrack_config["temp_files"]
    temp_files = utils.evlt(utils.decode_strings, [inp])
    inp = sixtrack_config["output_files"]
    output_files = utils.evlt(utils.decode_strings, [inp])
    inp = sixtrack_config["input_files"]
    input_files = utils.evlt(utils.decode_strings, [inp])
    boinc = sixtrack_config["boinc"]
    for infile in temp_files:
        infi = os.path.join(source_path, infile)
        if os.path.isfile(infi):
            shutil.copy2(infi, infile)
        else:
            print("The required file %s isn't found!"%infile)
            six_status = 0
            return six_status

    fc3aux = open('fort.3.aux', 'r')
    fc3aux_lines = fc3aux.readlines()
    fc3aux_2 = fc3aux_lines[1]
    c = fc3aux_2.split()
    lhc_length = c[4]
    fort3_config['length']=lhc_length

    #Create a temp folder to excute sixtrack
    if os.path.isdir('junk'):
        shutil.rmtree('junk')
    os.mkdir('junk')
    os.chdir('junk')

    print("Preparing the sixtrack input files!")

    #prepare the other input files
    if os.path.isfile('../fort.2') and os.path.isfile('../fort.16'):
        os.symlink('../fort.2', 'fort.2')
        os.symlink('../fort.16', 'fort.16')
        if not os.path.isfile('../fort.8'):
            open('fort.8', 'a').close()
        else:
            os.symlink('../fort.8', 'fort.8')
    else:
        print("There isn't the required input files for sixtrack!")
        six_status = 0
        return six_status

    if boinc.lower() == 'true':
        test_turn = sixtrack_config["test_turn"]
        fort3_config['turnss'] = test_turn
    keys = list(fort3_config.keys())
    patterns = ['%'+a for a in keys]
    values = [fort3_config[key] for key in keys]
    output = []
    for s in temp_files:
        dest = s + '.temp'
        source = os.path.join('../', s)
        status = utils.replace(patterns, values, source, dest)
        if not status:
            print("Failed to generate input file for oneturn sixtrack!")
            six_status = 0
            return six_status
        output.append(dest)
    temp1 = input_files['fc.3']
    temp1 = os.path.join('../', temp1)
    if os.path.isfile(temp1):
        output.insert(1, temp1)
    else:
        print("The %s file doesn't exist!"%temp1)
        six_status = 0
        return six_status
    concatenate_files(output, 'fort.3')

    #actually run
    wu_id = sixtrack_config['wu_id']
    print('Sixtrack job %s is runing...'%wu_id)
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    output_name = os.path.join('../', 'sixtrack.output')
    with open(output_name, 'w') as six_out:
        six_out.writelines(outputlines)
    if not os.path.isfile('fort.10'):
        print("The sixtrack job %s for chromaticity FAILED!"%wu_id)
        print("Check the file %s which contains the SixTrack fort.6 output."%output_name)
        six_status = 0
        return six_status
    else:
        result_name = '../fort.10'
        shutil.move('fort.10', result_name)
        shutil.move('fort.3','../fort.3')
        print('Sixtrack job %s has completed normally!'%wu_id)
    os.chdir('../') #get out of junk folder

    if boinc.lower() == 'true':
        boinc_work = sixtrack_config['boinc_work']
        boinc_results = sixtrack_config['boinc_results']
        job_name = sixtrack_config['job_name']
        if not os.path.isdir(boinc_work):
            os.makedirs(boinc_work)
        if not os.path.isdir(boinc_results):
            os.makedirs(boinc_results)
        print('The job passes the test and will be sumbitted to BOINC!')
        fort3_config['turnss'] = real_turn
        values = [fort3_config[key] for key in keys]
        output = []
        #recreate the fort.3 file
        for s in temp_files:
            dest = s + '.temp'
            status = utils.replace(patterns, values, s, dest)
            if not status:
                print("Failed to generate input file for sixtrack!")
                six_status = 0
                return six_status
            output.append(dest)
        temp1 = input_files['fc.3']
        if os.path.isfile(temp1):
            output.insert(1, temp1)
        else:
            print("The %s file doesn't exist!"%temp1)
            six_status = 0
            return six_status
        concatenate_files(output, 'fort.3')

        #zip all the input files, e.g. fort.3 fort.2 fort.8 fort.16
        input_zip = job_name + '.zip'
        ziph = zipfile.ZipFile(input_zip, 'w', zipfile.ZIP_DEFLATED)
        inputs = list(input_files.values())
        for infile in inputs:
            if infile in os.listdir('.'):
                ziph.write(infile)
            else:
                print("The required file %s isn't found!"%infile)
                six_status = 0
                return six_status
        ziph.close()

        boinc_config = job_name + '.desc'
        boinc_vars['workunitName'] = job_name
        with open(boinc_config, 'w') as f_out:
            pars = '\n'.join(boinc_vars.values())
            f_out.write(pars)
            f_out.write('\n')
        process = Popen(['cp', input_zip, boinc_config, boinc_work], stdout=PIPE,\
                stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            print(stdout)
            print(stderr)
            six_status = 0
        else:
            print("Submit to %s successfully!"%boinc_work)
            print(stdout)
            os.system('ll %s'%boinc_work)
            six_status = 1
    return six_status

def concatenate_files(source, dest):
    '''Concatenate the given files'''
    f_out = open(dest, 'w')
    if type(source) is list:
        for s_in in source:
            f_in = open(s_in, 'r')
            f_out.writelines(f_in.readlines())
            f_in.close()
    else:
        f_in = open(source, 'r')
        f_out.writelines(f_in.readlines())
        f_in.close()
    f_out.close()

if __name__ == '__main__':
    args = sys.argv
    num = len(args[1:])
    if num == 0 or num == 1:
        print("The input file is missing!")
        sys.exit(1)
    elif num == 2:
        wu_id = args[1]
        db_name = args[2]
        run(wu_id, db_name)
    else:
        print("Too many input arguments!")
        sys.exit(1)
