#!/usr/bin/python3
import os
import sys
import copy
import utils
import shutil
import zipfile
import configparser

from pysixdb import SixDB

def run(wu_id, db_name):
    db = SixDB(db_name)
    wu_id = str(wu_id)
    where = 'wu_id=%s'%wu_id
    outputs = db.select('sixtrack_wu', ['input_file', 'preprocess_id', 'boinc'], where)
    #db.close()
    if not outputs:
        print("There isn't input file for sixtrack job %s!"%wu_id)
        db.close()
        sys.exit(1)
    preprocess_id = outputs[0][1]
    boinc = outputs[0][2]
    input_buf = outputs[0][0]
    input_file = utils.evlt(utils.decompress_buf, [input_buf, None, 'buf'])
    cf = configparser.ConfigParser()
    cf.optionxform = str #preserve case
    cf.read_string(input_file)
    sixtrack_config = cf['sixtrack']
    inp = sixtrack_config["input_files"]
    input_files = utils.evlt(utils.decode_strings, [inp])
    where = 'wu_id=%s'%str(preprocess_id)
    task_id = db.select('preprocess_wu', ['task_id'], where)
    if not task_id:
        print("Can't find the preprocess task_id for this job!")
        sys.exit(1)
    inputs = list(input_files.values())
    task_id = task_id[0][0]
    where = 'task_id=%s'%str(task_id)
    input_buf = db.select('preprocess_task', inputs, where)
    db.close()
    if not input_buf:
        print("The required file %s isn't found!"%infile)
        sys.exit(1)

    for infile in inputs:
        i = inputs.index(infile)
        buf = input_buf[0][i]
        utils.evlt(utils.decompress_buf, [buf, infile])
    fort3_config = cf['fort3']
    boinc_vars = cf['boinc']
    sixtrack_config['boinc'] = boinc
    sixtrack_config['wu_id'] = wu_id
    sixtrackjob(sixtrack_config, fort3_config, boinc_vars)

def sixtrackjob(sixtrack_config, config_param, boinc_vars):
    '''The actual sixtrack job'''
    fort3_config = config_param
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    source_path = sixtrack_config["source_path"]
    #input_path = sixtrack_config["input_path"]
    dest_path = sixtrack_config["dest_path"]
    inp = sixtrack_config["temp_files"]
    temp_files = utils.evlt(utils.decode_strings, [inp])
    inp = sixtrack_config["output_files"]
    output_files = utils.evlt(utils.decode_strings, [inp])
    inp = sixtrack_config["input_files"]
    input_files = utils.evlt(utils.decode_strings, [inp])
    boinc = sixtrack_config["boinc"]
    #test_turn = sixtrack_config["test_turn"]
    for infile in temp_files:
        infi = os.path.join(source_path, infile)
        if os.path.isfile(infi):
            shutil.copy2(infi, infile)
        else:
            print("The required file %s isn't found!"%infile)
            sys.exit(1)

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
        sys.exit(1)

    #if boinc.lowercase() is 'true':
    #    fort3_config['turn'] = test_turn
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
            sys.exit(1)
        output.append(dest)
    temp1 = input_files['fc.3']
    temp1 = os.path.join('../', temp1)
    if os.path.isfile(temp1):
        output.insert(1, temp1)
    else:
        print("The %s file doesn't exist!"%temp1)
        sys.exit(1)
    concatenate_files(output, 'fort.3')

    #actually run
    wu_id = sixtrack_config['wu_id']
    print('Sixtrack job %s is runing...'%wu_id)
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    output_name = os.path.join('../', 'sixtrack.output')
    six_out = open(output_name, 'w')
    six_out.writelines(outputlines)
    if not os.path.isfile('fort.10'):
        print("The %s sixtrack job for chromaticity FAILED!"%jobname)
        print("Check the file %s which contains the SixTrack fort.6 output."%output_name)
        sys.exit(1)
    else:
        result_name = '../fort.10'
        shutil.move('fort.10', result_name)
        shutil.move('fort.3','../fort.3')
        print('Sixtrack job %s has completed normally!'%jobname)
    os.chdir('../') #get out of junk folder
    down_list = output_files
    down_list.append('fort.3')
    status = utils.download_output(down_list, dest_path)

    if boinc.lower() is 'true':
        print('The job passes the test and will be sumbitted to BOINC!')
        os.chdir('../')#get out of junk folder
        fort3_config['turn'] = config_param['turn']
        values = [fort3_config[key] for key in keys]
        output = []
        #recreate the fort.3 file
        for s in temp_files:
            dest = s + '.temp'
            status = utils.replace(patterns, values, s, dest)
            if not status:
                print("Failed to generate input file for oneturn sixtrack!")
                sys.exit(1)
            output.append(dest)
        temp1 = input_files['fc.3']
        if os.path.isfile(temp1):
            output.insert(1, temp1)
        else:
            print("The %s file doesn't exist!"%temp1)
            sys.exit(1)
        concatenate_files(output, 'fort.3')

        ziph = zipfile.ZipFile('test.zip', 'w', zipfile.ZIP_DEFLATED)
        for infile in inputs:
            if infile in os.listdir('.'):
                ziph.write(infile)
            else:
                print("The required file %s isn't found!"%infile)
                sys.exit(1)
        ziph.close()
        #TODO

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
