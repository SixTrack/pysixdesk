#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import sys
import time
import copy
import utils
import shutil
import traceback
import configparser
import generate_fort2
import resultparser as rp

from pysixdb import SixDB

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
    outputs = db.select('collimation_wu', ['input_file'], where)
    db.close()
    if not outputs[0]:
        print("There isn't input file for preprocess job %s!" % wu_id)
        return 0
    input_buf = outputs[0][0]
    input_file = utils.evlt(utils.decompress_buf, [input_buf, None, 'buf'])
    cf.clear()
    cf.read_string(input_file)
    coll_config = cf['collimation']
    mask_config = cf['mask']
    fort3_config = cf['fort3']
    status = madxjob(coll_config, mask_config)
    if status:
        # fc2 = fort2_config['fc2']
        inp = coll_config['input_files']
        inputfiles = utils.evlt(utils.decode_strings, [inp])
        source_path = madx_config["source_path"]
        for fil in inputfiles:
            fl = os.path.join(source_path, fil)
            shutil.copy2(fl, fil)
        fc2 = 'fc.2'
        aperture = 'allapert.b1'
        survery = 'SurveryWithCrossing_XP_lowb.dat'
        status = generate_fort2.run(fc2, aperture, survery)
    elif dbtype.lower == 'sql':
        content = "The madx job failed!"
        utils.message('Warning', content)
        return status

    if status:
        status = sixtrackjob(coll_config, fort3_cofig)
    elif dbtype.lower == 'sql':
        content = "The generation of fort.2 failed!"
        utils.message('Warning', content)
        return status

    if dbtype.lower() == 'mysql':
        dest_path = './result'
    else:
        dest_path = madx_config["dest_path"]
    if not os.path.isdir(dest_path):
        os.makedirs(dest_path)
    # Download the requested files.
    down_list = list(output_files.values())
    # down_list.append('madx_in')
    # down_list.append('madx_stdout')
    status = utils.download_output(down_list, dest_path)
    if status:
        print("All requested results have stored in %s" % dest_path)
    else:
        print("Job failed!")
    if dbtype.lower() == 'sql':
        return status

    try:
        pass
    except:
        pass
    finally:
        pass


def madxjob(madx_config, mask_config):
    '''madx job to generate fort.2 file'''
    madxexe = madx_config["madx_exe"]
    source_path = madx_config["source_path"]
    mask_name = madx_config["mask_file"]
    output_files = madx_config["output_files"]
    status, output_files = utils.decode_strings(output_files)
    if not status:
        print("Wrong setting of madx output!")
        return 0
    if 'mask' not in mask_name:
        mask_name = mask_name + '.mask'
    mask_file = os.path.join(source_path, mask_name)
    shutil.copy2(mask_file, mask_name)

    # Generate the actual madx file from mask file
    patterns = ['%'+a for a in mask_config.keys()]
    values = list(mask_config.values())
    madx_in = 'madx_in'
    status = utils.replace(patterns, values, mask_name, madx_in)
    if not status:
        print("Failed to generate actual madx input file!")
        return 0

    # Begin to execute madx job
    command = madxexe + " " + madx_in
    print("Calling madx %s" % madxexe)
    print("MADX job is running...")
    output = os.popen(command)
    outputlines = output.readlines()
    with open('madx_stdout', 'w') as mad_out:
        mad_out.writelines(outputlines)
    if 'finished normally' not in outputlines[-2]:
        print("MADX has not completed properly!")
        return 0
    else:
        print("MADX has completed properly!")

    # Check the existence of madx output
    status = utils.check(output_files)
    if not status:
        return status  # The required files aren't generated normally,we need to quit
    return 1


def sixtrackjob(sixtrack_config, fort3_config):
    '''run actual sixtrack job'''
    sixtrack_config = config
    source_path = sixtrack_config["source_path"]
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    status, temp_files = utils.decode_strings(sixtrack_config["temp_files"])
    if not status:
        print("Wrong setting of oneturn sixtrack templates!")
        return 0
    status, input_files = utils.decode_strings(sixtrack_config["input_files"])
    if not status:
        print("Wrong setting of oneturn sixtrack input!")
        return 0
    for tmp in temp_files:
        source = os.path.join(source_path, tmp)
        shutil.copy2(source, tmp)
    # fc3aux = open('fort.3.aux', 'r')
    # fc3aux_lines = fc3aux.readlines()
    # fc3aux_2 = fc3aux_lines[1]
    # c = fc3aux_2.split()
    # lhc_length = c[4]
    # fort3_config['length'] = lhc_length
    # fort3_config.update(args)

    # Create a temp folder to excute sixtrack
    if os.path.isdir('junk'):
        shutil.rmtree('junk')
    os.mkdir('junk')
    os.chdir('junk')

    print("Preparing the sixtrack input files!")
    keys = list(fort3_config.keys())
    patterns = ['%'+a for a in keys]
    values = [fort3_config[key] for key in keys]
    output = []
    for s in temp_files:
        dest = s
        source = os.path.join('../', s)
        status = utils.replace(patterns, values, source, dest)
        if not status:
            print("Failed to generate input file for sixtrack job!")
            return 0
    # temp1 = input_files['fc.3']
    # temp1 = os.path.join('../', temp1)
    # if os.path.isfile(temp1):
    #     output.insert(1, temp1)
    # else:
    #     print("The %s file doesn't exist!" % temp1)
    #     return 0
    # utils.concatenate_files(output, 'fort.3')

    # prepare the other input files
    input_files.append('fort.2')
    for fils in input_files:
        fs = os.path.join('../', fils)
        if os.path.isfile(fs):
            os.symlink(fs, fils)
        else:
            print("The required file %s doesn't exist!" % fils)
            continue

    # actually run
    print('Sixtrack job is runing...')
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    with open('../coll.output', 'w') as six_out:
        six_out.writelines(outputlines)
    #if not os.path.isfile('fort.10'):
    #    return 0
    #else:
    #    result_name = '../fort.10' + '_' + jobname
    #    shutil.move('fort.10', result_name)
    #    print('Sixtrack job %s has completed normally!' % jobname)

    # Get out the temp folder
    os.chdir('../')
    return 1


if __name__ == "__main__":
    args = sys.argv
    num = len(args[1:])
    if num == 0 or num == 1:
        print("The input file is missing!")
        sys.exit(1)
    elif num == 2:
        wu_id = args[1]
        input_info = args[2]
        run(wu_id, input_info)
    else:
        print("Too many input arguments!")
        sys.exit(1)
