import os
import sys
import copy
import utils
import shutil
import zipfile
import configparser

def run(input_file):
    cf = configparser.ConfigParser()
    cf.optionxform = str #preserve case
    cf.read(input_file)
    sixtrack_config = cf['sixtrack']
    fort3_config = cf['fort3']
    boinc_vars = cf['boinc']
    sixtrackjob(sixtrack_config, fort3_config, boinc_vars)

def sixtrackjob(sixtrack_config, config_param, boinc_vars):
    '''The actual sixtrack job'''
    fort3_config = copy.deepcopy(config_param)
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    source_path = sixtrack_config["source_path"]
    status, temp_files = utils.decode_strings(sixtrack_config["temp_files"])
    inputs = sixtrack_config["input"]
    boinc = sixtrack_config["Boinc"]
    test_turn = sixtrack_config["test_turn"]
    if not status:
        print("Wrong setting of oneturn sixtrack templates!")
        sys.exit(1)
    status, input_files = utils.decode_strings(sixtrack_config["input_files"])
    if not status:
        print("Wrong setting of oneturn sixtrack input!")
        sys.exit(1)

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

    if boinc.lowercase() is 'true':
        fort3_config['turn'] = test_turn
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
    print('Sixtrack job %s is runing...'%jobname)
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
        print('Sixtrack job %s has completed normally!'%jobname)

    if boinc.lowercase() is 'true':
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
