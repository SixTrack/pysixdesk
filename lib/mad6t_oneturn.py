#!/usr/bin/python3
import os
import sys
import copy
import shutil
import utils
import fileinput
import configparser

def run(input_file):
    cf = configparser.ConfigParser()
    cf.optionxform = str #preserve case
    cf.read(input_file)
    madx_config = cf['madx']
    mask_config = cf['mask']
    madxjob(madx_config, mask_config)

    sixtrack_config = cf['sixtrack']
    fort3_config = cf._sections['fort3']
    sixtrackjobs(sixtrack_config, fort3_config)

def madxjob(madx_config, mask_config):
    '''MADX job to generate input file for sixtrack'''
    madxexe = madx_config["madx_exe"]
    source_path = madx_config["source_path"]
    mask_name = madx_config["mask_name"]
    madx_input_name = madx_config["input_name"]
    output_files = madx_config["output_files"]
    output_files = utils.decode(output_files)
    if 'mask' not in mask_name:
        mask_name = mask_name + '.mask'
    mask_file = os.path.join(source_path, mask_name)
    shutil.copy2(mask_file, mask_name)
    dest_path = madx_config["dest_path"]
    if not os.path.isdir(dest_path):
        os.mkdir(dest_path)

    #Generate the actual madx file from mask file
    patterns = ['%'+a for a in mask_config.keys()]
    values = list(mask_config.values())
    utils.replace(patterns, values, mask_name, madx_input_name)

    #Begin to execute madx job
    command = madxexe + " " + madx_input_name
    print("Calling madx %s"%madxexe)
    print("MADX job is running...")
    output = os.popen(command)
    outputlines = output.readlines()
    mad_out = open('madx_stdout', 'w')
    mad_out.writelines(outputlines)
    if 'finished normally' not in outputlines[-2]:
        print("MADX has not completed properly!")
        sys.exit(1)
    else:
        print("MADX has completed properly!")

    #Check the existence of madx output
    status = utils.check(output_files)
    if not status:
        sys.exit(1) #The required files aren't generated normally,we need to quit
    #All the outputs are generated successfully,

    #Download the requested files.
    down_list = list(output_files.values())
    down_list.append(madx_input_name)
    down_list.append('madx_stdout')
    utils.download_output(down_list, dest_path)
    print("All requested files have zipped and downloaded to %s"%dest_path)

def sixtrackjobs(config, fort3_config):
    '''Manage all the one turn sixtrack job'''
    sixtrack_exe = config['sixtrack_exe']
    source_path = config["source_path"]
    temp_files = utils.decode(config["temp_files"])
    for s in temp_files:
        source = os.path.join(source_path, s)
        shutil.copy2(source, s)
    print('Calling sixtrack %s'%sixtrack_exe)
    sixtrackjob(config, fort3_config, 'first_oneturn', dp1='.000', dp2='.000')
    sixtrackjob(config, fort3_config, 'second_oneturn')

    #Calculate and write out the requested values
    tunes = open('sixdesktunes', 'w')
    tunes.write(fort3_config['chrom_eps'])
    tunes.write('\n')
    first = open('fort.10_first_oneturn')
    a = first.readline()
    valf = a.split()
    first.close()
    second = open('fort.10_second_oneturn')
    b = second.readline()
    vals = b.split()
    tunes.write(valf[2]+" "+valf[3])
    tunes.write('\n')
    tunes.write(vals[2]+" "+vals[3])
    tunes.write('\n')
    tunes.close()
    chrom_eps = fort3_config['chrom_eps']
    chrom1 = (float(vals[2])-float(valf[2]))/float(chrom_eps)
    chrom1 = str(chrom1)
    chrom2 = (float(vals[3])-float(valf[3]))/float(chrom_eps)
    chrom2 = str(chrom2)
    chrom = open('mychrom', 'w')
    chrom.write(chrom1+" "+chrom2)
    chrom.write('\n')
    chrom.close()

    sixtrackjob(config, fort3_config, 'beta_oneturn', dp1='.000', dp2='.000')
    f_in = open('fort.10_beta_oneturn', 'r')
    beta_line = f_in.readline()
    f_in.close()
    beta = beta_line.split()
    f_out = open('betavalues', 'w')
    beta_out = [beta[4], beta[47], beta[5], beta[48], beta[2], beta[3],\
                beta[49], beta[50], beta[52], beta[53], beta[54], beta[55],\
                beta[56], beta[57]]
    if fort3_config['CHROM'] == '0':
        beta_out[6] = chrom1
        beta_out[7] = chrom2
    for a in beta_out:
        f_out.write(str(a))
        if a != beta_out[-1]:
            f_out.write(' ')
        else:
            f_out.write('\n')
    f_out.close()
    #Download the requested files
    dest_path = config["dest_path"]
    b = ['sixdesktunes', 'mychrom', 'betavalues']
    utils.download_output(b, dest_path, False)

def sixtrackjob(config, config_re, jobname, **args):
    '''One turn sixtrack job'''
    sixtrack_config = config
    fort3_config = copy.deepcopy(config_re)
    source_path = sixtrack_config["source_path"]
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    temp_files = utils.decode(sixtrack_config["temp_files"])
    input_files = utils.decode(sixtrack_config["input_files"])
    fc3aux = open('fort.3.aux', 'r')
    fc3aux_lines = fc3aux.readlines()
    fc3aux_2 = fc3aux_lines[1]
    c = fc3aux_2.split()
    lhc_length = c[4]
    fort3_config['length']=lhc_length
    fort3_config.update(args)

    #Create a temp folder to excute sixtrack
    if os.path.isdir('junk'):
        shutil.rmtree('junk')
    os.mkdir('junk')
    os.chdir('junk')

    print("Preparing the sixtrack input files!")

    keys = list(fort3_config.keys())
    patterns = ['%'+a for a in keys]
    values = list(fort3_config.values())
    output = []
    for s in temp_files:
        dest = s+".t1"
        source = os.path.join('../', s)
        utils.replace(patterns, values, source, dest)
        output.append(dest)
    temp1 = input_files['fc.3']
    temp1 = os.path.join('../', temp1)
    if os.path.isfile(temp1):
        output.insert(1, temp1)
    else:
        print("The %s file doesn't exist!"%temp1)
        sys.exit(1)
    concatenate_files(output, 'fort.3')

    #prepare the other input files
    if os.path.isfile('../fort.2') and os.path.isfile('../fort.16'):
        os.symlink('../fort.2', 'fort.2')
        os.symlink('../fort.16', 'fort.16')
        if not os.path.isfile('../fort.8'):
            open('fort.8', 'a').close()
        else:
            os.symlink('../fort.8', 'fort.8')

    #actually run
    print('Sixtrack job %s is runing...'%jobname)
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    output_name = '../' + jobname + '.output'
    six_out = open(output_name, 'w')
    six_out.writelines(outputlines)
    if not os.path.isfile('fort.10'):
        print("The %s sixtrack job for chromaticity FAILED!"%jobname)
        print("Check the file %s which contains the SixTrack fort.6 output."%output_name)
        sys.exit(1)
    else:
        result_name = '../fort.10' + '_' + jobname
        shutil.move('fort.10', result_name)
        print('Sixtrack job %s has completed normally!'%jobname)

    #Get out the temp folder
    os.chdir('../')

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
    if num == 0:
        print("The input file is missing!")
        sys.exit(1)
    elif num == 1:
        input_file = args[1]
        run(input_file)
    else:
        print("Too many input arguments!")
        sys.exit(1)
