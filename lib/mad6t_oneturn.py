#!/usr/bin/python3
import os
import re
import sys
import copy
import gzip
import shutil
import configparser

def run(args):
    num = len(args[1:])
    if num == 0:
        print("The input file is missing!")
        sys.exit(1)
    elif num == 1:
        #TODO(xiaohan):Should we use ty-except statement to aviod crash due to wrong input? 
        input_file = args[1]
        cf = configparser.ConfigParser()
        cf.optionxform = str #preserve case
        cf.read(input_file)
        madx_config = cf['madx']
        mask_config = cf['mask']
        madxjob(madx_config, mask_config)

        sixtrack_config = cf['sixtrack']
        fort3_config = cf._sections['fort3']
        sixtrackjobs(sixtrack_config, fort3_config)
    else:
        print("Too many input arguments!")
        sys.exit(1)

def madxjob(madx_config, mask_config):
    '''MADX job to generate input file for sixtrack'''
    madxexe = madx_config["madx_exe"]
    source_path = madx_config["source_path"]
    mask_name = madx_config["mask_name"]
    if 'mask' not in mask_name:
        mask_name = mask_name + '.mask'
    mask_file = os.path.join(source_path, mask_name)
    seed = mask_config["SEEDRAN"]
    dest_path = madx_config["dest_path"]
    if os.path.isdir(dest_path):
        os.mkdir(dest_path)
    madx_file = os.path.join('.', mask_name+'.'+seed)

    #Generate the actual madx file from mask file
    patterns = ['%'+a for a in mask_config.keys()]
    values = list(mask_config.values())
    #TODO copy mask file
    replace(patterns, values, mask_file, madx_file)

    #Begin to execute madx job
    command = madxexe + " " + madx_file
    print("Calling madx %s"%madxexe)
    print("MADX job is running...")
    output = os.popen(command)
    outputlines = output.readlines()
    mad_out = open('mad_out', 'w')
    mad_out.writelines(outputlines)
    if 'finished normally' not in outputlines[-2]:
        print("MADX has not completed properly!")
        sys.exit(1)
    else:
        print("MADX has completed properly!")

    #Check the existence of madx output
    check_madxout('fc.3', 'fort.3.mad')
    if os.path.isfile('fc.3.aper'):
        with open('fort.3.mad', 'w') as fc3:
            fc3aper = fileinput.input('fc.3.aper')
            for line in fc3aper:
                fc3.write(line)
            fc3.close()
            fc3aper.close()
    check_madxout('fc.3.aux', 'fort.3.aux')
    check_madxout('fc.2', 'fort.2')
    check_madxout('fc.8', 'fort.8')
    check_madxout('fc.16', 'fort.16')
    check_madxout('fc.34', 'fort.34')

    #All the outputs are generated successfully,
    #and download the requested files.
    download_output('fort.3.mad', dest_path)
    download_output('fort.3.aux', dest_path)
    download_output('fort.2', dest_path)
    download_output('fort.8', dest_path)
    download_output('fort.16', dest_path)
    download_output('fort.34', dest_path)
    print("All requested files have zipped and downloaded to %s"%dest_path)

def sixtrackjobs(config, fort3_config):
    '''Manage all the one turn sixtrack job'''
    sixtrack_exe = config['sixtrack_exe']
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
    download_output('sixdesktunes', dest_path, False)
    download_output('mychrom', dest_path, False)
    download_output('betavalues', dest_path, False)

def sixtrackjob(config, config_re, jobname, **args):
    '''One turn sixtrack job'''
    sixtrack_config = config
    fort3_config = copy.deepcopy(config_re)
    source_path = sixtrack_config["source_path"]
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    input_files = sixtrack_config["input_files"]
    extra_inputs = input_files.split(',')
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
    for s in extra_inputs:
        source = os.path.join(source_path, s)
        dest = s+".t1"
        replace(patterns, values, source, dest)
        output.append(dest)
    if os.path.isfile('../fort.3.mad'):
        output.insert(1, '../fort.3.mad')
    else:
        print("The fort.3.mad file doesn't exist!")
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

def check_madxout(filename, newname):
    '''Check the existence of the given file and rename it'''
    if os.path.isfile(filename):
        os.rename(filename, newname)
    else:
        print("The file %s isn't generated successfully!" %filename)
        exit(1)

def download_output(filename, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    if os.path.isfile(filename):
        if not os.path.isdir(dest):
            os.mkdir(dest, 0o755)
        if zp:
            out_name = dest + filename + '.gz'
            with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy(filename, dest)
    else:
        print("The file %s doesn't exist, download failed!"%filename)

def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with repleacements.
    '''
    fin = open(source, 'r')
    fout = open(dest, 'w')
    num = len(patterns)
    for line in fin:
        for i in range(num):
            line = re.sub(patterns[i], str(replacements[i]), line)
        fout.write(line)
    fin.close()
    fout.close()

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
    run(sys.argv)
