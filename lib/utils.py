import os
import re
import sys
import gzip
import shutil


def check(file_dict):
    '''Check the existence of the given file and rename it'''
    for key,value in file_dict.items():
        if os.path.isfile(key):
            os.rename(key, value)
        else:
            print("The file %s isn't generated successfully!" %key)
            sys.exit(1)

def download_output(filenames, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    for filename in filenames:
        if os.path.isfile(filename):
            if not os.path.isdir(dest):
                os.mkdir(dest, 0o755)
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
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

def code(inputs):
    '''Convert list or directory to special-format string'''
    output = ''
    if isinstance(inputs, list):
        for i in inputs:
            output = output + str(i) + ','
        return output[:-1]
    elif isinstance(inputs, dict):
        for i,j in inputs.items():
            output = output + str(i) + ':' + str(j) + ','
        return output[:-1]
    else:
        return str(inputs)

def decode(inputs):
    '''Convert special-format string to list or directory'''
    if isinstance(inputs, str):
        if ':' in inputs:
            output = {}
            a = inputs.split(',')
            for i in a:
                b = i.split(':')
                output[b[0]] = b[1]
        else:
            output = inputs.split(',')
        return output
    else:
        print("The input is not string!")
        return []
