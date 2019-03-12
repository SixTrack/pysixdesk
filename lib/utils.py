import os
import re
import sys
import gzip
import shutil


def check(filename, newname):
    '''Check the existence of the given file and rename it'''
    if os.path.isfile(filename):
        os.rename(filename, newname)
    else:
        print("The file %s isn't generated successfully!" %filename)
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

