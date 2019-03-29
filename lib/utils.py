import os
import io
import re
import sys
import gzip
import shutil

def check(files):
    '''Check the existence of the files and rename them if the files is a dict
    which looks like {'file1_oldName': 'file1_newName',
    'file2_oldName': 'file2_newName'}
    '''
    lStatus = True
    if isinstance(files, dict):
        for key,value in files.items():
            if os.path.isfile(key):
                if key != value:
                    os.rename(key, value)
            else:
                print("The file %s isn't generated successfully!" %key)
                lStatus = False
    elif isinstance(files, list):
        for key in files:
            if not os.path.isfile(key):
                print("The file %s isn't generated successfully!" %key)
                lStatus = False
    else:
        print("The input must be a list or dict!")
        lStatus = False
    return lStatus

def download_output(filenames, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    lStatus = True
    if not os.path.isdir(dest):
        os.makedirs(dest, 0o755)

    for filename in filenames:
        if os.path.isfile(filename):
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
                with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy(filename, dest)
        else:
            print("The file %s doesn't exist, download failed!"%filename)
            lStatus = False
    return lStatus

def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with repleacements.
    '''
    lStatus = True
    if os.path.isfile(source):
        fin = open(source, 'r')
        fout = open(dest, 'w')
        num = len(patterns)
        for line in fin:
            for i in range(num):
                line = re.sub(patterns[i], str(replacements[i]), line)
            fout.write(line)
        fin.close()
        fout.close()
    else:
        print("The file %s doesn't exist!"%source)
        lStatus = False
    return lStatus

def encode_strings(inputs):
    '''Convert list or directory to special-format string'''
    lStatus = True
    if isinstance(inputs, list):
        output = ','.join(map(str, inputs))
    elif isinstance(inputs, dict):
        a = [':'.join(map(str, i)) for i in inputs.items()]
        output = ','.join(map(str, a))
    else:
        lStatus = False
        output = ''
    return lStatus, output

def decode_strings(inputs):
    '''Convert special-format string to list or directory'''
    lStatus = True
    if isinstance(inputs, str):
        if ':' in inputs:
            output = {}
            a = inputs.split(',')
            for i in a:
                b = i.split(':')
                output[b[0]] = b[1]
        else:
            output = inputs.split(',')
    else:
        print("The input is not string!")
        lStatus = False
        output = []
    return lStatus, output

def compress_buf(filename, ty='raw'):
    '''File compression for storing in database
    The file type can be raw, gzip'''
    lStatus = True
    zbuf = io.BytesIO()
    if os.path.isfile(filename):
        if ty is 'raw':
            with gzip.GzipFile(mode='wb', fileobj=zbuf) as zfile:
                with open(filename, 'rb') as f_in:
                    buf = f_in.read()
                    zfile.write(buf)
        elif ty is 'gzip':
            with open(filename, 'rb') as f_in:
                shutil.copyfileobj(f_in, zbuf)
        else:
            lStatus = False
            print("Unsupported file type %s!"%ty)
        return lStatus, zbuf.getvalue()
    else:
        lStatus = False
        print("The file %s doesn't exist!")
        return lStatus, zbuf.getvalue()

def decompress_buf(buf, filename):
    '''File decompression to retireve from database'''
    lStatus = True
    if isinstance(buf, bytes):
        zbuf = io.BytesIO(buf)
        with gzip.GzipFile(fileobj=zbuf) as f_in:
            with open(filename, 'wb') as f_out:
                f_out.write(f_in.read())
        return lStatus
    else:
        lStatus = False
        print("The input buf should be bytes!")
        return lStatus
