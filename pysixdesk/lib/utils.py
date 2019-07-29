import os
import io
import re
import sys
import gzip
import shutil
import logging

# Gobal variables
PYSIXDESK_ABSPATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def check(files):
    '''Check the existence of the files and rename them if the files is a dict
    which looks like {'file1_oldName': 'file1_newName',
    'file2_oldName': 'file2_newName'}
    '''
    if isinstance(files, dict):
        for key, value in files.items():
            if os.path.isfile(key):
                if key != value:
                    os.rename(key, value)
            else:
                raise FileNotFoundError("The file %s hasn't generated successfully!" % key)
    elif isinstance(files, list):
        for key in files:
            if not os.path.isfile(key):
                raise FileNotFoundError("The file %s hasn't generated successfully!" % key)
    else:
        raise TypeError("The input must be a list or dict!")


def download_output(filenames, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    if not os.path.isdir(dest):
        os.makedirs(dest, 0o755)

    for filename in filenames:
        if not os.path.isfile(filename):
            raise FileNotFoundError("The file %s doesn't exist, download failed!" % filename)
        if os.path.isfile(filename):
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
                with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy(filename, dest)


def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with replacements.
    '''
    if not os.path.isfile(source):
        raise FileNotFoundError("The file %s does not exist!" % source)

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


def encode_strings(inputs):
    '''Convert list or directory to special-format string'''
    if isinstance(inputs, list):
        output = ','.join(map(str, inputs))
    elif isinstance(inputs, dict):
        a = [':'.join(map(str, i)) for i in inputs.items()]
        output = ','.join(map(str, a))
    else:
        raise TypeError(f'{inputs} is not a list or dict.')
    return output


def decode_strings(inputs):
    '''Convert special-format string to list or directory'''
    if not isinstance(inputs, str):
        raise TypeError(f'{inputs} is not a string.')

    if ':' in inputs:
        output = {}
        a = inputs.split(',')
        for i in a:
            b = i.split(':')
            output[b[0]] = b[1]
    else:
        output = inputs.split(',')
    return output


def compress_buf(data, source='file'):
    '''Data compression for storing in database
    The data source can be file,gzip,str'''
    zbuf = io.BytesIO()
    if source == 'file' and os.path.isfile(data):
        with gzip.GzipFile(mode='wb', fileobj=zbuf) as zfile:
            with open(data, 'rb') as f_in:
                buf = f_in.read()
                zfile.write(buf)
    elif source == 'gzip' and os.path.isfile(data):
        with open(data, 'rb') as f_in:
            shutil.copyfileobj(f_in, zbuf)
    elif source == 'str' and isinstance(data, str):
        buf = data.encode()
        with gzip.GzipFile(mode='wb', fileobj=zbuf) as zfile:
            zfile.write(buf)
    else:
        raise TypeError("Invalid data source!")
    return zbuf.getvalue()


def decompress_buf(buf, out, des='file'):
    '''Data decompression to retrieve from database'''

    if not isinstance(buf, bytes):
        raise TypeError(f"Invalid input data '{buf}'!")

    zbuf = io.BytesIO(buf)
    if des == 'file':
        with gzip.GzipFile(fileobj=zbuf) as f_in:
            with open(out, 'wb') as f_out:
                f_out.write(f_in.read())
    elif des == 'buf':
        with gzip.GzipFile(fileobj=zbuf) as f_in:
            out = f_in.read()
            out = out.decode()
    else:
        raise ValueError(f"Unknown output type '{des}'!")
    return out


def condor_logger():
    '''
    Prepares a logger for job on HTCondor. It splits the levels to stdout
    and stderr, and disables module level logging.

    DEBUG, INFO go to stdout
    WARNING, ERROR go to stderr
    '''

    # disable module level logging of pysixdesk
    logger = logging.getLogger('pysixdesk')
    logger.setLevel(logging.CRITICAL)

    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                                  datefmt='%H:%M:%S')
    # enable local logging with stdout and stderr split
    logger = logging.getLogger('preprocess_job')
    h1 = logging.StreamHandler(sys.stdout)
    h1.setFormatter(formatter)
    h1.setLevel(logging.DEBUG)
    h1.addFilter(lambda record: record.levelno <= logging.INFO)

    h2 = logging.StreamHandler(sys.stderr)
    h2.setFormatter(formatter)
    h2.setLevel(logging.WARNING)

    logger.addHandler(h1)
    logger.addHandler(h2)
    logger.setLevel(logging.DEBUG)
    return logger
