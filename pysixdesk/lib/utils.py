import os
import io
import re
import sys
import gzip
import shutil
import logging
import difflib

from collections.abc import Iterable
from itertools import product

# Gobal variables
PYSIXDESK_ABSPATH = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


def check(files):
    '''Check the existence of the files and rename them if the files is a dict
    which looks like {'file1_oldName': 'file1_newName',
    'file2_oldName': 'file2_newName'}
    '''
    if not isinstance(files, (dict, list)):
        raise TypeError('"files" must be a list or dict.')

    status = False
    if isinstance(files, dict):
        for key, value in files.items():
            if os.path.isfile(key):
                if key != value:
                    os.rename(key, value)
            else:
                print("The file %s isn't generated successfully!" % key)
                return status
    elif isinstance(files, list):
        for key in files:
            if not os.path.isfile(key):
                print("The file %s isn't generated successfully!" % key)
                return status
    status = True
    return status


def download_output(filenames, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    if not os.path.isdir(dest):
        os.makedirs(dest, 0o755)

    for filename in filenames:
        if not os.path.isfile(filename):
            content = "The file %s doesn't exist, download failed!" % filename
            raise FileNotFoundError(content)
        if os.path.isfile(filename):
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
                with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy(filename, dest)


def check_fort3_block(fort3, block):
    '''Check the existence of the given block in fort.3'''

    with open(fort3, 'r') as f_in:
        lines = f_in.readlines()
    for line in lines:
        if line.lower().startswith(block.lower()):
            return True
    return False


def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with replacements.
    '''
    if not os.path.isfile(source):
        raise FileNotFoundError("The file %s doesn't exist!" % source)

    fin = open(source, 'r')
    fout = open(dest, 'w')
    num = len(patterns)
    for line in fin:
        for i in range(num):
            line = re.sub(patterns[i], str(replacements[i]), line)
        fout.write(line)
    fin.close()
    fout.close()


def diff(file1, file2, logger=None, **kwargs):
    '''
    Displays the diff of two files.

    Args:
        file1 (str/path): path to first file for the diff.
        file2 (str/path): path to second file for the diff.
        logger (logging.logger, optional): logger with which to display the
        diff, if None, will use print.
        **kwargs: additional arguments for `difflib.unified_diff`.
    '''

    if logger is not None and isinstance(logger, logging.Logger):
        display = logger.info
    else:
        display = print

    def get_lines(file):
        '''Returns the contents of 'file'.'''
        with open(file) as f:
            f_lines = f.read().split('\n')
        return f_lines

    file1_data = get_lines(file1)
    file2_data = get_lines(file2)

    diff_lines = difflib.unified_diff(file1_data, file2_data, **kwargs)

    if diff_lines:
        display(f'▼▼▼▼▼▼▼▼▼▼▼▼▼ {file1} --> {file2} diff ▼▼▼▼▼▼▼▼▼▼▼▼▼')
        for line in diff_lines:
            display(line)
        display(f'▲▲▲▲▲▲▲▲▲▲▲▲▲ {file1} --> {file2} diff ▲▲▲▲▲▲▲▲▲▲▲▲▲')


def encode_strings(inputs):
    '''Convert list or directory to special-format string'''
    if not isinstance(inputs, (list, dict)):
        raise TypeError('"inputs" must be list or dict.')

    if isinstance(inputs, list):
        output = ','.join(map(str, inputs))
    elif isinstance(inputs, dict):
        a = [':'.join(map(str, i)) for i in inputs.items()]
        output = ','.join(map(str, a))
    return output


def decode_strings(inputs):
    '''Convert special-format string to list or directory'''
    if not isinstance(inputs, str):
        raise TypeError('"inputs" must be a string.')

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
        raise ValueError("Invalid data source!")
    return zbuf.getvalue()


def decompress_buf(buf, out, des='file'):
    '''Data decompression to retrieve from database'''
    if not isinstance(buf, bytes):
        raise TypeError('"buf" must be bytes.')
    if des not in ['file', 'buf']:
        raise ValueError('"des" must be "file" or "buf".')

    zbuf = io.BytesIO(buf)
    if des == 'file':
        with gzip.GzipFile(fileobj=zbuf) as f_in:
            with open(out, 'wb') as f_out:
                f_out.write(f_in.read())
    elif des == 'buf':
        with gzip.GzipFile(fileobj=zbuf) as f_in:
            out = f_in.read()
            out = out.decode()
    return out


def concatenate_files(source, dest, ignore='ENDE'):
    '''Concatenate the given files'''
    f_out = open(dest, 'w')
    endline = ignore + '\n'
    if not isinstance(source, list):
        source = [source]
    for s_in in source:
        with open(s_in, 'r') as f_in:
            lines = f_in.readlines()
            valid_lines = []
            for line in lines:
                if line.lower().startswith(ignore.lower()):
                    endline = line
                    break
                valid_lines.append(line)
            f_out.writelines(valid_lines)
    f_out.writelines(endline)
    f_out.close()


def exc_catch(fun, exc_action=None, *args, **kwargs):
    '''
    Wrapper which catches errors of provided function "fun" and runs "action"
    if provided.

    Args:
        fun (callable): The wrapped function.
        exc_action (callable, optionnal): callable which will run if "fun"
        raises an Exception. If None will not do anything, the Exception will
        be supressed.
        *args **kwargs (optionnal): passed on to the wrapped function call.

    Returns:
        The output of the wrapped function if no exceptions are raised, the
        output of the "action" callable if provided and an Exception is raised.
    '''
    try:
        return fun(*args, **kwargs)
    except Exception:
        if callable(exc_action):
            return exc_action()


def condor_logger(name):
    '''
    Prepares a logger for job on HTCondor. It splits the levels to stdout
    and stderr, and disables module level logging.

    DEBUG, INFO go to stdout
    WARNING, ERROR go to stderr
    '''

    # disable module level logging of pysixdesk
    # logger = logging.getLogger('pysixdesk')
    # logger.setLevel(logging.CRITICAL)

    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                                  datefmt='%H:%M:%S')
    # enable local logging with stdout and stderr split
    logger = logging.getLogger(name)
    h1 = logging.StreamHandler(sys.stdout)
    h1.setFormatter(formatter)
    h1.setLevel(logging.DEBUG)
    h1.addFilter(lambda record: record.levelno <= logging.WARNING)

    h2 = logging.StreamHandler(sys.stderr)
    h2.setFormatter(formatter)
    h2.setLevel(logging.ERROR)

    logger.addHandler(h1)
    logger.addHandler(h2)
    logger.setLevel(logging.DEBUG)
    return logger


def merge_dicts(x, y):
    """Merges two dicts.

    Returns:
        dict: Merged dict
    """
    z = x.copy()
    z.update(y)
    return z


def product_dict(**kwargs):
    '''
    Cartesian product of dict of lists.
    From:
    https://stackoverflow.com/questions/5228158/cartesian-product-of-a-dictionary-of-lists

    Args:
        inp (dict): dict of lists on which to do the product.

    Returns:
        list : list of dicts.

    Example:
        list(self._product_dict(**{"number": [1,2,3],
                                   "color": ["orange","blue"]}))
        >>[{"number": 1, "color": "orange"},
           {"number": 1, "color": "blue"},
           {"number": 2, "color": "orange"},
           {"number": 2, "color": "blue"},
           {"number": 3, "color": "orange"},
           {"number": 3, "color": "blue"}]
    '''
    keys = kwargs.keys()
    vals = []
    for v in kwargs.values():
        if not isinstance(v, Iterable) or isinstance(v, str):
            v = [v]
        vals.append(v)
    for instance in product(*vals):
        yield dict(zip(keys, instance))
