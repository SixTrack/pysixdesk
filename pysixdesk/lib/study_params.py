import re
import os
import logging
from collections import OrderedDict

from . import machineparams
from .constants import PROTON_MASS
from .utils import PYSIXDESK_ABSPATH, merge_dicts


class StudyParams:
    '''
    Looks for any placeholders in the provided paths and extracts the
    placeholder if no default values found, use None.
    This implements __setitem__ and __getitem__ so the user can interact with
    the StudyParams object similarly to a dict.

    To get the placeholder patterns for the mask file use `self.madx`.
    To get the placeholder patterns for the oneturn sixtrack job use
    `self.oneturn`.
    To get the placeholder patterns for the sixtrack file use
    `self.sixtrack`.
    '''

    def __init__(self, mask_path,
                 fort_path=f'{PYSIXDESK_ABSPATH}/templates/fort.3',
                 machine_defaults=machineparams.HLLHC['col']):
        """
        Args:
            mask_path (str): path to the mask file
            fort_path (str): path to the fort file
        """
        self._logger = logging.getLogger(__name__)
        # comment regexp
        self._reg_comment = re.compile(r'^(\s?!|\s?/).*', re.MULTILINE)
        # placeholder pattern regexp
        self._reg = re.compile(r'%(?!FILE|%)([a-zA-Z0-9_]+/?)')
        self.fort_path = fort_path
        self.mask_path = mask_path
        # initialize empty calculation queue
        self.calc_queue = []
        # default parameters for the sixtrack specific placeholders
        self.f3_defaults = dict([
                                ("ax0s", 0.1),
                                ("ax1s", 0.1),
                                ("chrom_eps", 0.000001),
                                ("dp1", 0.000001),
                                ("dp2", 0.000001),
                                ("turnss", 1e5),
                                ("ibtype", 0),
                                ("iclo6", 2),
                                ("idfor", 0),
                                ("imc", 1),
                                ("ition", 0),
                                ("length", 26658.864),
                                ("ndafi", 1),
                                ("nss", 60),  # I think this should be 60?
                                ("pmass", PROTON_MASS),
                                ("Runnam", 'FirstTurn'),
                                ("ratios", 1),
                                ("toggle_post/", '/'),
                                ("toggle_diff/", '/'),
                                ("writebins", 1),
                                ])
        self.machine_defaults = machine_defaults
        self.defaults = merge_dicts(self.f3_defaults, self.machine_defaults)
        # phasespace params
        # TODO: find sensible defaults
        amp = [8, 10, 12]  # The amplitude
        self.phasespace = dict([
                               ('amp', list(zip(amp, amp[1:]))),
                               ('kang', list(range(1, 1 + 1))),
                               ('kmax', 5),
                               ])

        self.madx = self.find_patterns(self.mask_path)
        self.sixtrack = self.find_patterns(self.fort_path)

    @property
    def oneturn(self):
        sixtrack = self.sixtrack.copy()
        sixtrack['turnss'] = 1
        sixtrack['nss'] = 1
        sixtrack['Runnam'] = 'FirstTurn'
        return sixtrack

    def keys(self):
        """Gets the keys of `self.madx`, `self.sixtrack` and `self.phasespace`

        Returns:
            list: list of keys.
        """
        return (list(self.madx.keys()) +
                list(self.sixtrack.keys()) +
                list(self.phasespace.keys()))

    def _extract_patterns(self, file):
        '''
        Extracts the patterns from a file.

        Args:
            file (str): path to the file from which to extract the placeholder
            patterns.
        Returns:
            list: list containing the matches
        '''
        with open(file) as f:
            lines = f.read()
        lines_no_comments = re.sub(self._reg_comment, '', lines)
        matches = re.findall(self._reg, lines_no_comments)
        return matches

    def find_patterns(self, file_path, folder=False, keep_none=True):
        '''
        Reads file at `file_path` and populates a dict with the matched
        patterns and values taken from `self.defaults`.

        Args:
            file_path (str): path to file to extract placeholder patterns
            folder (bool, optional): if True, check for placeholder patterns
            in all files in the `file_path` fodler.
            keep_none (bool, optional): if True, keeps the None entries in the
            output dict.

        Returns:
            OrderedDict: dictionnary of the extracted placeholder patterns with
            their values set the entry on `self.defaults`.
        '''
        dirname = os.path.dirname(file_path)
        if folder and dirname != '':
            # extract the patterns for all the files in the directory of the
            # maskfile
            matches = []
            for file in os.listdir(dirname):
                matches += self._extract_patterns(os.path.join(dirname,
                                                               file))
        else:
            matches = self._extract_patterns(file_path)

        out = OrderedDict()
        for ph in matches:
            if ph in self.defaults.keys():
                out[ph] = self.defaults[ph]
            elif keep_none:
                out[ph] = None

        self._logger.debug(f'Found {len(matches)} placeholders.')
        self._logger.debug(f'With {len(set(matches))} unique placeholders.')
        for k, v in out.items():
            self._logger.debug(f'{k}: {v}')
        return out

    def add_calc(self, in_keys, out_keys, fun):
        '''
        Add calculations to the calc queue. Any extra arguments of
        fun can be given in the *args/**kwargs of the self.calc call.

        Args:
            in_keys (list): keys to the input data of `fun`.
            out_keys (list): keys to place the output `fun` in
            `self.sixtrack`. The `len(out_keys)` must match the number of
            outputs of `fun`.
            fun (function): function to run, must take as input the values
            given by the `in_keys` and outputs to the `out_keys` in
            `self.sixtrack`. Can also have **kwargs which will be
            passed to it when calling `self.calc`
        '''
        if not isinstance(in_keys, list):
            in_keys = [in_keys]
        if not isinstance(out_keys, list):
            out_keys = [out_keys]
        self.calc_queue.append([in_keys, out_keys, fun])

    def calc(self, **kwargs):
        '''
        Runs the queued calculations, in order. *args and **kwargs are passed
        to the queued function at run time. The output of the queue is put
        in `self.sixtrack`.

        Args:
            *args: passed to the `fun` in the queued calculations
            **kwargs: passed to the `fun` in the queued calculations

        Returns:
            OrderedDict: `self.sixtrack` after running the calculation
            queue
        '''
        for in_keys, out_keys, fun in self.calc_queue:
            # get the input values with __getitem__
            inp = [self.__getitem__(k) for k in in_keys]

            out = fun(*inp, **kwargs)
            if not isinstance(out, list):
                out = [out]
            if len(out) != len(out_keys):
                content = (f'The number of outputs of {fun} does not match the'
                           f' number of keys in {out_keys}.')
                raise ValueError(content)
            for i, k in enumerate(out_keys):
                self.sixtrack[k] = out[i]
        # reset calculation queue
        self.calc_queue = []
        return self.sixtrack

    def __repr__(self):
        '''
        Unified __repr__ of the three dictionnaries.
        '''
        return '\n\n'.join(['Madx params: ' + self.madx.__repr__(),
                            'SixTrack params: ' + self.sixtrack.__repr__(),
                            'Phase space params: ' + self.phasespace.__repr__()])

    # set and get items like a dict
    def __setitem__(self, key, val):
        '''
        Adds entry to the appropriate dictionnary(ies) which already contains
        the key.
        '''
        if key not in self.keys():
            raise KeyError(f'"{key}" not in extracted placeholders.')
        if key in self.phasespace.keys():
            self.phasespace[key] = val
        if key in self.madx.keys():
            self.madx[key] = val
        if key in self.sixtrack.keys():
            self.sixtrack[key] = val

    def __getitem__(self, key):
        '''
        Gets entry from the first dictionnary which contains the key.
        '''
        if key not in self.keys():
            raise KeyError(key)
        if key in self.phasespace.keys():
            return self.phasespace[key]
        if key in self.madx.keys():
            return self.madx[key]
        if key in self.sixtrack.keys():
            return self.sixtrack[key]

    # def update(self, *args, **kwargs):
    #     '''
    #     Updates both dictionnaries.
    #     '''
    #     self.madx.update(*args, **kwargs)
    #     self.sixtrack.update(*args, **kwargs)

    @staticmethod
    def _find_none(dic):
        """Finds the keys of any entry in `dic` with a None value.

        Args:
            dic (dict): Dictionnary to check.

        Returns:
            list: list of keys whose value are None.
        """
        out = []
        for k, v in dic.items():
            if v is None:
                out.append(k)
        return out

    def _remove_none(self, dic):
        """Removes Nones in dictionnary `dic`."""
        for k in self._find_none(dic):
            del dic[k]

    def drop_none(self):
        """
        Drop Nones from `self.madx`, `self.sixtrack` and `self.phasespace`.
        """
        self._remove_none(self.madx)
        self._remove_none(self.sixtrack)
        self._remove_none(self.phasespace)


# for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test = StudyParams('../templates/lhc_aperture/hl13B1_elens_aper.mask')
    print(test.params)
