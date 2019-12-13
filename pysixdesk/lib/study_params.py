import re
import logging

from pathlib import Path
from collections import OrderedDict
from itertools import product
from functools import partial
from collections.abc import Iterable

from . import machineparams
from .constants import PROTON_MASS
from .utils import PYSIXDESK_ABSPATH, merge_dicts


class StudyParams:
    '''
    Looks for any placeholders in the provided paths and extracts the
    placeholder assigns values from the machine_defaults parameters and the
    f3_defaults attribute. If no default values found, use None.
    This class implements __setitem__ and __getitem__ so the user can interact
    with the StudyParams object similarly to a dict.

    To get the placeholder patterns for the mask file use self.madx.
    To get the placeholder patterns for the oneturn sixtrack job use
    self.oneturn.
    To get the placeholder patterns for the fort.3 file use self.sixtrack.
    '''

    def __init__(self,
                 mask_path=Path(PYSIXDESK_ABSPATH) / 'templates' / 'hl10.mask',
                 fort_path=Path(PYSIXDESK_ABSPATH) / 'templates' / 'fort.3',
                 machine_defaults=machineparams.HLLHC['col']):
        """
        Args:
            mask_path (str, path): path to the mask file.
            fort_path (str, path): path to the fort file.
            machine_defaults (dict): dictionary containing the default
                parameters of the desired machine/configuration.
        """
        if not isinstance(mask_path, Path):
            mask_path = Path(mask_path)
        if not isinstance(fort_path, Path):
            fort_path = Path(fort_path)

        self._logger = logging.getLogger(__name__)
        # comment regexp
        self._reg_comment = re.compile(r'^(\s?!|\s?/).*', re.MULTILINE)
        # placeholder pattern regexp
        self._reg = re.compile(r'%([a-zA-Z0-9_]+/?)')
        self.fort_path = fort_path
        self.mask_path = mask_path
        # initialize empty calculation queue
        self.calc_queue = []
        # TODO: Figure out how to nicely handle the 'chrom_eps' and 'CHROM'
        # parameters, they are not substituting any placeholders, they are only
        # used in the preprocessing job to do some calculations for the
        # oneturnresult file. They shouldn't really be included in the
        # f3_defaults dict, as they are not values for placeholders in fort.3.

        # default parameters for sixtrack/fort.3 specific placeholders
        self.f3_defaults = {"ax0s": 0.1,
                            "ax1s": 0.1,
                            "chrom_eps": 0.000001,  # this is not a placeholder is it the same as dp1/2 ?
                            "CHROM": 0,  # this is not a placeholder, it's a toggle for the preprocess job
                            "dp1": 0.000001,
                            "dp2": 0.000001,
                            "EI": 3.5,  # eigen emittance
                            "ibtype": 0,
                            "iclo6": 2,
                            "idfor": 0,
                            "imc": 1,
                            "ilin": 1,
                            "ition": 1,
                            "length": 26658.864,
                            "ndafi": 1,
                            "nss": 30,  # should this be 60? 30?
                            "pmass": PROTON_MASS,
                            "Runnam": 'FirstTurn',
                            "ratio": 1,
                            "turnss": 1e6,
                            # these toggle_*: aren't very pretty.
                            "toggle_post/": '',  # '' --> on, '/' --> off
                            "toggle_diff/": '/',  # '' --> on, '/' --> off
                            "toggle_coll/": '/',  # '' --> on, '/' --> off
                            "writebins": 1,
                            }
        self.machine_defaults = machine_defaults
        self.defaults = merge_dicts(self.f3_defaults, self.machine_defaults)
        # phasespace params
        # TODO: find sensible defaults for the phasespace parameters.
        amp = [8, 10, 12]  # The amplitude
        self.phasespace = {"amp": list(zip(amp, amp[1:])),
                           "kang": list(range(1, 1 + 1)),
                           "kmax": 5,
                           }

        self.madx = self.find_patterns(self.mask_path)
        self.sixtrack = self.find_patterns(self.fort_path,
                                           mandatory=['chrom_eps', 'CHROM'])

    @property
    def _sixtrack_only(self):
        '''Parameters which are exclusively found in the fort.3
        '''
        six_only = list(set(self.sixtrack.keys()) - set(self.madx.keys()))
        return {k: self.sixtrack[k] for k in six_only}

    @property
    def oneturn(self):
        sixtrack = self.sixtrack.copy()
        sixtrack['turnss'] = 1
        sixtrack['nss'] = 1
        sixtrack['Runnam'] = 'FirstTurn'
        # oneturn job must output fort.10 --> POST block
        sixtrack['toggle_post/'] = ''
        # oneturn job cannot do collimation
        sixtrack['toggle_coll/'] = '/'
        return sixtrack

    def keys(self):
        """Gets the keys of 'self.madx', 'self.sixtrack' and 'self.phasespace'.

        Returns:
            list: list of keys.
        """
        return (list(self.madx.keys()) +
                list(self.sixtrack.keys()) +
                list(self.phasespace.keys()))

    def _extract_patterns(self, file):
        '''Extracts the patterns from a file.

        Args:
            file (str, path): path to the file from which to extract the
                placeholder patterns.
        Returns:
            list: list containing the regexp matches, i.e. the placeholders
        '''
        with open(file) as f:
            lines = f.read()
        lines_no_comments = re.sub(self._reg_comment, '', lines)
        matches = re.findall(self._reg, lines_no_comments)
        return matches

    def find_patterns(self, file_path, keep_none=True, mandatory=None):
        '''Reads file at 'file_path' and populates a dict with the matched
        patterns and values taken from 'self.defaults'.

        Args:
            file_path (str, path): path to file frim which to extract
                placeholder patterns.
            keep_none (bool, optional): if True, keeps the None entries in the
                output dict.
            mandatory (list, optional): if provided will add the keys in the
                provided list to the output dict, regardless if they are found
                in the file.

        Returns:
            OrderedDict: dictionary of the extracted placeholder patterns with
                their values given by 'self.defaults'.
        '''
        matches = self._extract_patterns(file_path)

        out = OrderedDict()
        for ph in matches:
            if ph in self.defaults.keys():
                out[ph] = self.defaults[ph]
            elif keep_none:
                out[ph] = None
        if mandatory is not None:
            for k in mandatory:
                out[k] = self.defaults[k]

        self._logger.debug(f'Found {len(matches)} placeholders in '
                           f'{file_path}.')
        self._logger.debug(f'With {len(set(matches))} unique placeholders.')
        for k, v in out.items():
            self._logger.debug(f'{k}: {v}')
        return out

    @staticmethod
    def _combinations_prep(**kwargs):
        '''Sanitizes the paramter values.

        Args:
            **kwargs: Parameter name = parameter value.

        Returns:
            dict: Dictionary of the parameter values with the values changed to
                lists, if not already.
        '''
        for k, v in kwargs.items():
            if not isinstance(v, Iterable) or isinstance(v, str):
                kwargs[k] = [v]
        return kwargs

    @staticmethod
    def combination_logic(param_dict):
        """This method defines the combination logic of the parameters, by
        default, it will do the cartesian product of the values, using
        itertools.product. This method can be overwritten, to define exotic
        parameter scanning behaviour.

        Args:
            param_dict: dictionary containing lists of the parameters.

        Returns:
            iterable: iterable on the cartesian product of the input parameter
                values.
        """
        # TODO: decide if the overwriting of this method should be done by
        # doing someting like:
        # self.params.combinations = other_method
        # in config.py
        # Or maybe it should be an argument in the init of this class and
        # default to this ?
        return product(*param_dict.values())

    def combinations(self):
        '''Performs the combinations of the user provided parameters.

        Yields:
            tuple: a tuple containing 2 dictionaries, the first with the madx
                parameters, the other with the sixtrack parameters.
        '''
        param_dict = self._combinations_prep(**self.madx,
                                             **self._sixtrack_only,
                                             **self.phasespace)
        for e in (dict(zip(param_dict.keys(), e))
                  for e in self.combination_logic(param_dict)):
            yield ({k: e[k] for k in self.madx.keys()},
                   {k: e[k] for k in (list(self.sixtrack.keys()) +
                                      list(self.phasespace.keys()))})

    def calc(self, params, task_id=None, get_val_db=None, require=None):
        """Runs the queued calculations, in order. A dictionary containing the
        calculation results is returned.

        Args:
            params (dict): One element of the combination of the parameter
                dict.
            task_id (int): task_id of the require parameters, when fetching the
                data from the database.
            get_val_db (SixDB, optional): SixDB object to fecth values from db
                in for the calculations.
            require (list, str, optional): If 'all' will run all function in
                calculation queue.
                If None or 'none', will run all calculations which don't
                require any database.
                If list of table names, will run calculations whose 'require'
                attribute's keys are a subset of the provided list.

        Returns:
            dict: Dictionary containing the output of the calculations.
        """
        params = params.copy()
        if require == 'all':
            # all the functions
            queue = self.calc_queue
        elif require in [None, 'none']:
            queue = [f for f in self.calc_queue if not hasattr(f, 'require')]
        else:
            queue = self._filter_queue(require)

        out_dict = {}
        for fun in queue:
            # filter inputs from params
            inputs = [params[k] for k in getattr(fun, 'input_keys', [])]
            # get additionnal kwargs from db
            required = self._get_required_values(get_val_db, fun, task_id)
            # run functions
            output = fun(*inputs, **required)
            if not isinstance(output, tuple):
                output = tuple([output])
            if len(output) != len(fun.output_keys):
                content = (f'The number of outputs of {fun.__name__} does not'
                           ' match the number of of keys in'
                           f' {fun.output_keys}.')
                raise ValueError(content)

            # construct output dict
            o_dict = {}
            for k, v in zip(fun.output_keys, output):
                self._logger.debug(f'Inserting "{k}": {v}')
                o_dict[k] = v
            # make results available for next functions in calc queue
            params.update(o_dict)
            # update output dict of results
            out_dict.update(o_dict)

        return out_dict

    def _filter_queue(self, require):
        '''Filters the calculation queue based on the 'require' attribute.

        Args:
            require (list): list of keys which must be contained in the
                'require' attribute dictionary for the function to be included.

        Returns:
            list: subset of the calculation queue.
        '''
        queue = []
        for f in self.calc_queue:
            # if the required tables are a subset of the require list
            req_table = getattr(f, 'require', None)
            if req_table is not None:
                if set(req_table.keys()).issubset(set(require)):
                    queue.append(f)
        return queue

    def _get_required_values(self, db, fun, task_id):
        '''Gets the values needed in the dict fun.require from the database.

        Args:
            db (SicDB): Database from which to extract the values.
            fun (callable): calculation queue function.
            task_id (int): task_id of the row from which to fetch the parameter
                values.

        Returns:
            dict: dictionary containing the values of the required parameters.
        '''
        required = {}
        if not hasattr(fun, 'require'):
            return required
        for r_table, r_list in fun.require.items():
            if not isinstance(r_list, list):
                r_list = [r_list]
            r_values = db.select(r_table, r_list, where=f'task_id={task_id}')
            # convert columns to list of list
            r_values = zip(*r_values)
            required.update({k: v[0] for k, v in zip(r_list, r_values)})
        return required

    def __repr__(self):
        '''Unified __repr__ of the three dictionaries.
        '''
        return '\n\n'.join(['Madx params: ' + self.madx.__repr__(),
                            'SixTrack params: ' + self.sixtrack.__repr__(),
                            'Phase space params: ' + self.phasespace.__repr__()])

    # set and get items like a dict
    def __setitem__(self, key, val):
        '''Adds entry to the appropriate dictionary(ies) which already contains
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
        '''Gets entry from the first dictionary which contains the key.
        '''
        if key not in self.keys():
            raise KeyError(key)
        if key in self.phasespace.keys():
            return self.phasespace[key]
        if key in self.madx.keys():
            return self.madx[key]
        if key in self.sixtrack.keys():
            return self.sixtrack[key]

    def _remove_none(self, dic):
        """Removes Nones in dictionary 'dic'.
        Note, it modifies the dictionary inplace.

        Args:
            dic (dict): Dictionary to check.
        """
        for key in [k for k, v in dic.items() if v is None]:
            del dic[key]

    def drop_none(self):
        """Drop Nones from 'self.madx', 'self.sixtrack' and 'self.phasespace'.
        """
        self._remove_none(self.madx)
        self._remove_none(self.sixtrack)
        self._remove_none(self.phasespace)


def _set_property(key, value):
    '''Simple decorator to add attributes to functions.
    '''
    def decorated_func(func):
        setattr(func, key, value)
        return func
    return decorated_func


# predefined properties
set_input_keys = partial(_set_property, 'input_keys')
set_output_keys = partial(_set_property, 'output_keys')
set_requirements = partial(_set_property, 'require')
