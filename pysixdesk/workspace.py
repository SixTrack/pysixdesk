import os
import sys
import utils
import shutil
import logging

from importlib.machinery import SourceFileLoader

# Global variable
LEVEL = 1


class WorkSpace(object):
    '''
    class for handling workspaces.
    Current structure:
    ./sandbox/
       |_ studies/
       |    |_ test/
       |    |_ study_000/
       |    |_ study_001/
       |    |_ study_002/
       |_ templates/
            |_ htcondor_run.sub
            |_ hl10.mask
            |_ fort.3.mother2
            |_ fort.3.mother1
            |_ config.py
    '''

    def __init__(self, workspace_name='./sandbox', log_file=None):
        self._logger = logging.getLogger(__name__)
        self.name = workspace_name
        self.paths = {}
        self.studies = []
        self.log_file = log_file
        self._update_list_existing_studies()

    def _check_name(self, level=LEVEL):
        '''Check the workspace name'''
        if len(self.name) == 0:
            content = "...undefined workspace! Please create one first"
            self._logger.error(content)
            return False

    def _check_study_name(self, study_name, level=LEVEL):
        '''Check the study name'''
        input_study_name = study_name
        if input_study_name is None:
            if len(self.studies) > 0:
                input_study_name = 'study_%03i' % (len(self.studies))
            else:
                input_study_name = 'test'
        elif not isinstance(input_study_name, str):
            content = 'invalid string for study name.'
            raise ValueError(content)
        return input_study_name

    def _inflate_paths(self, sanity_check=True, level=LEVEL):
        '''Assemble structural full-paths of current workspace'''
        if sanity_check:
            self._check_name()
        content = 'Inflating paths of workspace %s ...' % (self.name)
        self._logger.info(content)
        self.paths['workspace'] = os.path.abspath(self.name)
        self.paths['studies'] = os.path.join(self.paths['workspace'],
                                             'studies')
        self.paths['templates'] = os.path.join(self.paths['workspace'],
                                               'templates')

    def _inflate_study_path(self, study_name, sanity_check=True, level=LEVEL):
        '''Generate the study path'''
        if sanity_check:
            input_study_name = self._check_study_name(study_name)
            self._inflate_paths()
        else:
            input_study_name = study_name
        content = 'Inflating path to study %s ...' % (input_study_name)
        self._logger.info(content)
        return os.path.join(self.paths['studies'], input_study_name)

    def _init_dirs(self, sanity_check=True, level=LEVEL):
        '''Initialise directories of current workspace, including copy of
           template files'''
        if sanity_check:
            self._inflate_paths()
        content = 'Checking directories of workspace %s ...' % (self.name)
        self._logger.info(content)
        for key in self.paths.keys():
            if not os.path.isdir(self.paths[key]):
                os.mkdir(self.paths[key])
                content = '...created %s directory: %s' % (
                    key, self.paths[key])
                self._logger.info(content)
            else:
                content = '...%s directory already exists: %s' % (
                    key, self.paths[key])
                self._logger.info(content)

        content = 'Checking template files in %s...' % (
            self.paths['templates'])
        self._logger.info(content)
        tem_path = os.path.join(utils.PYSIXDESK_ABSPATH, 'templates')
        for item in os.listdir(tem_path):
            sour = os.path.join(tem_path, item)
            dest = os.path.join(self.paths['templates'], item)
            if os.path.isfile(sour) and not os.path.isfile(dest):
                shutil.copy2(sour, dest)
                content = '...copied template file %s from %s .' % (
                    item, utils.PYSIXDESK_ABSPATH)
                self._logger.info(content)
            else:
                content = '...template file %s present.' % (item)
                self._logger.info(content)
        content = '...done.\n'
        self._logger.info(content)

    def _update_list_existing_studies(self, sanity_check=True, level=LEVEL):
        '''Update and report list of studies in the current workspace'''
        if sanity_check:
            self._init_dirs()
        content = 'Loading list of studies in %s...' % (
            self.paths['studies'])
        self._logger.info(content)
        for item in os.listdir(self.paths['studies']):
            if os.path.isdir(os.path.join(self.paths['studies'], item)):
                if (item not in self.studies):
                    self.studies.append(item)
        if len(self.studies) == 0:
            content = '...workspace %s contains no studies at the moment' % (
                self.name)
            self._logger.info(content)
        else:
            if len(self.studies) == 1:
                content = '...workspace %s contains %i study:' % (
                    self.name, len(self.studies))
                self._logger.info(content)
            else:
                content = '...workspace %s contains %i studies:' % (
                    self.name, len(self.studies))
                self._logger.info(content)
            print(self.studies)
        content = '...done.\n'
        self._logger.info(content)

    def show_studies(self):
        '''Show all the studies in the current workspace'''
        self._update_list_existing_studies(False)

    def init_study(self, study_name=None, sanity_check=True, level=LEVEL):
        '''Initialise the directory hosting a study'''

        # sanity checks
        if sanity_check:
            self._update_list_existing_studies()
            input_study_name = self._check_study_name(study_name=study_name)
        else:
            input_study_name = study_name

        content = 'Initialising study %s in workspace %s...' % (
            input_study_name, self.paths['workspace'])
        self._logger.info(content)

        # study directory
        study_path = self._inflate_study_path(input_study_name)
        if not os.path.isdir(study_path):
            os.makedirs(study_path)
            content = '...created directory %s' % (study_path)
            self._logger.info(content)
        else:
            content = '...%s directory already exists' % (study_path)
            self._logger.info(content)

        # template files
        for item in os.listdir(self.paths['templates']):
            sour = os.path.join(self.paths['templates'], item)
            dest = os.path.join(study_path, item)
            if os.path.isfile(sour) and not os.path.isfile(dest):
                shutil.copy2(sour, dest)
                content = '...copied template file %s from %s .' % (
                    item, self.paths['templates'])
                self._logger.info(content)
            else:
                content = '...template file %s present.' % (item)
                self._logger.info(content)

        # update list of existing studies
        self._update_list_existing_studies()

    def load_study(self, study_name, sanity_check=True, level=LEVEL,
                   module_path=None, class_name='MyStudy'):
        '''Load a study'''
        # sanity checks
        if sanity_check:
            self._update_list_existing_studies()
            input_study_name = self._check_study_name(study_name=study_name)
        else:
            input_study_name = study_name
        if study_name not in self.studies:
            content = ("Study %s not present in workspace %s. "
                       "Please create one with the init_study()") % (study_name, self.paths['workspace'])
            raise FileNotFoundError(content)

        # other sanity checks:
        study_path = self._inflate_study_path(input_study_name)
        if module_path is None:
            module_path = os.path.join(study_path, 'config.py')
        if not os.path.isfile(module_path):
            content = "The config file %s isn't found!" % module_path
            raise FileNotFoundError(content)

        content = 'Loading study %s in workspace %s ...' % (
            study_name, self.paths['workspace'])
        self._logger.info(content)
        module_name = os.path.abspath(module_path)
        module_name = module_name.replace('.py', '')
        mod = SourceFileLoader(module_name, module_path).load_module()
        cls = getattr(mod, class_name)
        content = "Study %s loaded from %s \n" % (study_name, study_path)
        self._logger.info(content)
        return cls(study_name, self.paths['studies'])
