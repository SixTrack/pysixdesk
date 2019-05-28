import os
import shutil
from importlib.machinery import SourceFileLoader
import utils

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

    def __init__(self, workspace_name='./sandbox'):
        self.name = workspace_name
        self.paths = {}
        self.studies = []
        self._update_list_existing_studies()

    def _check_name(self):
        if ( len(self.name)==0 ):
            print("...undefined workspace! Please create one first")
            return False
        
    def _check_study_name(self,study_name):
        input_study_name=study_name
        if (input_study_name is None):
            if (len(self.studies)>0):
                input_study_name='study_%03i'%(len(self.studies))
            else:
                input_study_name='test'
        elif (not isinstance(input_study_name, str)):
            print('invalid string for study name.')
            exit(1)
        return input_study_name
        
    def _inflate_paths(self,log=True,sanity_check=True):
        '''assemble structural full-paths of current workspace'''
        if sanity_check: self._check_name()
        if log: print('Inflating paths of workspace %s ...'%(self.name))
        self.paths['workspace']=os.path.abspath(self.name)
        self.paths['studies']=os.path.join(self.paths['workspace'],
                                           'studies')
        self.paths['templates']=os.path.join(self.paths['workspace'],
                                             'templates')
        
    def _inflate_study_path(self,study_name,log=True,sanity_check=True):
        if sanity_check:
            input_study_name=self._check_study_name(study_name)
            self._inflate_paths(log=log)
        else:
            input_study_name=study_name
        if log: print('Inflating path to study %s ...'%(input_study_name))
        return os.path.join(self.paths['studies'], study_name)

    def _init_dirs(self,log=True,sanity_check=True):
        '''Initialise directories of current workspace, including copy of
           template files'''
        if sanity_check: self._inflate_paths(log=log)
        
        if log: print('Checking directories of workspace %s ...'%(self.name))
        for key in self.paths.keys():
            if not os.path.isdir(self.paths[key]):
                os.mkdir(self.paths[key])
                if log: print('...created %s directory: %s'%(
                        key,self.paths[key]))
            else:
                if log: print('...%s directory already exists: %s'%(
                        key,self.paths[key]))

        if log: print ('Checking template files in %s...'%(
                self.paths['templates']))
        tem_path = os.path.join(utils.PYSIXDESK_ABSPATH, 'templates')
        for item in os.listdir(tem_path):
            sour = os.path.join(tem_path, item)
            dest = os.path.join(self.paths['templates'], item)
            if os.path.isfile(sour) and not os.path.isfile(dest):
                shutil.copy2(sour, dest)
                if log: print('...copied template file %s from %s .'%(
                        item,utils.PYSIXDESK_ABSPATH))
            else:
                if log: print('...template file %s present.'%(item))
        if log: print('...done.\n')

    def _update_list_existing_studies(self,log=True,sanity_check=True):
        '''Update and report list of studies in the current workspace'''
        if sanity_check: self._init_dirs(log=log)
        if log: print ('Loading list of studies in %s...'%(
                self.paths['studies']))
        for item in os.listdir(self.paths['studies']):
            if os.path.isdir(os.path.join(self.paths['studies'], item)):
                if (item not in self.studies):
                    self.studies.append(item)
        if (len(self.studies)==0):
            if log: print('...workspace %s contains no studies at the moment'%(
                    self.name))
        else:
            if (len(self.studies)==1):
                if log: print('...workspace %s contains %i study:'%(
                        self.name,len(self.studies)))
            else:
                if log: print('...workspace %s contains %i studies:'%(
                        self.name,len(self.studies)))
            print( self.studies )
        if log: print('...done.\n')

    def init_study(self, study_name=None, log=True, sanity_check=True):
        '''Initialise the directory hosting a study'''

        # sanity checks
        if sanity_check:
            self._update_list_existing_studies(log=log)
            input_study_name=self._check_study_name(study_name=study_name)
        else:
            input_study_name=input_study_name
        
        if log: print('Initialising study %s in workspace %s...'%(
                input_study_name,self.paths['workspace']))

        # study directory
        study_path = self._inflate_study_path(input_study_name,log=log)
        if not os.path.isdir(study_path):
            os.makedirs(study_path)
            if log: print('...created directory %s'%(study_path))
        else:
            if log: print('...%s directory already exists'%(study_path))

        # template files
        for item in os.listdir(self.paths['templates']):
            sour = os.path.join(self.paths['templates'], item)
            dest = os.path.join(study_path, item)
            if os.path.isfile(sour) and not os.path.isfile(dest):
                shutil.copy2(sour, dest)
                if log: print('...copied template file %s from %s .'%(
                        item,self.paths['templates']))
            else:
                if log: print('...template file %s present.'%(item))

        # update list of existing studies
        self._update_list_existing_studies(log=log)
    
    def load_study(self, study_name, module_path=None, class_name='MyStudy',
                   log=True, sanity_check=True):
        '''Load a study'''
        
        # sanity checks
        if sanity_check:
            self._update_list_existing_studies(log=log)
            input_study_name=self._check_study_name(study_name=study_name)
        else:
            input_study_name=input_study_name
        if (study_name not in self.studies):
            print("Study %s not present in workspace %s"%(
                study_name,self.paths['workspace']))
            print("Please create one with the init_study()")
            exit(1)

        # other sanity checks:
        study_path = self._inflate_study_path(input_study_name,log=log)
        if module_path is None:
            module_path = os.path.join(study_path, 'config.py')
        if not os.path.isfile(module_path):
            print("The config file %s isn't found!"%module_path)
            exit(1)
            
        if log: print('Loading study %s in workspace %s ...'%(
                study_name,self.paths['workspace']))
        module_name = os.path.abspath(module_path)
        module_name = module_name.replace('.py', '')
        mod = SourceFileLoader(module_name, module_path).load_module()
        cls = getattr(mod, class_name)
        if log: print("Study %s loaded from %s \n"%(study_name,study_path))
        return cls(study_name, self.paths['studies'])
