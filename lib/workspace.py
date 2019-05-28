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
        self._updateListExistingStudies()

    # check methods
    
    def _checkName(self):
        if ( len(self.name)==0 ):
            print("...undefined workspace! Please create one first")
            return False
        
    def _checkStudyName(self,studyName):
        iStudyName=studyName
        if (iStudyName is None):
            if (len(self.studies)>0):
                iStudyName='study_%03i'%(len(self.studies))
            else:
                iStudyName='test'
        elif (not isinstance(iStudyName, str)):
            print('invalid string for study name.')
            exit(1)
        return iStudyName
        
    # path methods
    
    def _inflatePaths(self,log=True,sanityCheck=True):
        '''assemble structural full-paths of current workspace'''
        if sanityCheck: self._checkName()
        if log: print('Inflating paths of workspace %s ...'%(self.name))
        self.paths['workspace']=os.path.abspath(self.name)
        self.paths['studies']=os.path.join(self.paths['workspace'], 'studies')
        self.paths['templates']=os.path.join(self.paths['workspace'], 'templates')
        
    def _inflateStudyPath(self,studyName,log=True,sanityCheck=True):
        if sanityCheck:
            iStudyName=self._checkStudyName(studyName)
            self._inflatePaths(log=log)
        else:
            iStudyName=studyName
        if log: print('Inflating path to study %s ...'%(iStudyName))
        return os.path.join(self.paths['studies'], studyName)

    # init methods

    def _initDirs(self,log=True,sanityCheck=True):
        '''Initialise directories of current workspace, including copy of template files'''
        if sanityCheck: self._inflatePaths(log=log)
        
        if log: print('Checking directories of workspace %s ...'%(self.name))
        for key in self.paths.keys():
            if not os.path.isdir(self.paths[key]):
                os.mkdir(self.paths[key])
                if log: print('...created %s directory: %s'%(key,self.paths[key]))
            else:
                if log: print('...%s directory already exists: %s'%(key,self.paths[key]))

        if log: print ('Checking template files in %s...'%(self.paths['templates']))
        tem_path = os.path.join(utils.pySixDeskAbsPath, 'templates')
        for item in os.listdir(tem_path):
            s = os.path.join(tem_path, item)
            d = os.path.join(self.paths['templates'], item)
            if os.path.isfile(s) and not os.path.isfile(d):
                shutil.copy2(s, d)
                if log: print('...copied template file %s from %s .'%(item,utils.pySixDeskAbsPath))
            else:
                if log: print('...template file %s present.'%(item))
        if log: print('...done.\n')

    def _updateListExistingStudies(self,log=True,sanityCheck=True):
        '''Update and report list of existing studies in the current workspace'''
        if sanityCheck: self._initDirs(log=log)
        if log: print ('Loading list of studies in %s...'%(self.paths['studies']))
        for item in os.listdir(self.paths['studies']):
            if os.path.isdir(os.path.join(self.paths['studies'], item)):
                if (item not in self.studies):
                    self.studies.append(item)
        if (len(self.studies)==0):
            if log: print('...workspace %s contains no studies at the moment'%(self.name))
        else:
            if (len(self.studies)==1):
                if log: print('...workspace %s contains %i study:'%(self.name,len(self.studies)))
            else:
                if log: print('...workspace %s contains %i studies:'%(self.name,len(self.studies)))
            print( self.studies )
        if log: print('...done.\n')

    def initStudy(self, studyName=None, log=True, sanityCheck=True):
        '''Initialise the directory hosting a study'''

        # sanity checks
        if sanityCheck:
            self._updateListExistingStudies(log=log)
            iStudyName=self._checkStudyName(studyName=studyName)
        else:
            iStudyName=iStudyName
        
        if log: print('Initialising study %s in workspace %s...'%(iStudyName,self.paths['workspace']))

        # study directory
        studyPath = self._inflateStudyPath(iStudyName,log=log)
        if not os.path.isdir(studyPath):
            os.makedirs(studyPath)
            if log: print('...created directory %s'%(studyPath))
        else:
            if log: print('...%s directory already exists'%(studyPath))

        # template files
        for item in os.listdir(self.paths['templates']):
            s = os.path.join(self.paths['templates'], item)
            d = os.path.join(studyPath, item)
            if os.path.isfile(s) and not os.path.isfile(d):
                shutil.copy2(s, d)
                if log: print('...copied template file %s from %s .'%(item,self.paths['templates']))
            else:
                if log: print('...template file %s present.'%(item))

        # update list of existing studies
        self._updateListExistingStudies(log=log)
    
    def loadStudy(self, studyName, modulePath=None, className='MyStudy', log=True, sanityCheck=True):
        '''Load a study'''
        
        # sanity checks
        if sanityCheck:
            self._updateListExistingStudies(log=log)
            iStudyName=self._checkStudyName(studyName=studyName)
        else:
            iStudyName=iStudyName
        if (studyName not in self.studies):
            print("Study %s not present in workspace %s"%(studyName,self.paths['workspace']))
            print("Please create one with the initStudy() method of the class WorkSpace")
            exit(1)

        # other sanity checks:
        studyPath = self._inflateStudyPath(iStudyName,log=log)
        if modulePath is None:
            modulePath = os.path.join(studyPath, 'config.py')
        if not os.path.isfile(modulePath):
            print("The config file %s isn't found!"%modulePath)
            exit(1)
            
        if log: print('Loading study %s in workspace %s ...'%(studyName,self.paths['workspace']))
        moduleName = os.path.abspath(modulePath)
        moduleName = moduleName.replace('.py', '')
        mod = SourceFileLoader(moduleName, modulePath).load_module()
        cls = getattr(mod, className)
        if log: print("Study %s loaded from %s \n"%(studyName,studyPath))
        return cls(studyName, self.paths['studies'])
