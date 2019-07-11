from .study import Study
from .workspace import WorkSpace
from .pysixdb import SixDB
from .submission import HTCondor

import logging
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

# The module logger is 'pysixdesk'
# to change the logging level, in your script do:
# ---------------------------------------
# import logging
# logger = logging.getLogger('pysixdesk')
# logger.setLevel(logging.ERROR)
# ---------------------------------------

# To add file logging, in your script do:
# ---------------------------------------
# filehandler = logging.FileHandler('log')
# filehandler.setFormatter(logging.Formatter(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
#                                            datefmt='%H:%M:%S'))
# filehandler.setLevel(logging.DEBUG)
# logger.addHandler(filehandler)
# ---------------------------------------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__all__ = []
__all__.append('Study')
__all__.append('SixDB')
__all__.append('WorkSpace')
__all__.append('HTCondor')
