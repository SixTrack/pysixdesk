from .study import Study
from .workspace import WorkSpace
from .pysixdb import SixDB
from .submission import HTCondor


import logging
# The module level logger is 'pysixdesk'
# to change the logging level, in your script do:
# ---------------------------------------
# logger = logging.getLogger('pysixdesk')
# logger.setLevel(logging.ERROR)
# ---------------------------------------

# To add logging to file, in your script do:
# ---------------------------------------
# filehandler = logging.FileHandler(log_path)
# filehandler.setFormatter(logging.Formatter(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
#                                            datefmt='%H:%M:%S'))
# filehandler.setLevel(logging.DEBUG)
# logger.addHandler(filehandler)
# ---------------------------------------

# To set the logging level, in your script do:
# ---------------------------------------
# logger = logging.getLogger('pysixdesk')
# logger.setLevel(logger.WARNING)
# ---------------------------------------

logger = logging.getLogger(__name__)  # logger name: 'pysixdesk'
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                                  datefmt='%H:%M:%S'))
logger.addHandler(sh)
logger.setLevel(logging.INFO)

__all__ = []
__all__.append('Study')
__all__.append('SixDB')
__all__.append('WorkSpace')
__all__.append('HTCondor')
