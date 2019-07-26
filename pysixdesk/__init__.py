from .lib.study import Study
from .lib.workspace import WorkSpace
from .lib.pysixdb import SixDB
from .lib.submission import HTCondor

import logging
# The module level logger is 'pysixdesk'

default_frmt = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                                 datefmt='%H:%M:%S')

logger = logging.getLogger(__name__)  # logger name: 'pysixdesk'
sh = logging.StreamHandler()
sh.setFormatter(default_frmt)
logger.addHandler(sh)
logger.setLevel(logging.INFO)

__all__ = []
__all__.append('Study')
__all__.append('SixDB')
__all__.append('WorkSpace')
__all__.append('HTCondor')
