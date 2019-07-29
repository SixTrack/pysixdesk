import os
import sys
# give the test runner the import access
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pysixdesk
from pysixdesk.lib import machineparams
from pysixdesk.lib import submission