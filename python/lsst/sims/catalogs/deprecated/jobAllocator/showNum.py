from lsst.sims.catalogs.generation.db import jobDB

import sys

eM = jobDB.JobState(sys.argv[1])
eM.updateState(sys.argv[1] + 'blah', 'blah')
eM.showStates()
t0 = eM.queryState(sys.argv[1] + 'NumJobs')
print t0
