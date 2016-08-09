# execfile('dumpJobDB.py')

import os, sys, time
from lsst.sims.catalogs.generation.db import jobDB
import throttleUtils

owner = sys.argv[1]; iD = int(sys.argv[2])

def dump(owner, iD):
    j = jobDB.JobId(iD, owner=owner)
    eDBM = throttleUtils.getCopyDBM(j)
    if eDBM == None:
        raise RuntimeError, '*** No EDBM for ID: %s' % (iD)

    s = eDBM.showStates()
    f = open('dumpJobDB%s_%i.dat' % (owner, iD), 'w')
    for k in s.keys():
        f.write('%s,%s\n' % (k, s[k]))
        print '%s,%s' % (k, s[k])

    f.close()


#while True:
dump(owner, iD)

