import sys, os, time
import pyoorb
import lsst.sims.catalogs.generation.utils.runDiaCat as rdc
from lsst.sims.catalogs.generation.db import jobDB

def getIds(fhids):
  ids = []
  for line in fhids:
      ids.append(int(line.rstrip()))
  fhids.close()
  return ids

def run(idlist, csize=50000, radius=2.1, outdir='/state/partition1/krughoff', repodir='/share/pogo3/krughoff/diaCats', compress=True, cleanup=False):
    id = int(time.time()*1000)
    je = jobDB.LogEvents("Test job number %i"%(id), jobid=id)
    print "Job number is %i"%(je._jobid)
    try:
        rdc.runDia(csize, idlist , radius, outdir, repodir, je, compress=compress, cleanup=cleanup)
    except Exception, e:
        rdc.writeJobEvent(je, "Exception", description=e.__str__())
        raise e

if __name__ == "__main__":
  run(getIds(open(sys.argv[1])), radius=float(sys.argv[2]), cleanup=True)
