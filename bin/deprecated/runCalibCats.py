import sys, os, time
import lsst.sims.catalogs.generation.utils.runCalibCat as rcc
from lsst.sims.catalogs.generation.db import jobDB

def getIds(offset, number, fhids):
  ids = []
  for i,line in enumerate(fhids):
    if i >= offset and i < offset+number:
      flds = line.rstrip().split()
      ids.append(int(flds[0]))
  fhids.close()
  return ids

def run(id, csize=50000, radius=2.1, outdir='./testOut/', repodir='./testRepo/', compress=True, cleanup=True):
    je = jobDB.LogEvents("Test job number %i"%(id), jobid=id)
    print "Job number is %i"%(je._jobid)
    try:
        rcc.runCalib(
          csize, id, radius, outdir, repodir, je,
          compress=compress, cleanup=cleanup)
    except Exception, e:
        rcc.writeJobEvent(je, "Exception", description=e.__str__())
        raise e

if __name__ == "__main__":
  obshist_id = int(sys.argv[1])
  cat_radius = float(sys.argv[2])
  run(obshist_id, radius=cat_radius, repodir='./testRepo/', csize=20000, cleanup=True)
