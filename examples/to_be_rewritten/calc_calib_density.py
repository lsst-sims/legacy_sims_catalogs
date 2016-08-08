#!/usr/bin/env python
import pyoorb
from lsst.sims.catalogs.generation.db import queryDB
import math,numpy,sys
import numpy.random as rand
def generateRaDec(n):
  RA = 360.*rand.random(n)
  Dec = numpy.arcsin(2.*rand.random(n)-1.)*180./numpy.pi
  return RA, Dec

if __name__ == "__main__":
  csize = 100000
  nsamp = 10
  magcut = 21.5
  boxsizedeg = 0.2
  halfboxsizedeg = boxsizedeg/2.
  sqarea = (math.sin(halfboxsizedeg*math.pi/180.) -
          math.sin(-halfboxsizedeg*math.pi/180.))*(boxsizedeg*math.pi/180.)
  radius = math.acos(1-(sqarea/(2.*math.pi)))*180./math.pi
  ra,dec = generateRaDec(nsamp)
  fhout = open(sys.argv[1], "w")
  for i in xrange(nsamp):
    nums = {'u':0,'g':0,'r':0,'i':0,'z':0,'y':0}
    expmjd = 0.
    myqdb = queryDB.queryDB(chunksize=csize,objtype='CALIBSTARS',filetypes=('CALIB',))
    ic = myqdb.getInstanceCatalogByCirc(ra[i],dec[i],radius,expmjd=expmjd)
    while not ic is None:
      for f in nums.keys():
        nums[f] += len(numpy.where(ic.dataArray[f] < magcut)[0])
      ic = myqdb.getNextChunk()
    fhout.write(",".join([str(x) for x in\
        (ra[i],dec[i],sqarea,nums['u'],nums['g'],nums['r'],nums['i'],nums['z'],nums['y'])])+"\n")
    fhout.flush()
  fhout.close()

