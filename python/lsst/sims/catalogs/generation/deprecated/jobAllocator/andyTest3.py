#!/usr/bin/env python
import numpy, math, copy, cPickle, os, sys
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.measures.astrometry import Bbox
import lsst.sims.catalogs.measures.utils as mUtils

csize = 10
myqdb = queryDB.queryDB(chunksize=csize, objtype=sys.argv[1])
ic = myqdb.getInstanceCatalogById(85748128)
print ic.metadata.parameters
mUtils.trimGeneration.derivedTrimMetadata(ic)
ic.calculateUnrefractedAltAz()
print ic.metadata.parameters
curMD = copy.deepcopy(ic.metadata)
curMD.writeMetadata('testMetadata.dat', 'TRIM', myqdb.opsim, newfile=True)
cPickle.dump(ic, open('test.pkl', 'w'))
#ic.makeHelio()
ic.makeTrimCoords()
ic.metadata.validateMetadata('TRIM', myqdb.opsim)
print ic.dataArray
ic.validateData('TRIM')
ic.writeCatalogData('iCTest.txt', 'TRIM')
ic.metadata.writeMetadata('iCTest.meta','TRIM', myqdb.opsim, newfile = True)

