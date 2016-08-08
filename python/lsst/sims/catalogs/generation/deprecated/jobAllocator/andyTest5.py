#!/usr/bin/env python
import numpy, math, copy, cPickle, os, sys
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.measures.astrometry import Bbox
import lsst.sims.catalogs.measures.utils as mUtils

tNum = 0
curMD = None
cSize = 500000
obsId = 85679372
#types = ['ALLSTARS', 'GALAXY_DISK', 'GALAXY_BULGE', 'AGN']
types = ['ALLSTARS', 'AGN']

def doIC(ic, myQDB, curMD, tNum):
    print 'HANDLING NEW IC'
    mUtils.trimGeneration.derivedTrimMetadata(ic)
    print ic.metadata.parameters
    ic.makeTrimCoords()
    ic.metadata.validateMetadata('TRIM', myQDB.opsim)
    print ic.dataArray
    ic.validateData('TRIM')
    t0 = 'aT5IC%i' % tNum
    ic.writeCatalogData(t0 + '.txt', 'TRIM')
    if curMD == None: curMD = copy.deepcopy(ic.metadata)
    else: curMD.mergeMetadata(ic.metadata)
    return curMD

def doQueryType(t, curMD, tNum, cSize=cSize, obsId=obsId):
    print 'NOW QUERYING:', t
    myQDB = queryDB.queryDB(chunksize=cSize, objtype=t)
    ic = myQDB.getInstanceCatalogById(obsId)
    while ic != None:
        curMD = doIC(ic, myQDB, curMD, tNum)
        tNum += 1
        ic = myQDB.getNextChunk()
    return curMD, tNum, myQDB

for t in types:
    curMD, tNum, myQDB = doQueryType(t, curMD, tNum)

curMD.writeMetadata('aT5.meta','TRIM', myQDB.opsim, newfile = True)
