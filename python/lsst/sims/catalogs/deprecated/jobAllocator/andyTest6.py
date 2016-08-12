#!/usr/bin/env python
import numpy, math, copy, cPickle, os, sys, cPickle
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.measures.astrometry import Bbox
import lsst.sims.catalogs.measures.utils as mUtils

tNum = 0
curMD = None
cSize = 500000
obsId = 85679372
types = ['ALLSTARS']

def doIC(ic, myQDB, curMD, tNum):
    print 'HANDLING NEW IC'
    mUtils.trimGeneration.derivedTrimMetadata(ic)
    print ic.metadata.parameters
    ic.makeTrimCoords()
    if curMD == None: curMD = copy.deepcopy(ic.metadata)
    else: curMD.mergeMetadata(ic.metadata)
    tBase = 'aT6IC%i' % tNum
    print '   WRITING TO PICKLE'
    cPickle.dump(ic, open(tBase + '.p', 'w'))
    print '   READING FROM PICKLE'
    icNew = cPickle.load(open(tBase + '.p'))
    print icNew.dataArray
    icNew.makeTrimCoords()
    icNew.validateData('TRIM')
    icNew.writeCatalogData(tBase + '.txt', 'TRIM')
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

curMD.writeMetadata('aT6.meta','TRIM', myQDB.opsim, newfile = True)
