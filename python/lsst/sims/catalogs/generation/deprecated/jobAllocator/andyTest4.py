#!/usr/bin/env python
import numpy, math, copy, cPickle, os, sys
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.measures.astrometry import Bbox
import lsst.sims.catalogs.measures.utils as mUtils

csize = 10
myQDB = queryDB.queryDB(chunksize=csize, objtype='ALLSTARS')
ic = myQDB.getInstanceCatalogById(85748128)
print ic.metadata.parameters
mUtils.trimGeneration.derivedTrimMetadata(ic)
ic.makeTrimCoords()
print ic.metadata.parameters
curMD = copy.deepcopy(ic.metadata)

myQDB = queryDB.queryDB(chunksize=csize, objtype='GALAXY_DISK')
ic = myQDB.getInstanceCatalogById(85748128)
print ic.metadata.parameters
mUtils.trimGeneration.derivedTrimMetadata(ic)
ic.makeTrimCoords()
print ic.metadata.parameters
curMD.mergeMetadata(ic.metadata)

myQDB = queryDB.queryDB(chunksize=csize, objtype='GALAXY_BULGE')
ic = myQDB.getInstanceCatalogById(85748128)
print ic.metadata.parameters
mUtils.trimGeneration.derivedTrimMetadata(ic)
ic.makeTrimCoords()
print ic.metadata.parameters
curMD.mergeMetadata(ic.metadata)

myQDB = queryDB.queryDB(chunksize=csize, objtype='AGN')
ic = myQDB.getInstanceCatalogById(85748128)
print ic.metadata.parameters
mUtils.trimGeneration.derivedTrimMetadata(ic)
ic.makeTrimCoords()
print ic.metadata.parameters
curMD.mergeMetadata(ic.metadata)

curMD.writeMetadata('andyTest4MetaData.dat', 'TRIM', myQDB.opsim, newfile=True)

