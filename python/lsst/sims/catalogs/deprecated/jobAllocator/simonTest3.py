#!/usr/bin/env python
import copy
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.measures.astrometry import Bbox

if __name__ == "__main__":
    csize = 10
    myqdb = queryDB.queryDB(chunksize=csize, objtype="SSM")
    ic = myqdb.getInstanceCatalogById(85748128)
    print ic.metadata.parameters
    curMD = copy.deepcopy(ic.metadata)
    curMD.validateRequiredMetadata('TRIM', myqdb.opsim)
