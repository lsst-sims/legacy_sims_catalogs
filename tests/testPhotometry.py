import numpy

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.instance import InstanceCatalog, compound, cached
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.astrometry.Astrometry import Astrometry
from lsst.sims.catalogs.measures.astrometry.Site import Site

from lsst.sims.catalogs.measures.photometry.photUtils import Photometry

class testCatalog(InstanceCatalog,Astrometry,Photometry):
    catalog_type = 'MISC'
    default_columns=[('expmjd',5000.0,float)]
    def db_required_columns(self):
        return ['raJ2000'],['varParamStr']
        

class photometryUnitTest(unittest.TestCase):

    galaxy = DBObject.from_objid('galaxyBase')
    rrly = DBObject.from_objid('rrly')
    obsMD = DBObject.from_objid('opsim3_61')
    obs_metadata = obsMD.getObservationMetaData(88544919, 0.1, makeCircBounds = True)
    galcat = testCatalog(galaxy,obs_metadata = obs_metadata)
    rrlycat = testCatalog(rrly,obs_metadata = obs_metadata)
               
        
def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
