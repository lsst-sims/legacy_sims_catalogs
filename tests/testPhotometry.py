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
from lsst.sims.catalogs.measures.photometry.Variability import Variability

class testCatalog(InstanceCatalog,Astrometry,Photometry,Variability):
    catalog_type = 'MISC'
    default_columns=[('expmjd',5000.0,float)]
    def db_required_columns(self):
        return ['raJ2000'],['varParamStr']
        

class photometryUnitTest(unittest.TestCase):

    galaxy = DBObject.from_objid('galaxyBase')
    rrly = DBObject.from_objid('rrly')
    obsMD = DBObject.from_objid('opsim3_61')
    obs_metadata = obsMD.getObservationMetaData(88544919, 0.1, makeCircBounds = True)

    rrlycat = testCatalog(rrly,obs_metadata = obs_metadata)
    
    def testGalaxyVariability(self):   
                
        galcat = testCatalog(self.galaxy,obs_metadata = self.obs_metadata)
        rows = self.galaxy.query_columns(['varParamStr'], constraint = 'VarParamStr is not NULL')
        rows = rows.next()
        print "len ",len(rows)
        for i in range(20):
            #print "i ",i
            mags=galcat.applyVariability(rows[i]['varParamStr'])
            #print mags
        
def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
