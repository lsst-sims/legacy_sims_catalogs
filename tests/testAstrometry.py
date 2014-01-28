import numpy

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.instance import InstanceCatalog, compound, cached
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData

class testCatalog(InstanceCatalog):
    catalog_type = 'MISC'
    default_columns=[('Opsim_expmjd',5000.0,float)]
    def db_required_columns(self):
        return ['Unrefracted_Dec'],['Opsim_altitude']

class astrometryUnitTest(unittest.TestCase):

    obsMD = DBObject.from_objid('opsim3_61')
    obs_metadata=obsMD.getObservationMetaData(88544919, 0.1, makeCircBounds=True)
    cat=testCatalog(obsMD,obs_metadata=obs_metadata)
    tol=1.0e-5

    def testSphericalToCartesian(self):
        arg1=2.19911485751
        arg2=5.96902604182
        output=self.cat.sphericalToCartesian(arg1,arg2)
        self.assertAlmostEqual(output[0],-5.590169943749473402e-1,7)
        self.assertAlmostEqual(output[1],7.694208842938133897e-1,7)
        self.assertAlmostEqual(output[2],-3.090169943749476178e-1,7)

    def testCartesianToSpherical(self):
         xyz=numpy.zeros((3,3),dtype=float)
         
         -1.528397655830016078e-03 -1.220314328441649110e+00 -1.209496845057127512e+00
-2.015391452804179195e+00 3.209255728096233051e-01 -2.420049632697228503e+00
-1.737023855580406284e+00 -9.876134719050078115e-02 -2.000636201137401038e+00
        

def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(astrometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
