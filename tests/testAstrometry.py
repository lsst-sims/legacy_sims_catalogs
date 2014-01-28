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
         
        xyz[0][0]=-1.528397655830016078e-03 
        xyz[0][1]=-1.220314328441649110e+00 
        xyz[0][2]=-1.209496845057127512e+00
        xyz[1][0]=-2.015391452804179195e+00 
        xyz[1][1]=3.209255728096233051e-01 
        xyz[1][2]=-2.420049632697228503e+00
        xyz[2][0]=-1.737023855580406284e+00 
        xyz[2][1]=-9.876134719050078115e-02 
        xyz[2][2]=-2.000636201137401038e+00
        
        output=self.cat.cartesianToSpherical(xyz)
        
        self.assertAlmostEqual(output[0][0],-1.571554689325760146e+00,7)
        self.assertAlmostEqual(output[1][0],-7.113500771245374610e-01,7) 
        self.assertAlmostEqual(output[0][1],2.884429715637988778e+00,7)
        self.assertAlmostEqual(output[1][1],-7.811044420646305608e-02,7)
        self.assertAlmostEqual(output[0][2],-2.034269388180792504e+00,7)
        self.assertAlmostEqual(output[1][2],-6.367345775760760995e-01,7) 
        

    def testAngularSeparation(self):
        arg1 = 7.853981633974482790e-01 
        arg2 = 3.769911184307751517e-01 
        arg3 = 5.026548245743668986e+00 
        arg4 = -6.283185307179586232e-01
        
        output=self.cat.angularSeparation(arg1,arg2,arg3,arg4)
        
        self.assertAlmostEqual(output,1.522426604701640152e+00,7)

def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(astrometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
