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
        
        self.assertAlmostEqual(output,2.162615946398791955e+00,7)
    
    def testRotationMatrixFromVectors(self):
        v1=numpy.zeros((3),dtype=float)
        v2=numpy.zeros((3),dtype=float)
        v3=numpy.zeros((3),dtype=float)
        
        v1[0]=-3.044619987218469825e-01 
        v2[0]=5.982190522311925385e-01
        v1[1]=-5.473550908956383854e-01 
        v2[1]=-5.573565912346714057e-01 
        v1[2]=7.795545496018386755e-01 
        v2[2]=-5.757495946632366079e-01
        
        output=self.cat.rotationMatrixFromVectors(v1,v2)
        
        for i in range(3):
            for j in range(3):
                v3[i]+=output[i][j]*v1[j] 
        
        for i in range(3):
            self.assertAlmostEqual(v3[i],v2[i],7)

    def testApplyPrecession(self):
    
        ra=numpy.zeros((3),dtype=float)
        dec=numpy.zeros((3),dtype=float)
        
        ra[0]=2.549091039839124218e+00 
        dec[0]=5.198752733024248895e-01
        ra[1]=8.693375673649429425e-01 
        dec[1]=1.038086165642298164e+00
        ra[2]=7.740864769302191473e-01 
        dec[2]=2.758053025017753179e-01
        
        output=self.cat.applyPrecession(ra,dec)
        self.assertAlmostEqual(output[0][0],2.514361575034799401e+00,6)
        self.assertAlmostEqual(output[1][0], 5.306722463159389003e-01,6)
        self.assertAlmostEqual(output[0][1],8.224493314855578774e-01,6)
        self.assertAlmostEqual(output[1][1],1.029318353760459104e+00,6)
        self.assertAlmostEqual(output[0][2],7.412362765815005972e-01,6)
        self.assertAlmostEqual(output[1][2],2.662034339930458571e-01,6)

    def testApplyProperMotion(self):
    
        ra=numpy.zeros((3),dtype=float)
        dec=numpy.zeros((3),dtype=float)
        pm_ra=numpy.zeros((3),dtype=float)
        pm_dec=numpy.zeros((3),dtype=float)
        parallax=numpy.zeros((3),dtype=float)
        v_rad=numpy.zeros((3),dtype=float)
        
        ra[0]=2.549091039839124218e+00 
        dec[0]=5.198752733024248895e-01 
        pm_ra[0]=-8.472633255615005918e-05 
        pm_dec[0]=-5.618517146980475171e-07 
        parallax[0]=9.328946209650547383e-02 
        v_rad[0]=3.060308412186171267e+02 
         
        ra[1]=8.693375673649429425e-01 
        dec[1]=1.038086165642298164e+00 
        pm_ra[1]=-5.848962163813087908e-05 
        pm_dec[1]=-3.000346282603337522e-05 
        parallax[1]=5.392364722571952457e-02 
        v_rad[1]=4.785834687356999098e+02 
        
        ra[2]=7.740864769302191473e-01 
        dec[2]=2.758053025017753179e-01 
        pm_ra[2]=5.904070507320858615e-07 
        pm_dec[2]=-2.958381482198743105e-05 
        parallax[2]=2.172865273161764255e-02 
        v_rad[2]=-3.225459751425886452e+02
        
        ep=2.001040286039033845e+03 
        mjd=2.018749109074271473e+03 
        
        output=self.cat.applyProperMotion(ra,dec,pm_ra,pm_dec,parallax,v_rad,EP0=ep,MJD=mjd)
        
        self.assertAlmostEqual(output[0][0],2.549309127917495754e+00,6) 
        self.assertAlmostEqual(output[1][0],5.198769294314042888e-01,6)
        self.assertAlmostEqual(output[0][1],8.694881589882680339e-01,6) 
        self.assertAlmostEqual(output[1][1],1.038238225568303363e+00,6)
        self.assertAlmostEqual(output[0][2],7.740849573146946216e-01,6) 
        self.assertAlmostEqual(output[1][2],2.758844356561930278e-01,6)

    def testEquatorialToGalactic(self):
    
        ra=numpy.zeros((3),dtype=float)
        dec=numpy.zeros((3),dtype=float)
        
        ra[0]=2.549091039839124218e+00 
        dec[0]=5.198752733024248895e-01
        ra[1]=8.693375673649429425e-01 
        dec[1]=1.038086165642298164e+00
        ra[2]=7.740864769302191473e-01 
        dec[2]=2.758053025017753179e-01
        
        output=self.cat.equatorialToGalactic(ra,dec)
        
        self.assertAlmostEqual(output[0][0],3.452036693523627964e+00,6) 
        self.assertAlmostEqual(output[1][0],8.559512505657201897e-01,6)
        self.assertAlmostEqual(output[0][1],2.455968474619387720e+00,6) 
        self.assertAlmostEqual(output[1][1],3.158563770667878468e-02,6)
        self.assertAlmostEqual(output[0][2],2.829585540991265358e+00,6) 
        self.assertAlmostEqual(output[1][2],-6.510790587552289788e-01,6)


    def testGalacticToEquatorial(self):
        
        lon=numpy.zeros((3),dtype=float)
        lat=numpy.zeros((3),dtype=float)
        
        lon[0]=3.452036693523627964e+00 
        lat[0]=8.559512505657201897e-01
        lon[1]=2.455968474619387720e+00 
        lat[1]=3.158563770667878468e-02
        lon[2]=2.829585540991265358e+00 
        lat[2]=-6.510790587552289788e-01
        
        output=self.cat.galacticToEquatorial(lon,lat)
        
        self.assertAlmostEqual(output[0][0],2.549091039839124218e+00,6) 
        self.assertAlmostEqual(output[1][0],5.198752733024248895e-01,6)
        self.assertAlmostEqual(output[0][1],8.693375673649429425e-01,6) 
        self.assertAlmostEqual(output[1][1],1.038086165642298164e+00,6)
        self.assertAlmostEqual(output[0][2],7.740864769302191473e-01,6) 
        self.assertAlmostEqual(output[1][2],2.758053025017753179e-01,6)
        
        
def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(astrometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
