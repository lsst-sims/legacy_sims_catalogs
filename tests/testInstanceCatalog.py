import os
import unittest
import lsst.utils.tests as utilsTests
from collections import OrderedDict
from lsst.sims.catalogs.generation.db import ObservationMetaData, Site
from lsst.sims.catalogs.generation.utils import myTestStars, makeStarTestDB
from lsst.sims.catalogs.measures.instance import InstanceCatalog

class myCatalogClass(InstanceCatalog):
    column_outputs = ['raJ2000','decJ2000']

class InstanceCatalogMetaDataTest(unittest.TestCase):
    """
    This class will test how Instance catalog handles the metadata
    class variables (UnrefractedRA, UnrefractedDec, etc.)
    """
    
    def setUp(self):
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
            
        makeStarTestDB()
        self.myDB = myTestStars()
    
    def tearDown(self):
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
        
        del self.myDB
        
    def testDefault(self):
    
        testObsMD = ObservationMetaData()
        testCat = myCatalogClass(self.myDB,obs_metadata=testObsMD)
        
        self.assertAlmostEqual(testCat.UnrefractedRA(),0.0,10)
        self.assertAlmostEqual(testCat.UnrefractedDec(),-0.5,10)
        self.assertAlmostEqual(testCat.RotSkyPos(),0.0,10)
        self.assertEqual(testCat.bandpass(),'i')
        
        self.assertAlmostEqual(testCat.site().longitude,-1.2320792,10)
        self.assertAlmostEqual(testCat.site().latitude,-0.517781017,10)
        self.assertAlmostEqual(testCat.site().height,2650,10)
        self.assertAlmostEqual(testCat.site().xPolar,0,10)
        self.assertAlmostEqual(testCat.site().yPolar,0,10)
        self.assertAlmostEqual(testCat.site().meanTemperature,284.655,10)
        self.assertAlmostEqual(testCat.site().meanPressure,749.3,10)
        self.assertAlmostEqual(testCat.site().meanHumidity,0.4,10)
        self.assertAlmostEqual(testCat.site().lapseRate,0.0065,10)

    def testAssignment(self):
        mjd = 5120.0
        RA = 1.5
        Dec = -1.1
        RotSkyPos = -0.2
        
        testSite = Site(longitude = 2.0, latitude = -1.0, height = 4.0,
            xPolar = 0.5, yPolar = -0.5, meanTemperature = 100.0,
            meanPressure = 500.0, meanHumidity = 0.1, lapseRate = 0.1)
        
        testObsMD = ObservationMetaData(site=testSite, 
            mjd=mjd, UnrefractedRA=RA,
            UnrefractedDec=Dec, RotSkyPos=RotSkyPos, bandpassName = 'z')    
        
        testCat = myCatalogClass(self.myDB,obs_metadata=testObsMD)
        
        self.assertAlmostEqual(testCat.mjd(),5120.0,10)
        self.assertAlmostEqual(testCat.UnrefractedRA(),1.5,10)
        self.assertAlmostEqual(testCat.UnrefractedDec(),-1.1,10)
        self.assertAlmostEqual(testCat.RotSkyPos(),-0.2,10)
        self.assertEqual(testCat.bandpass(),'z')
        
        self.assertAlmostEqual(testCat.site().longitude,2.0,10)
        self.assertAlmostEqual(testCat.site().latitude,-1.0,10)
        self.assertAlmostEqual(testCat.site().height,4.0,10)
        self.assertAlmostEqual(testCat.site().xPolar,0.5,10)
        self.assertAlmostEqual(testCat.site().yPolar,-0.5,10)
        self.assertAlmostEqual(testCat.site().meanTemperature,100.0,10)
        self.assertAlmostEqual(testCat.site().meanPressure,500.0,10)
        self.assertAlmostEqual(testCat.site().meanHumidity,0.1,10)
        self.assertAlmostEqual(testCat.site().lapseRate,0.1,10)
    
def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(InstanceCatalogMetaDataTest)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
