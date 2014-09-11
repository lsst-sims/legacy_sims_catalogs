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
