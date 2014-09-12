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
    class variables (unrefractedRA, unrefractedDec, etc.)
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
    
    def testObsMetaDataAssignment(self):
        """
        Test that you get an error when you pass something that is not
        ObservationMetaData as obs_metadata
        """
        
        xx=5.0
        self.assertRaises(ValueError,myCatalogClass,self.myDB,obs_metadata=xx)
        
    def testDefault(self):
    
        testCat = myCatalogClass(self.myDB)
        
        self.assertEqual(testCat.unrefractedRA(),None)
        self.assertEqual(testCat.unrefractedDec(),None)
        self.assertAlmostEqual(testCat.rotSkyPos(),0.0,10)
        self.assertEqual(testCat.bandpass(),'r')
        
        self.assertAlmostEqual(testCat.site.longitude,-1.2320792,10)
        self.assertAlmostEqual(testCat.site.latitude,-0.517781017,10)
        self.assertAlmostEqual(testCat.site.height,2650,10)
        self.assertAlmostEqual(testCat.site.xPolar,0,10)
        self.assertAlmostEqual(testCat.site.yPolar,0,10)
        self.assertAlmostEqual(testCat.site.meanTemperature,284.655,10)
        self.assertAlmostEqual(testCat.site.meanPressure,749.3,10)
        self.assertAlmostEqual(testCat.site.meanHumidity,0.4,10)
        self.assertAlmostEqual(testCat.site.lapseRate,0.0065,10)

    def testAssignment(self):
        mjd = 5120.0
        RA = 1.5
        Dec = -1.1
        rotSkyPos = -0.2
        
        testSite = Site(longitude = 2.0, latitude = -1.0, height = 4.0,
            xPolar = 0.5, yPolar = -0.5, meanTemperature = 100.0,
            meanPressure = 500.0, meanHumidity = 0.1, lapseRate = 0.1)
        
        testObsMD = ObservationMetaData(site=testSite, 
            mjd=mjd, unrefractedRA=RA,
            unrefractedDec=Dec, rotSkyPos=rotSkyPos, bandpassName = 'z')    
        
        testCat = myCatalogClass(self.myDB,obs_metadata=testObsMD)
        
        self.assertAlmostEqual(testCat.mjd(),5120.0,10)
        self.assertAlmostEqual(testCat.unrefractedRA(),1.5,10)
        self.assertAlmostEqual(testCat.unrefractedDec(),-1.1,10)
        self.assertAlmostEqual(testCat.rotSkyPos(),-0.2,10)
        self.assertEqual(testCat.bandpass(),'z')
        
        self.assertAlmostEqual(testCat.site.longitude,2.0,10)
        self.assertAlmostEqual(testCat.site.latitude,-1.0,10)
        self.assertAlmostEqual(testCat.site.height,4.0,10)
        self.assertAlmostEqual(testCat.site.xPolar,0.5,10)
        self.assertAlmostEqual(testCat.site.yPolar,-0.5,10)
        self.assertAlmostEqual(testCat.site.meanTemperature,100.0,10)
        self.assertAlmostEqual(testCat.site.meanPressure,500.0,10)
        self.assertAlmostEqual(testCat.site.meanHumidity,0.1,10)
        self.assertAlmostEqual(testCat.site.lapseRate,0.1,10)
        
        #
        #Note: because of how we have chosen to handle the observation
        #metadata data, changing the obs_metadata object outside of 
        #InstanceCatalog will change the results of calls to the InstanceCatalog
        #methods
        #
        
        phosimMD = OrderedDict([('Unrefracted_RA', (-2.0,float)), 
                                ('Unrefracted_Dec', (0.9,float)),
                                ('Opsim_rotskypos', (1.1,float)), 
                                ('Opsim_expmjd',(4000.0,float)),
                                ('Opsim_filter',(1,int))])
        
        testObsMD.assignPhoSimMetaData(phosimMD)
        
        self.assertAlmostEqual(testCat.mjd(),4000.0,10)
        self.assertAlmostEqual(testCat.unrefractedRA(),-2.0,10)
        self.assertAlmostEqual(testCat.unrefractedDec(),0.9,10)
        self.assertAlmostEqual(testCat.rotSkyPos(),1.1,10)
        self.assertEqual(testCat.bandpass(),'g')
        
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
