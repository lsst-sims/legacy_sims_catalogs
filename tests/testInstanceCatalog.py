import os
import numpy
import sqlite3
import unittest
import lsst.utils.tests as utilsTests
from collections import OrderedDict
from lsst.sims.catalogs.generation.db import ObservationMetaData, CatalogDBObject, Site
from lsst.sims.catalogs.generation.utils import myTestStars, makeStarTestDB
from lsst.sims.catalogs.measures.instance import InstanceCatalog

def createCannotBeNullTestDB():
    """
    Create a database to test the 'cannot_be_null' functionality in InstanceCatalog

    This method will return the contents of the database as a recarray for baseline comparison
    in the unit tests.
    """

    dbName = 'cannotBeNullTest.db'
    numpy.random.seed(32)
    dtype = numpy.dtype([('id',int),('n1',numpy.float64),('n2',numpy.float64),('n3',numpy.float64)])
    output = None

    if os.path.exists(dbName):
        os.unlink(dbName)

    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE testTable (id int, n1 float, n2 float, n3 float)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")

    for ii in range(100):

        values = numpy.random.sample(3);
        for i in range(len(values)):
            draw = numpy.random.sample(1)
            if draw[0]<0.5:
                values[i] = None

        if output is None:
            output=numpy.array([(ii,values[0],values[1],values[2])], dtype = dtype)
        else:
            size = output.size
            output.resize(size+1)
            output[size] = (ii, values[0], values[1], values[2])

        if numpy.isnan(values[0]):
            v0 = 'NULL'
        else:
            v0 = str(values[0])

        if numpy.isnan(values[1]):
            v1 = 'NULL'
        else:
            v1 = str(values[1])

        if numpy.isnan(values[2]):
            v2 = 'NULL'
        else:
            v2 = str(values[2])

        cmd = '''INSERT INTO testTable VALUES (%s, %s, %s, %s)''' % (ii,v0,v1,v2)
        c.execute(cmd)

    conn.commit()
    conn.close()
    return output

class myCannotBeNullDBObject(CatalogDBObject):
    dbAddress = 'sqlite:///cannotBeNullTest.db'
    tableid = 'testTable'
    objid = 'cannotBeNull'
    idColKey = 'id'

class myCannotBeNullCatalog(InstanceCatalog):
    """
    This catalog class will not write rows with a null value in the n2 column
    """
    column_outputs = ['id','n1','n2','n3']
    cannot_be_null = ['n2']
    catalog_type = 'cannotBeNull'

class myCanBeNullCatalog(InstanceCatalog):
    """
    This catalog class will write all rows to the catalog
    """
    column_outputs = ['id','n1','n2','n3']
    catalog_type = 'canBeNull'

class myCatalogClass(InstanceCatalog):
    column_outputs = ['raJ2000','decJ2000']

class InstanceCatalogMetaDataTest(unittest.TestCase):
    """
    This class will test how Instance catalog handles the metadata
    class variables (unrefractedRA, unrefractedDec, etc.)
    """

    @classmethod
    def setUpClass(cls):
        if os.path.exists('testInstanceCatalogDatabase.db'):
            os.unlink('testInstanceCatalogDatabase.db')

        makeStarTestDB(filename='testInstanceCatalogDatabase.db')

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('testInstanceCatalogDatabase.db'):
            os.unlink('testInstanceCatalogDatabase.db')

    def setUp(self):
        self.myDB = myTestStars(address = 'sqlite:///testInstanceCatalogDatabase.db')

    def tearDown(self):
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

        self.assertEqual(testCat.unrefractedRA,None)
        self.assertEqual(testCat.unrefractedDec,None)
        self.assertAlmostEqual(testCat.rotSkyPos,0.0,10)
        self.assertEqual(testCat.bandpass,'r')

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

        self.assertAlmostEqual(testCat.mjd,5120.0,10)
        self.assertAlmostEqual(numpy.degrees(testCat.unrefractedRA),1.5,10)
        self.assertAlmostEqual(numpy.degrees(testCat.unrefractedDec),-1.1,10)
        self.assertAlmostEqual(testCat.rotSkyPos,-0.2,10)
        self.assertEqual(testCat.bandpass,'z')

        self.assertAlmostEqual(testCat.site.longitude,2.0,10)
        self.assertAlmostEqual(testCat.site.latitude,-1.0,10)
        self.assertAlmostEqual(testCat.site.height,4.0,10)
        self.assertAlmostEqual(testCat.site.xPolar,0.5,10)
        self.assertAlmostEqual(testCat.site.yPolar,-0.5,10)
        self.assertAlmostEqual(testCat.site.meanTemperature,100.0,10)
        self.assertAlmostEqual(testCat.site.meanPressure,500.0,10)
        self.assertAlmostEqual(testCat.site.meanHumidity,0.1,10)
        self.assertAlmostEqual(testCat.site.lapseRate,0.1,10)

        phosimMD = OrderedDict([('Unrefracted_RA', (-2.0,float)),
                                ('Unrefracted_Dec', (0.9,float)),
                                ('Opsim_rotskypos', (1.1,float)),
                                ('Opsim_expmjd',(4000.0,float)),
                                ('Opsim_filter',(1,int))])

        testObsMD.assignPhoSimMetaData(phosimMD)

        self.assertAlmostEqual(testCat.mjd,5120.0,10)
        self.assertAlmostEqual(numpy.degrees(testCat.unrefractedRA),1.5,10)
        self.assertAlmostEqual(numpy.degrees(testCat.unrefractedDec),-1.1,10)
        self.assertAlmostEqual(testCat.rotSkyPos,-0.2,10)
        self.assertEqual(testCat.bandpass,'z')

        testObsMD.site.longitude=-2.0
        testObsMD.site.latitude=-2.0
        testObsMD.site.height=-2.0
        testObsMD.site.xPolar=-2.0
        testObsMD.site.yPolar=-2.0
        testObsMD.site.meanTemperature=-2.0
        testObsMD.site.meanPressure=-2.0
        testObsMD.site.meanHumidity=-2.0
        testObsMD.site.lapseRate=-2.0

        self.assertAlmostEqual(testCat.site.longitude,2.0,10)
        self.assertAlmostEqual(testCat.site.latitude,-1.0,10)
        self.assertAlmostEqual(testCat.site.height,4.0,10)
        self.assertAlmostEqual(testCat.site.xPolar,0.5,10)
        self.assertAlmostEqual(testCat.site.yPolar,-0.5,10)
        self.assertAlmostEqual(testCat.site.meanTemperature,100.0,10)
        self.assertAlmostEqual(testCat.site.meanPressure,500.0,10)
        self.assertAlmostEqual(testCat.site.meanHumidity,0.1,10)
        self.assertAlmostEqual(testCat.site.lapseRate,0.1,10)

class InstanceCatalogCannotBeNullTest(unittest.TestCase):

        def setUp(self):
            self.baselineOutput = createCannotBeNullTestDB()

        def tearDown(self):
            del self.baselineOutput
            if os.path.exists('cannotBeNullTest.db'):
                os.unlink('cannotBeNullTest.db')

        def testCannotBeNull(self):
            """
            Test to make sure that the code for filtering out rows with null values
            in key catalogs works.
            """
            dbobj = CatalogDBObject.from_objid('cannotBeNull')
            cat = dbobj.getCatalog('cannotBeNull')
            fileName = 'cannotBeNullTestFile.txt'
            cat.write_catalog(fileName)
            dtype = numpy.dtype([('id',int),('n1',numpy.float64),('n2',numpy.float64),('n3',numpy.float64)])
            testData = numpy.genfromtxt(fileName,dtype=dtype,delimiter=',')

            j = 0
            for i in range(len(self.baselineOutput)):
                if not numpy.isnan(self.baselineOutput['n2'][i]):
                    for (k,xx) in enumerate(self.baselineOutput[i]):
                        if not numpy.isnan(xx):
                            self.assertAlmostEqual(xx,testData[j][k],3)
                        else:
                            self.assertTrue(numpy.isnan(testData[j][k]))
                    j+=1

            self.assertEqual(i,99)
            self.assertEqual(j,len(testData))

            if os.path.exists(fileName):
                os.unlink(fileName)

        def testCanBeNull(self):
            """
            Test to make sure that we can still write all rows to catalogs,
            even those with null values in key columns
            """
            dbobj = CatalogDBObject.from_objid('cannotBeNull')
            cat = dbobj.getCatalog('canBeNull')
            fileName = 'canBeNullTestFile.txt'
            cat.write_catalog(fileName)
            dtype = numpy.dtype([('id',int),('n1',numpy.float64),('n2',numpy.float64),('n3',numpy.float64)])
            testData = numpy.genfromtxt(fileName,dtype=dtype,delimiter=',')

            for i in range(len(self.baselineOutput)):
                if not numpy.isnan(self.baselineOutput['n2'][i]):
                    for (k,xx) in enumerate(self.baselineOutput[i]):
                        if not numpy.isnan(xx):
                            self.assertAlmostEqual(xx,testData[i][k],3)
                        else:
                            self.assertTrue(numpy.isnan(testData[i][k]))

            self.assertEqual(i,99)

            if os.path.exists(fileName):
                os.unlink(fileName)

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(InstanceCatalogMetaDataTest)
    suites += unittest.makeSuite(InstanceCatalogCannotBeNullTest)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
