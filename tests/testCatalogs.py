from __future__ import with_statement
import os
import numpy
import unittest
import eups
import lsst.utils.tests as utilsTests
from lsst.sims.catalogs.generation.db import CatalogDBObject, ObservationMetaData, haversine
from lsst.sims.catalogs.measures.instance import InstanceCatalog, compound
import lsst.sims.catalogs.generation.utils.testUtils as tu

#a class of catalog that outputs all the significant figures in
#ra and dec so that it can be read back in to make sure that our
#Haversine-based query actually returns all of the points that
#are inside the circular bound desired
class BoundsCatalog(InstanceCatalog):
    catalog_type = 'bounds_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000']
    transformations = {'raJ2000':numpy.degrees,
                       'decJ2000':numpy.degrees}
    
    default_formats = {'f':'%.20f'}

class BasicCatalog(InstanceCatalog):
    catalog_type = 'basic_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000', 'umag', 'gmag', 'rmag', 'imag',
                       'zmag', 'ymag']
    transformations = {'raJ2000':numpy.degrees,
                       'decJ2000':numpy.degrees}

class TestAstMixin(object):
    @compound('ra_corr', 'dec_corr')
    def get_points_corrected(self):
        #Fake astrometric correction
        ra_corr = self.column_by_name('raJ2000')+0.001
        dec_corr = self.column_by_name('decJ2000')+0.001
        return ra_corr, dec_corr

class CustomCatalog(BasicCatalog, TestAstMixin):
    catalog_type = 'custom_catalog'
    refIdCol = 'id'
    column_outputs = BasicCatalog.column_outputs+['points_corrected']
    transformations = BasicCatalog.transformations
    transformations['ra_corr'] = numpy.degrees
    transformations['dec_corr'] = numpy.degrees

def compareFiles(file1, file2):
    with open(file1) as fh:
        str1 = "".join(fh.readlines())
    with open(file2) as fh:
        str2 = "".join(fh.readlines())
    return str1 == str2

class InstanceCatalogTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists('icStarTestDatabase.db'):
            os.unlink('icStarTestDatabase.db')
        if os.path.exists('icGalTestDatabase.db'):
            os.unlink('icGalTestDatabase.db')
        tu.makeStarTestDB(filename='icStarTestDatabase.db', size=100000, seedVal=1)
        tu.makeGalTestDB(filename='icGalTestDatabase.db', size=100000, seedVal=1)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('icStarTestDatabase.db'):
            os.unlink('icStarTestDatabase.db')
        if os.path.exists('icGalTestDatabase.db'):
            os.unlink('icGalTestDatabase.db')     

    def setUp(self):
        self.obsMd = ObservationMetaData(boundType = 'circle', unrefractedRA = 210.0, unrefractedDec = -60.0,
                     boundLength=1.75, mjd=52000.,bandpassName='r')
                                               
        self.mystars = CatalogDBObject.from_objid('teststars', address='sqlite:///icStarTestDatabase.db')
        
        self.mygals = CatalogDBObject.from_objid('testgals', address='sqlite:///icGalTestDatabase.db')
        
        self.basedir = eups.productDir('sims_catalogs_measures')+"/tests/"

    def tearDown(self):
        del self.obsMd
        del self.mystars
        del self.mygals
        del self.basedir

    def testStarLike(self):
    
        t = self.mystars.getCatalog('custom_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_CUSTOM.out')
     
        self.assertTrue(compareFiles('test_CUSTOM.out', self.basedir+'testdata/CUSTOM_STAR.out'))
        os.unlink('test_CUSTOM.out')
        
        t = self.mystars.getCatalog('basic_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_BASIC.out')

        self.assertTrue(compareFiles('test_BASIC.out', self.basedir+'testdata/BASIC_STAR.out'))
        os.unlink('test_BASIC.out')

    def testGalLike(self):
    
        t = self.mygals.getCatalog('custom_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_CUSTOM.out')
 
        self.assertTrue(compareFiles('test_CUSTOM.out', self.basedir+'testdata/CUSTOM_GAL.out'))
        os.unlink('test_CUSTOM.out')
        
        t = self.mygals.getCatalog('basic_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_BASIC.out')
     
        self.assertTrue(compareFiles('test_BASIC.out', self.basedir+'testdata/BASIC_GAL.out'))
        os.unlink('test_BASIC.out')

def controlHaversine(ra1deg,dec1deg,ra2deg,dec2deg):
    #for use testing circular bounds
    raw = haversine(numpy.radians(ra1deg), numpy.radians(dec1deg),
                    numpy.radians(ra2deg), numpy.radians(dec2deg))
        
    return numpy.degrees(raw)

class boundingBoxTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists('bboxStarTestDatabase.db'):
            os.unlink('bboxStarTestDatabase.db')
        tu.makeStarTestDB(filename='bboxStarTestDatabase.db', size=100000, seedVal=1)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('bboxStarTestDatabase.db'):
            os.unlink('bboxStarTestDatabase.db')
    
    def setUp(self):
        
        self.RAmin = 190.
        self.RAmax = 210.
        self.DECmin = -70.
        self.DECmax = -50.
        
        self.RAcenter = 200.
        self.DECcenter = -60.
        self.radius = 40.0
        
        
        self.obsMdCirc = ObservationMetaData(boundType='circle',unrefractedRA=self.RAcenter,unrefractedDec=self.DECcenter,
                         boundLength=self.radius,mjd=52000., bandpassName='r')
                         
        self.obsMdBox = ObservationMetaData(boundType='box', unrefractedRA=0.5*(self.RAmax+self.RAmin),
                        unrefractedDec=0.5*(self.DECmin+self.DECmax),
                        boundLength=numpy.array([0.5*(self.RAmax-self.RAmin),0.5*(self.DECmax-self.DECmin)]),
                        mjd=52000., bandpassName='r')
                                            
        self.mystars = CatalogDBObject.from_objid('teststars', address='sqlite:///bboxStarTestDatabase.db')

    def tearDown(self):
        del self.obsMdCirc
        del self.obsMdBox
        del self.mystars
        del self.RAmin
        del self.DECmin
        del self.RAmax
        del self.DECmax
        del self.RAcenter
        del self.DECcenter
        del self.radius
    

    def testBoxBounds(self):
        """
        Make sure that box_bound_constraint in sims.catalogs.generation.db.dbConnection.py
        does not admit any objects outside of the bounding box
        """
        myCatalog = self.mystars.getCatalog('bounds_catalog',obs_metadata = self.obsMdBox)

        myIterator = myCatalog.iter_catalog(chunk_size=10)
        
        for line in myIterator:
            self.assertTrue(line[1]>self.RAmin)
            self.assertTrue(line[1]<self.RAmax)
            self.assertTrue(line[2]>self.DECmin)
            self.assertTrue(line[2]<self.DECmax)
       
        myCatalog.write_catalog('box_test_catalog.txt')
        
        #now we will test for the completeness of the box bounds 
        obsMdControl = ObservationMetaData(boundType = 'box',
                       unrefractedRA = self.RAcenter,unrefractedDec=self.DECcenter,
                       boundLength=20.0,mjd=52000.0, bandpassName = 'r')
      
        controlCatalog = self.mystars.getCatalog('bounds_catalog',obs_metadata = obsMdControl)
        controlCatalog.write_catalog('box_control_catalog.txt')
        
        ftest = open('box_test_catalog.txt')
        idtest = []
        for longline in ftest:
            line = longline.split()    
            
            #note that catalogs are output with commas at the end of values  
            idtest.append(line[0][:-1])
        ftest.close()
        
        fcontrol = open('box_control_catalog.txt')
        for longline in fcontrol:
            line = longline.split()
            if line[0] != '#id,':
                #note that catalogs are output with commas at the end of values
                idcontrol = line[0][:-1]
                racontrol = float(line[1][:-1])
                deccontrol = float(line[2][:-1])
                
                if racontrol < self.RAmax and racontrol > self.RAmin \
                and deccontrol < self.DECmax and deccontrol > self.DECmin:
                    
                    self.assertTrue(idcontrol in idtest)
        
        fcontrol.close()
        os.unlink('box_control_catalog.txt')
        os.unlink('box_test_catalog.txt')
        

    def testCircBounds(self):
        
        """
        Make sure that circular_bound_constraint in sims.catalogs.generation.db.dbConnection.py
        does not admit any objects outside of the bounding circle
        """

        myCatalog = self.mystars.getCatalog('bounds_catalog',obs_metadata = self.obsMdCirc)
        myIterator = myCatalog.iter_catalog(chunk_size=10)
    
        for line in myIterator:
            rtest = controlHaversine(self.RAcenter, self.DECcenter, line[1], line[2])
            self.assertTrue(rtest<self.radius)
       
        myCatalog.write_catalog('circular_test_catalog.txt')
       
        #now we will test for the completeness of the circular bounds 
        obsMdControl = ObservationMetaData(boundType='box',unrefractedRA=self.RAcenter,unrefractedDec=self.DECcenter,
                       boundLength=70.0,mjd=52000.0, bandpassName = 'r')
      
        controlCatalog = self.mystars.getCatalog('bounds_catalog',obs_metadata = obsMdControl)
        controlCatalog.write_catalog('circular_control_catalog.txt')
       
        ftest = open('circular_test_catalog.txt')
        idtest = []
        for longline in ftest:
            line = longline.split()
            
            #note that the catalogs are output with commas at the end of values

            idtest.append(line[0][:-1])
        ftest.close()

       
        fcontrol = open('circular_control_catalog.txt')
        for longline in fcontrol:
            line=longline.split()
            if line[0]!='#id,':
                
                #note that the catalogs are output with commas at the end of values
                idcontrol = line[0][:-1]
                racontrol = float(line[1][:-1])
                deccontrol = float(line[2][:-1])
                rr = controlHaversine(self.RAcenter, self.DECcenter, racontrol, deccontrol)
                
                if rr < self.radius:
                    self.assertTrue(idcontrol in idtest)
               
        fcontrol.close()
        os.unlink('circular_control_catalog.txt')
        os.unlink('circular_test_catalog.txt')

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(InstanceCatalogTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    suites += unittest.makeSuite(boundingBoxTest)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
