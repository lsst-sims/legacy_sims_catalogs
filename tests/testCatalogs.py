from __future__ import with_statement
import os
import numpy
import unittest
import eups
import sqlalchemy
import lsst.utils.tests as utilsTests
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
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
    def setUp(self):
        
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
        tu.makeStarTestDB(size=100000, seedVal=1)
        tu.makeGalTestDB(size=100000, seedVal=1)
                
        self.obsMd = ObservationMetaData(circ_bounds=dict(ra=210., dec=-60, radius=1.75), mjd=52000.,
                                               bandpassName='r')
                                               
        self.mystars = DBObject.from_objid('teststars')
        
        self.mygals = DBObject.from_objid('testgals')
        
        self.basedir = eups.productDir('sims_catalogs_measures')+"/tests/"

    def tearDown(self):
        del self.obsMd
        del self.mystars
        del self.mygals

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
    """
    Evaluate the Haversine formula.  For use testing circular bounds
          
    http://en.wikipedia.org/wiki/Haversine_formula
    """
        
    conversion = numpy.pi/180.0
    ra1 = ra1deg*conversion
    dec1 = dec1deg*conversion
    ra2 = ra2deg*conversion
    dec2 = dec2deg*conversion
        
    arg = (numpy.sin(0.5*(dec1-dec2)))**2+numpy.cos(dec1)*numpy.cos(dec2)*(numpy.sin(0.5*(ra1-ra2)))**2
    rr = 2.0*numpy.arcsin(numpy.sqrt(arg))
        
    return rr/conversion

class boundingBoxTest(unittest.TestCase):
    def setUp(self):
        
        self.RAmin = 190.
        self.RAmax = 210.
        self.DECmin = -70.
        self.DECmax = -50.
        
        self.RAcenter = 200.
        self.DECcenter = -60.
        self.radius = 40.0
        
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
        tu.makeStarTestDB(size=100000, seedVal=1)
        self.obsMdCirc = ObservationMetaData(
                         circ_bounds=dict(ra=self.RAcenter, dec=self.DECcenter, radius=self.radius), 
                         mjd=52000., bandpassName='r')
                         
        self.obsMdBox = ObservationMetaData(box_bounds=dict(ra_min=self.RAmin,ra_max=self.RAmax,
                                            dec_min=self.DECmin,dec_max=self.DECmax),
                                            mjd=52000., bandpassName='r')
                                            
        self.mystars = DBObject.from_objid('teststars')

        
        #self.basedir = eups.productDir('sims_catalogs_measures')+"/tests/"

    def tearDown(self):
        del self.obsMdCirc
        del self.obsMdBox
        del self.mystars
    

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
        obsMdControl = ObservationMetaData(box_bounds=dict(ra_min = self.RAcenter-20.0, 
                                                ra_max = self.RAcenter+20.0,
                                                dec_min = self.DECcenter-20.0, 
                                                dec_max = self.DECcenter+20.0),
                                                mjd=52000.0, bandpassName = 'r')
      
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
        obsMdControl = ObservationMetaData(box_bounds=dict(ra_min = self.RAcenter-70.0, 
                                                ra_max = self.RAcenter+70.0,
                                                dec_min = self.DECcenter-70.0, 
                                                dec_max = self.DECcenter+70.0),
                                                mjd=52000.0, bandpassName = 'r')
      
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
