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
        conn=self.mystars.engine.raw_connection()
        conn.create_function("COS",1,numpy.cos)
        conn.create_function("SIN",1,numpy.sin)
        conn.create_function("ASIN",1,numpy.arcsin)
        conn.create_function("SQRT",1,numpy.sqrt)
        conn.close()
    
    
        t = self.mystars.getCatalog('custom_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_CUSTOM.out')
        self.assertTrue(compareFiles('test_CUSTOM.out', self.basedir+'testdata/CUSTOM_STAR.out'))
        os.unlink('test_CUSTOM.out')
        t = self.mystars.getCatalog('basic_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_BASIC.out')
        self.assertTrue(compareFiles('test_BASIC.out', self.basedir+'testdata/BASIC_STAR.out'))
        os.unlink('test_BASIC.out')

    def testGalLike(self):
    
        gconn=self.mygals.engine.raw_connection()
        gconn.create_function("COS",1,numpy.cos)
        gconn.create_function("SIN",1,numpy.sin)
        gconn.create_function("ASIN",1,numpy.arcsin)
        gconn.create_function("SQRT",1,numpy.sqrt)
        gconn.close()
    
    
        t = self.mygals.getCatalog('custom_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_CUSTOM.out')
        self.assertTrue(compareFiles('test_CUSTOM.out', self.basedir+'testdata/CUSTOM_GAL.out'))
        os.unlink('test_CUSTOM.out')
        t = self.mygals.getCatalog('basic_catalog', obs_metadata=self.obsMd)
        t.write_catalog('test_BASIC.out')
        self.assertTrue(compareFiles('test_BASIC.out', self.basedir+'testdata/BASIC_GAL.out'))
        os.unlink('test_BASIC.out')

class boundingBoxTest(unittest.TestCase):
    def setUp(self):
        
        self.RAmin = 200.
        self.RAmax = 215.
        self.DECmin = -70.
        self.DECmax = -50.
        
        self.RAcenter = 200.
        self.DECcenter = -60.
        self.radius = 1.75
        
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
        
        myCatalog = self.mystars.getCatalog('basic_catalog',obs_metadata = self.obsMdCirc)

        myIterator = myCatalog.iter_catalog(chunk_size=10)
        
        for line in myIterator:
            print line
        
        myCatalog.write_catalog("myCatalog_junk.sav")
    

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    #suites += unittest.makeSuite(InstanceCatalogTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    suites += unittest.makeSuite(boundingBoxTest)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
