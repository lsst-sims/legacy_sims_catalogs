import os
import numpy
import unittest
import sqlite3, json
import lsst.utils.tests as utilsTests
from lsst.sims.catalogs.measures.instance import InstanceCatalog, cached, compound
from lsst.sims.catalogs.generation.db import CatalogDBObject

def makeTestDB(size=10, **kwargs):
    """
    Make a test database
    """
    conn = sqlite3.connect('colOriginsTestDatabase.db')
    c = conn.cursor()
#    try:
    c.execute('''CREATE TABLE testTable
                 (id int, aa float, bb float, ra float, decl float)''')
    conn.commit()
#    except Exception:
#        raise RuntimeError("Error creating database.")
    
    for i in xrange(size):
        
        ra = numpy.random.sample()*360.0
        dec = (numpy.random.sample()-0.5)*180.0
        
        #insert the row into the data base
        qstr = '''INSERT INTO testTable VALUES (%i, '%f', '%f', '%f', '%f')''' % (i, 2.0*i,3.0*i,ra,dec)
        c.execute(qstr)
        
    conn.commit()
    conn.close()

class testDBobject(CatalogDBObject):
    objid = 'testDBobject'
    tableid = 'testTable'
    idColKey = 'id'
    #Make this implausibly large?  
    appendint = 1023
    dbAddress = 'sqlite:///colOriginsTestDatabase.db'
    raColName = 'ra'
    decColName = 'decl'
    columns = [('objid', 'id', int),
               ('raJ2000', 'ra*%f'%(numpy.pi/180.)),
               ('decJ2000', 'decl*%f'%(numpy.pi/180.)),
               ('aa', None),
               ('bb', None)]

#Below we define mixins which calculate the variables 'cc' and 'dd in different
#ways.  The idea is to see if InstanceCatalog correctly identifies where
#the columns come from in those cases
class mixin1(object):
    @cached
    def get_cc(self):
        aa = self.column_by_name('aa')
        bb = self.column_by_name('bb')
        
        return numpy.array(aa-bb)

    @cached
    def get_dd(self):
        aa = self.column_by_name('aa')
        bb = self.column_by_name('bb')
        
        return numpy.array(aa+bb)

class mixin2(object):
    @compound('cc','dd')
    def get_both(self):
        aa = self.column_by_name('aa')
        bb = self.column_by_name('bb')
        
        return numpy.array([aa-bb,aa+bb])

class mixin3(object):
    @cached
    def get_cc(self):
        aa = self.column_by_name('aa')
        bb = self.column_by_name('bb')
        
        return numpy.array(aa-bb)

#Below we define catalog classes that use different combinations
#of the mixins above to calculate the columns 'cc' and 'dd'    
class testCatalogDefaults(InstanceCatalog):
    column_outputs = ['objid','aa','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('cc',0.0,float),('dd',1.0,float)]

class testCatalogMixin1(InstanceCatalog,mixin1):
    column_outputs = ['objid','aa','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('cc',0.0,float),('dd',1.0,float)]

class testCatalogMixin2(InstanceCatalog,mixin2):
    column_outputs = ['objid','aa','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('cc',0.0,float),('dd',1.0,float)]

class testCatalogMixin3(InstanceCatalog,mixin3):
    column_outputs = ['objid','aa','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('cc',0.0,float),('dd',1.0,float)]
    
class testCatalogMixin3Mixin1(InstanceCatalog,mixin3,mixin1):
    column_outputs = ['objid','aa','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('cc',0.0,float),('dd',1.0,float)]

class testCatalogAunspecified(InstanceCatalog,mixin3,mixin1):
    column_outputs = ['objid','bb','cc','dd','raJ2000','decJ2000']
    default_columns = [('aa',-1.0,float),('cc',0.0,float),('dd',1.0,float)]

class testColumnOrigins(unittest.TestCase):

    def setUp(self):
        if os.path.exists('colOriginsTestDatabase.db'):
            os.unlink('colOriginsTestDatabase.db')
        
        makeTestDB()
        self.myDBobject = testDBobject()
    
    def tearDown(self):
        if os.path.exists('colOriginsTestDatabase.db'):
            os.unlink('colOriginsTestDatabase.db')
        
        del self.myDBobject

    def testDefaults(self):
        """
        Test case where the columns cc and dd come from defaults
        """
        myCatalog = testCatalogDefaults(self.myDBobject)
        
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(myCatalog._column_origins['cc'],'default column')
        self.assertEqual(myCatalog._column_origins['dd'],'default column')

    def testMixin1(self):
        """
        Test case where the columns cc and dd come from non-compound getters
        """
        myCatalog = testCatalogMixin1(self.myDBobject)
        mixin1Name = '<class \'__main__.mixin1\'>'
        
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(str(myCatalog._column_origins['cc']),mixin1Name)
        self.assertEqual(str(myCatalog._column_origins['dd']),mixin1Name)     

    def testMixin2(self):
        """
        Test case where the columns cc and dd come from a compound getter
        """
        myCatalog = testCatalogMixin2(self.myDBobject)
        mixin2Name = '<class \'__main__.mixin2\'>'
        
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(str(myCatalog._column_origins['cc']),mixin2Name)
        self.assertEqual(str(myCatalog._column_origins['dd']),mixin2Name) 
    
    def testMixin3(self):
        """
        Test case where cc comes from a mixin and dd comes from the default
        """
        myCatalog = testCatalogMixin3(self.myDBobject)
        mixin3Name = '<class \'__main__.mixin3\'>'
        
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(str(myCatalog._column_origins['cc']),mixin3Name)
        self.assertEqual(str(myCatalog._column_origins['dd']),'default column') 

    def testMixin3Mixin1(self):
        """
        Test case where one mixin overwrites another for calculating cc
        """
        myCatalog = testCatalogMixin3Mixin1(self.myDBobject)
        mixin3Name = '<class \'__main__.mixin3\'>'
        mixin1Name = '<class \'__main__.mixin1\'>'
   
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(str(myCatalog._column_origins['cc']),mixin3Name)
        self.assertEqual(str(myCatalog._column_origins['dd']),mixin1Name) 

    def testAunspecified(self):
        """
        Test case where aa is not specified in the catalog (and has a default)
        """
        myCatalog = testCatalogAunspecified(self.myDBobject)
        mixin3Name = '<class \'__main__.mixin3\'>'
        mixin1Name = '<class \'__main__.mixin1\'>'
   
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(str(myCatalog._column_origins['cc']),mixin3Name)
        self.assertEqual(str(myCatalog._column_origins['dd']),mixin1Name) 

def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(testColumnOrigins)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
