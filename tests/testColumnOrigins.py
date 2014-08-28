import os
import numpy
import unittest
import sqlite3, json
import lsst.utils.tests as utilsTests
from lsst.sims.catalogs.measures.instance import InstanceCatalog, cached, compound
from lsst.sims.catalogs.generation.db import DBObject

def makeTestDB(size=10, **kwargs):
    """
    Make a test database to serve information to the mflarTest object
    """
    conn = sqlite3.connect('testDatabase.db')
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE testTable
                     (id int, aa float, bb float, ra float, decl float)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")
    
    for i in xrange(size):
        
        ra = numpy.random.sample()*360.0
        dec = (numpy.random.sample()-0.5)*180.0
        
        #insert the row into the data base
        qstr = '''INSERT INTO testTable VALUES (%i, '%f', '%f', '%f', '%f')''' % (i, 2.0*i,3.0*i,ra,dec)
        c.execute(qstr)
        
    conn.commit()
    conn.close()

class testDBobject(DBObject):
    objid = 'testDBobject'
    tableid = 'testTable'
    idColKey = 'id'
    #Make this implausibly large?  
    appendint = 1023
    dbAddress = 'sqlite:///testDatabase.db'
    raColName = 'ra'
    decColName = 'decl'
    columns = [('objid', 'id', int),
               ('raJ2000', 'ra*%f'%(numpy.pi/180.)),
               ('decJ2000', 'decl*%f'%(numpy.pi/180.)),
               ('aa', None),
               ('bb', None)]

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

class testColumnOrigins(unittest.TestCase):

    def setUp(self):
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
        
        makeTestDB()
        self.myDBobject = testDBobject()
    
    def tearDown(self):
        if os.path.exists('testDatabase.db'):
            os.unlink('testDatabase.db')
        
        del self.myDBobject

    def testDefaults(self):
        myCatalog = testCatalogDefaults(self.myDBobject)
        
        self.assertEqual(myCatalog._column_origins['objid'],'the database')
        self.assertEqual(myCatalog._column_origins['raJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['decJ2000'],'the database')
        self.assertEqual(myCatalog._column_origins['aa'],'the database')
        self.assertEqual(myCatalog._column_origins['bb'],'the database')
        self.assertEqual(myCatalog._column_origins['cc'],'default column')
        self.assertEqual(myCatalog._column_origins['dd'],'default column')

    def testMixin1(self):
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


def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(testColumnOrigins)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
