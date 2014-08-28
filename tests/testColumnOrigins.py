import os
import numpy
import unittest
from lsst.sims.catalogs.measures.instance import InstanceCatalog
from lsst.sims.catalogs.generation.db import DBObject

def makeTestDB(size=10, **kwargs):
    """
    Make a test database to serve information to the mflarTest object
    """
    conn = sqlite3.connect('testDatabase.db')
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE testTable
                     (id int, glon float, glat float, ra float, decl float)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")
    
    for i in xrange(size):
        
        ra = numpy.random.sample()*360.0
        dec = (numpy.random.sample()-0.5)*180.0
        
        #insert the row into the data base
        qstr = '''INSERT INTO testTable VALUES (%i, '%f', '%f', '%f', '%f')''' % (i, 0.0,0.0,ra,dec)
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
               ('glon', None),
               ('glat', None)]

class testCatalog(InstanceCatalog,EBVmixin,AstrometryBase):
    column_outputs=['objid','glon','glat','EBV','raJ2000','decJ2000']

if os.path.exists('testDatabase.db'):
    os.unlink('testDatabase.db')

makeTestDB()
myDBobject = testDBobject()
myCatalog = testCatalog(myDBobject)
myCatalog.print_column_origins()
print myCatalog._column_origins
myCatalog.write_catalog('catOutput.sav')
