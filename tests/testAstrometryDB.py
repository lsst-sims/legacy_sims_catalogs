import numpy

import sqlite3
from sqlite3 import dbapi2 as sqlite

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.photometry.photUtils import Photometry
from lsst.sims.catalogs.measures.astrometry.Astrometry import Astrometry
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.instance import InstanceCatalog
import lsst.sims.catalogs.generation.utils.testUtils as tu

class sfdTestDB(DBObject):
    objid = 'teststars'
    tableid = 'stars'
    idColKey = 'id'
    #Make this implausibly large?  
    appendint = 1023
    dbAddress = 'sqlite:///testDatabase.db'
    raColName = 'ra'
    decColName = 'decl'
    spatialModel = 'POINT'
    columns = [('id', None, int),
               ('raJ2000', 'ra*%f'%(numpy.pi/180.)),
               ('decJ2000', 'decl*%f'%(numpy.pi/180.)),
               ('umag', None),
               ('gmag', None),
               ('rmag', None),
               ('imag', None),
               ('zmag', None),
               ('ymag', None),
               ('mag_norm', None)]
    
               #('ra_corr',None),
               #('dec_corr',None)]

class sfdTestCatalog(InstanceCatalog,Astrometry):
    catalog_type = 'sfd_test'
    column_outputs=['id','raJ2000','decJ2000','ra_corr','dec_corr']

sfd_db=sfdTestDB()
print 'key is ',sfd_db.getIdColKey()

#result=sfd_db.query_columns()
#tu.writeResult(result,"sfd_db.sav")

sfd_cat=sfdTestCatalog(sfd_db)
sfd_cat.write_catalog("sfd_catalog_output.sav")
