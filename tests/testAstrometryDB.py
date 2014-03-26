import numpy

import sqlite3
from sqlite3 import dbapi2 as sqlite

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.photometry.photUtils import Photometry
from lsst.sims.catalogs.measures.photometry.Variability import Variability
from lsst.sims.catalogs.measures.astrometry.Astrometry import Astrometry
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.instance import InstanceCatalog
import lsst.sims.catalogs.generation.utils.testUtils as tu

"""
class sfdStarClass(DBObject):
    objid = 'msstars'
    tableid = 'stars'
    idColKey = 'id'
    raColName = 'ra'
    decColName = 'decl'
    objectTypeId = 4
    spatialModel = 'POINT'
    dbAddress = 'sqlite:///testDatabase.db'
    dbDefaultValues = {'varsimobjid':-1, 'runid':-1, 'ismultiple':-1, 'run':-1,
                       'runobjid':-1}
    #These types should be matched to the database.
    #: Default map is float.  If the column mapping is the same as the column name, None can be specified
    columns = [('id',None, int),
               ('raJ2000', 'ra*%f'%(numpy.pi/180.)),
               ('decJ2000', 'decl*%f'%(numpy.pi/180.)),
               #('glon', 'gal_l*%f'%(numpy.pi/180.)),
               #('glat', 'gal_b*%f'%(numpy.pi/180.)),
               #('magNorm', '(-2.5*log(flux_scale)/log(10.)) - 18.402732642'),
               #('properMotionRa', '(mudecl/(1000.*3600.))*PI()/180.'),
               #('properMotionDec', '(mura/(1000.*3600.))*PI()/180.'),
               #('galacticAv', 'CONVERT(float, ebv*3.1)'),
               #('radialVelocity', 'vrad'),
               ('variabilityParameters', 'varParamStr', str, 256),
               ('sedFilename', 'sedfilename', unicode, 40)]

"""

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



class sfdTestCatalog(InstanceCatalog,Astrometry,Photometry,Variability):
    catalog_type = 'sfd_test'
    column_outputs=['id','raJ2000','decJ2000','ra_corr','dec_corr','EBV','lsst_u','lsst_g','lsst_r','lsst_i','lsst_z','lsst_y']

sfd_db=DBObject.from_objid('rrly')
print 'key is ',sfd_db.getIdColKey()

#results=sfd_db.query_columns(['sedFilename'])
#results=results.next()
#for i in range(10):
#    print results[i]['sedFilename']


obs_metadata_pointed=ObservationMetaData(circ_bounds=dict(ra=200., dec=-30, radius=1.))
sfd_cat=sfdTestCatalog(sfd_db,obs_metadata=obs_metadata_pointed)


print "and now to write"

sfd_cat.write_catalog("sfd_catalog_output.sav")

#query = sfd_db.query_columns(['raJ2000'])
#sfd_cat._set_current_chunk(query)
#col=sfd_cat.column_by_name('raJ2000')

#print col

"""
sfd_stars=sfdStarClass()

#sfd_stars=DBObject.from_objid('msstars')
result=sfd_stars.query_columns()
tu.writeResult(restul,"sfd_star_db.sav")
"""
