import numpy

import sqlite3
from sqlite3 import dbapi2 as sqlite

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.photometry.EBV import EBVmixin
from lsst.sims.catalogs.measures.photometry.Photometry import PhotometryStars, PhotometryGalaxies
from lsst.sims.catalogs.measures.photometry.Variability import Variability
from lsst.sims.catalogs.measures.astrometry.Astrometry import Astrometry
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.instance import InstanceCatalog
import lsst.sims.catalogs.generation.utils.testUtils as tu

from lsst.sims.catalogs.measures.example_utils.exampleCatalogDefinitions import TrimCatalogPoint

#from lsst.sims.catalogs.measures.example_utils.exampleCatalogDefinitions import GalaxyPhotometry


class sfdTestStars(InstanceCatalog,Astrometry,EBVmixin,Variability,PhotometryStars):
    catalog_type = 'sfd_test'
    column_outputs=['id','ra_corr','dec_corr','magNorm','lsst_u','lsst_g','lsst_r','lsst_i','lsst_z','lsst_y','EBV']

class sfdTestGalaxies(InstanceCatalog,Astrometry,EBVmixin,Variability,PhotometryGalaxies):
    catalog_type = 'sfd_test_galaxies'
    column_outputs=['galid','ra_corr','dec_corr','uRecalc', 'gRecalc', 'rRecalc', 'iRecalc', 'zRecalc', 'yRecalc',\
        'sedFilenameBulge','uBulge', 'gBulge', 'rBulge', 'iBulge', 'zBulge', 'yBulge',\
        'sedFilenameDisk','uDisk', 'gDisk', 'rDisk', 'iDisk', 'zDisk', 'yDisk',\
        'sedFilenameAgn','uAgn', 'gAgn', 'rAgn', 'iAgn', 'zAgn', 'yAgn']


sfd_db=DBObject.from_objid('rrly')
print 'key is ',sfd_db.getIdColKey()

obs_metadata_pointed=ObservationMetaData(circ_bounds=dict(ra=200., dec=-30, radius=1.))
sfd_cat=sfdTestStars(sfd_db,obs_metadata=obs_metadata_pointed)

#sfd_cat=GalaxyPhotometry(sfd_db)

print "and now to write"

sfd_cat.write_catalog("sfd_stellar_output.sav")


sfd_gal=DBObject.from_objid('galaxyBase')
obs_metadata_pointed = ObservationMetaData(circ_bounds=dict(ra=0., dec=0, radius=0.01))

gal_cat=sfdTestGalaxies(sfd_gal,obs_metadata=obs_metadata_pointed)
gal_cat.write_catalog("sfd_galaxy_output.sav")

obsMD = DBObject.from_objid('opsim3_61')
obs_metadata = obsMD.getObservationMetaData(88544919, 1.0, makeCircBounds = True)
sfd_trim = TrimCatalogPoint(sfd_db,obs_metadata=obs_metadata)
sfd_trim.write_catalog("sfd_trim_output.sav")

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
