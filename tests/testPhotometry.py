import numpy

import sqlite3
from sqlite3 import dbapi2 as sqlite

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.instance import InstanceCatalog, compound, cached
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.coordUtils.Astrometry import Astrometry
from lsst.sims.photUtils.Photometry import PhotometryGalaxies, PhotometryStars
from lsst.sims.photUtils.EBV import EBVmixin
from lsst.sims.catalogs.measures.instance.Site import Site

from lsst.sims.photUtils.Variability import Variability

class testDefaults(object):

    def get_proper_motion_ra(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=0.0
        
        return out
  
    
    def get_proper_motion_dec(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=0.0
        
        return out
    
    def get_parallax(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=1.2
        
        return out
    
    def get_radial_velocity(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=0.0
        
        return out
    
    def get_epoch(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=2000.0
        
        return out
    
    def get_mjd(self):
        ra=self.column_by_name('raJ2000')
        out=numpy.zeros(len(ra))
        for i in range(len(ra)):
            out[i]=2015.0
        
        return out

class testCatalog(InstanceCatalog,Astrometry,Variability,testDefaults):
    catalog_type = 'MISC'
    default_columns=[('expmjd',5000.0,float)]
    def db_required_columns(self):
        return ['raJ2000'],['varParamStr']

        
class testStars(InstanceCatalog,Astrometry,EBVmixin,Variability,PhotometryStars,testDefaults):
    catalog_type = 'test_stars'
    column_outputs=['id','ra_corr','dec_corr','magNorm',\
    'lsst_u','lsst_u_var','lsst_g','lsst_g_var',\
    'lsst_r','lsst_r_var','lsst_i','lsst_i_var','lsst_z','lsst_z_var',
    'lsst_y','lsst_y_var',\
    'EBV','varParamStr']

"""
class testStars(InstanceCatalog,Astrometry,EBVmixin,Variability,PhotometryStars,testDefaults):
    catalog_type = 'test_stars'
    column_outputs=['id','ra_corr','dec_corr','magNorm',\
    'lsst_u','lsst_g','lsst_r','lsst_i','lsst_z','lsst_y',\
    'EBV','varParamStr']
"""
    
class testGalaxies(InstanceCatalog,Astrometry,EBVmixin,Variability,PhotometryGalaxies,testDefaults):
    catalog_type = 'test_galaxies'
    column_outputs=['galid','ra_corr','dec_corr','uRecalc', 'gRecalc', 'rRecalc', 'iRecalc', 'zRecalc', 'yRecalc',\
        'uRecalc_var','gRecalc_var','rRecalc_var','iRecalc_var','zRecalc_var','yRecalc_var',\
        'sedFilenameBulge','uBulge', 'gBulge', 'rBulge','iBulge', 'zBulge', 'yBulge', \
        'sedFilenameDisk','uDisk', 'gDisk', 'rDisk','iDisk', 'zDisk', 'yDisk',\
        'sedFilenameAgn','uAgn', 'uAgn_var', 'gAgn', 'gAgn_var', 'rAgn', 'rAgn_var', \
        'iAgn', 'iAgn_var', 'zAgn', 'zAgn_var', 'yAgn', 'yAgn_var', 'varParamStr']


class variabilityUnitTest(unittest.TestCase):

    galaxy = DBObject.from_objid('galaxyBase')
    rrly = DBObject.from_objid('rrly')
    obsMD = DBObject.from_objid('opsim3_61')
    obs_metadata = obsMD.getObservationMetaData(88544919, 0.1, makeCircBounds = True)
    
    def testGalaxyVariability(self):   
                
        galcat = testCatalog(self.galaxy,obs_metadata = self.obs_metadata)
        rows = self.galaxy.query_columns(['varParamStr'], constraint = 'VarParamStr is not NULL',chunk_size=20)
        rows = rows.next()
        print "len ",len(rows)
        for i in range(20):
            #print "i ",i
            mags=galcat.applyVariability(rows[i]['varParamStr'])
            #print mags

    def testRRlyVariability(self):
        rrlycat = testCatalog(self.rrly,obs_metadata = self.obs_metadata)
        rows = self.rrly.query_columns(['varParamStr'], constraint = 'VarParamStr is not NULL',chunk_size=20)
        rows = rows.next()
        for i in range(20):
            mags=rrlycat.applyVariability(rows[i]['varParamStr'])

class photometryUnitTest(unittest.TestCase):
       
    def testStars(self):
        dbObj=DBObject.from_objid('rrly')
        obs_metadata_pointed=ObservationMetaData(mjd=2013.23, circ_bounds=dict(ra=200., dec=-30, radius=1.))
        test_cat=testStars(dbObj,obs_metadata=obs_metadata_pointed)
        test_cat.write_catalog("testStarsOutput.txt")
    
    
    def testGalaxies(self):
        dbObj=DBObject.from_objid("galaxyBase")
        obs_metadata_pointed=ObservationMetaData(mjd=50000.0, circ_bounds=dict(ra=0., dec=0., radius=0.01))
        test_cat=testGalaxies(dbObj,obs_metadata=obs_metadata_pointed)
        test_cat.write_catalog("testGalaxiesOutput.txt")
        
        
def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(variabilityUnitTest)
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
