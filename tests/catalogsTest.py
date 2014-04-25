import os
import numpy
from lsst.sims.coordUtils import compound
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.instance import InstanceCatalog
from lsst.sims.catalogs.measures.example_utils.exampleCatalogDefinitions import\
        TrimCatalogPoint
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

class TestTrim(TrimCatalogPoint):
    catalog_type = 'test_trim_catalog'
    column_outputs = ['prefix', 'objectid','raTrim','decTrim','mag_norm','sedFilename',
                      'redshift','shear1','shear2','kappa','raOffset','decOffset',
                      'spatialmodel','galacticExtinctionModel','galacticAv','galacticRv',
                      'internalExtinctionModel']
    default_columns = TrimCatalogPoint.default_columns
    default_columns.append(('galacticAv', 0.1, float))
    default_columns.append(('sedFilename', 'flat.dat', (str,8)))

if __name__=="__main__":
    if not os.path.exists('testDatabase.db'):
        tu.makeTestDB(size=100000)
    mystars = tu.myTestStars()

    obsMD = DBObject.from_objid('opsim3_61')
    #Observation metadata from OpSim
    obs_metadata = obsMD.getObservationMetaData(88544919, 1.75, makeCircBounds=True)
    #Observation metadata from pointing
    obs_metadata_pointed = ObservationMetaData(circ_bounds=dict(ra=210., dec=-60, radius=1.75))
    
    t = mystars.getCatalog('custom_catalog', obs_metadata=obs_metadata)
    t.write_catalog('test_CUSTOM.out')
    t = mystars.getCatalog('custom_catalog', obs_metadata=obs_metadata_pointed)
    t.write_catalog('test_CUSTOM_POINTED.out')

    #We can't write a TRIM file with observation metadata constructed by hand
    #because it does not contain all the necessary bits to make the TRIM header
    t = mystars.getCatalog('test_trim_catalog', obs_metadata=obs_metadata)
    t.write_catalog('test_TRIM.out')
    t.write_catalog('test_TRIM_CHUNKED.out', chunk_size=2)
