import scipy
from lsst.sims.catalogs.generation.db.rewrite import\
        DBObject, ObservationMetaData
from lsst.sims.catalogs.measures.instance.rewrite import\
        InstanceCatalog

if __name__ == '__main__':
    obsMD = DBObject.from_objid('opsim3_61')
    obs_metadata = obsMD.getObservationMetaData(88544919, 0.1, makeCircBounds=True)
    obs_metadata_gal = ObservationMetaData(circ_bounds=dict(ra=.0,
                                                        dec=.0,
                                                        radius=0.1))
    #objectNames = ['galaxyBase', 'galaxyTiled', 'galaxyBulge', 'galaxyDisk']
    #filetype = ['test_catalog', 'test_catalog', 'trim_catalog_SERSIC2D',
    #            'trim_catalog_SERSIC2D']
    #constraints = ["r_ab < 22", "r_ab < 20.", "mass_bulge > 1.", 
    #               "DiskLSSTg < 20."]
    #constraints = [None, None, None, None]
    #metadataList = [obs_metadata_gal, obs_metadata, 
    #                obs_metadata, obs_metadata]
    objectNames = ['msstars','galaxyBulge', 'galaxyDisk', 'galaxyAgn']
    filetype = ['trim_catalog_POINT', 'trim_catalog_SERSIC2D', 'trim_catalog_SERSIC2D', 'trim_catalog_ZPOINT']
    constraints = [None,None,None,None]
    metadataList = [obs_metadata,obs_metadata,obs_metadata,obs_metadata]

    for objectName, constraint, md, ftype in zip(objectNames, constraints, metadataList, filetype):
        dbobj = DBObject.from_objid(objectName)
        t = InstanceCatalog.new_catalog(ftype, dbobj,
                        obs_metadata=md, constraint=constraint)


        print
        print "These are the required columns from the database:"
        print t.db_required_columns()
        print
        print "These are the columns that will be output to the file:"
        print t.column_outputs
        print
    
        filename = 'catalog_test_%s.dat'%(objectName)
        print "querying and writing catalog to %s:" % filename
        t.write_catalog(filename)
        filename = 'catalog_test_%s_chunked.dat'%(objectName)
        t.write_catalog(filename, chunk_size=100000)
        print " - finished"
