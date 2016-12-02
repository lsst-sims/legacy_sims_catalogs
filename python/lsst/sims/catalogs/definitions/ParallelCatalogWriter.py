import copy


__all__ = ["parallelCatalogWriter"]


def parallelCatalogWriter(catalog_dict, chunk_size=None, constraint=None,
                          write_mode='w', write_header=True):
    """
    This method will take several InstanceCatalog classes that are meant
    to be based on the same CatalogDBObject and write them out in parallel
    from a single database query.  The imagined use-case is simultaneously
    writing out a PhoSim InstanceCatalog as well as the truth catalog with
    the pre-calculated positions and magnitudes of the sources.

    Parameters
    ----------
    catalog_dict is a dict keyed on the names of the files to be written.
    The values are the IntanceCatalogs to be written (note: these are full
    instantiations of InstanceCatalogs, not just InstanceCatalog classes
    as with the CompoundInstanceCatalog)

    constraint is an optional SQL constraint to be applied to the database query.
    Note: constraints applied to individual catalogs will be ignored.

    chunk_size is an int which optionally specifies the number of rows to be
    returned from db_obj at a time

    write_mode is either 'w' (write) or 'a' (append), determining whether or
    not the writer will overwrite existing catalog files (assuming they exist)

    write_header is a boolean that controls whether or not to write the header
    in the catalogs.

    Output
    ------
    This method does not return anything, it just writes the files that are the
    keys of catalog_dict
    """

    list_of_file_names = catalog_dict.keys()
    ref_cat = catalog_dict[list_of_file_names[0]]
    for ix, file_name in enumerate(list_of_file_names):
        if ix>0:
            cat = catalog_dict[file_name]
            try:
                assert cat.obs_metadata == ref_cat.obs_metadata
            except:
                print cat.obs_metadata
                print ref_cat.obs_metadata
                raise RuntimeError('Catalogs passed to parallelCatalogWriter have different '
                                   'ObservationMetaData.  I do not know how to deal with that.')

            try:
                assert cat.db_obj.connection.database is ref_cat.db_obj.connection.database
                assert cat.db_obj.connection.host is ref_cat.db_obj.connection.host
                assert cat.db_obj.connection.port is ref_cat.db_obj.connection.port
                assert cat.db_obj.connection.driver is ref_cat.db_obj.connection.driver
                assert cat.db_obj.tableid is ref_cat.db_obj.tableid
                assert cat.db_obj.objid is ref_cat.db_obj.objid
            except:
                msg = ('Cannot build these catalogs in parallel. '
                       'The two databases are different.  Connection info is:\n'
                       'database: %s != %s\n' % (cat.db_obj.connection.database, ref_cat.db_obj.database)
                       + 'host: %s != %s\n' % (cat.db_obj.connection.host, ref_cat.db_obj.connection.host)
                       + 'port: %s != %s\n' % (cat.db_obj.connection.port, ref_cat.db_obj.connection.port)
                       + 'driver: %s != %s\n' % (cat.db_obj.connection.driver, ref_cat.db_obj.connection.driver)
                       + 'table: %s != %s\n' % (cat.db_obj.tableid, ref_cat.db_obj.tableid))

                raise RuntimeError(msg)

    for file_name in list_of_file_names:
        cat = catalog_dict[file_name]
        cat._write_pre_process()

    active_columns = None
    for file_name in catalog_dict:
        cat = catalog_dict[file_name]
        if active_columns is None:
            active_columns = copy.deepcopy(cat._active_columns)
        else:
            for col_name in cat._active_columns:
                if col_name not in active_columns:
                    active_columns.append(col_name)

    query_result = ref_cat.db_obj.query_columns(colnames=active_columns,
                                                obs_metadata=ref_cat.obs_metadata,
                                                constraint=constraint,
                                                chunk_size=chunk_size)

    file_handle_dict = {}
    for file_name in catalog_dict:
        file_handle = open(file_name, write_mode)
        file_handle_dict[file_name] = file_handle

        if write_header:
            catalog_dict[file_name].write_header(file_handle)

    for master_chunk in query_result:
        chunk = master_chunk

        for i_file, file_name in enumerate(list_of_file_names):
            cat = catalog_dict[file_name]
            good_dexes = cat._filter_chunk(chunk)

            if len(good_dexes) < len(chunk):
                for i_old in range(i_file):
                    old_cat = catalog_dict[list_of_file_names[i_old]]
                    old_cat._update_current_chunk(good_dexes)

                chunk = chunk[good_dexes]

        for file_name in catalog_dict:
            catalog_dict[file_name]._write_current_chunk(file_handle_dict[file_name])

    for file_name in file_handle_dict:
        file_handle_dict[file_name].close()
