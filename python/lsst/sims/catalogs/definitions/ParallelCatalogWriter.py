from __future__ import print_function
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
    The values are the InstanceCatalogs to be written.  These are full
    instantiations of InstanceCatalogs, not just InstanceCatalog classes
    as with the CompoundInstanceCatalog.  They cannot be CompoundInstanceCatalogs

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

    list_of_file_names = list(catalog_dict.keys())
    ref_cat = catalog_dict[list_of_file_names[0]]
    for ix, file_name in enumerate(list_of_file_names):
        if ix>0:
            cat = catalog_dict[file_name]
            try:
                assert cat.obs_metadata == ref_cat.obs_metadata
            except:
                print(cat.obs_metadata)
                print(ref_cat.obs_metadata)
                raise RuntimeError('Catalogs passed to parallelCatalogWriter have different '
                                   'ObservationMetaData.  I do not know how to deal with that.')

            try:
                assert cat.db_obj.connection == ref_cat.db_obj.connection
            except:
                msg = ('Cannot build these catalogs in parallel. '
                       'The two databases are different.  Connection info is:\n'
                       'database: %s vs. %s\n' % (cat.db_obj.connection.database, ref_cat.db_obj.database)
                       + 'host: %s vs. %s\n' % (cat.db_obj.connection.host, ref_cat.db_obj.connection.host)
                       + 'port: %s vs. %s\n' % (cat.db_obj.connection.port, ref_cat.db_obj.connection.port)
                       + 'driver: %s vs. %s\n' % (cat.db_obj.connection.driver, ref_cat.db_obj.connection.driver)
                       + 'table: %s vs. %s\n' % (cat.db_obj.tableid, ref_cat.db_obj.tableid)
                       + 'objid: %s vs. %s\n' % (cat.db_obj.objid, ref_cat.db_obj.objid))

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
    local_write_mode = write_mode
    if write_header:
        for file_name in catalog_dict:
            with open(file_name, local_write_mode) as file_handle:
                catalog_dict[file_name].write_header(file_handle)
        local_write_mode = 'a'

    for master_chunk in query_result:

        for i_file, file_name in enumerate(list_of_file_names):
            chunk = master_chunk
            cat = catalog_dict[file_name]
            good_dexes = cat._filter_chunk(chunk)
            if len(good_dexes) < len(chunk):
                chunk = chunk[good_dexes]

            with open(file_name, local_write_mode) as file_handle:
                catalog_dict[file_name]._write_current_chunk(file_handle)

        local_write_mode = 'a'
