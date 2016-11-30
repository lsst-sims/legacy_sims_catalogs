import copy


__all__ = ["parallelCatalogWriter"]


def parallelCatalogWriter(catalog_class_dict, db_obj,
                          obs_metadata=None, constraint=None,
                          cannot_be_null=None, chunk_size=None,
                          write_mode='w'):
    """
    This method will take several InstanceCatalog classes that are meant
    to be based on the same CatalogDBObject and write them out in parallel
    from a single database query.  The imagined use-case is simultaneously
    writing out a PhoSim InstanceCatalog as well as the truth catalog with
    the pre-calculated positions and magnitudes of the sources.

    Parameters
    ----------
    catalog_class_dict is a dict keyed on the names of the files to be written.
    The values are the IntanceCatalog classes to be written (note: the classes,
    not instantiations of those classes; this method will instantiate them
    for you)

    db_obj is the CatalogDBObject on which to base the catalogs (the actual
    instantiation; not just the class)

    obs_metadata is an ObservationMetaData describing the field of view

    constraint is an optional SQL constraint to be applied to the database query

    cannot_be_null as an optional list of catalog columns that cannot be null
    in any of the catalogs (note: catalogs will only contain rows which pass this
    test for all catalogs).

    chunk_size is an int which optionally specifies the number of rows to be
    returned from db_obj at a time

    write_mode is either 'w' (write) or 'a' (append), determining whether or
    not the writer will overwrite existing catalog files (assuming they exist)

    Output
    ------
    This method does not return anything, it just writes the files that are the
    keys of catalog_class_dict
    """

    catalog_dict = {}
    for file_name in catalog_class_dict:
        cat_class = catalog_class_dict[file_name]
        cat = cat_class(db_obj, obs_metadata=obs_metadata,
                        constraint=constraint,
                        cannot_be_null=cannot_be_null)

        cat._write_pre_process()
        catalog_dict[file_name] = cat

    active_columns = None
    for file_name in catalog_dict:
        cat = catalog_dict[file_name]
        if active_columns is None:
            active_columns = copy.deepcopy(cat._active_columns)
        else:
            for col_name in cat._active_columns:
                if col_name not in active_columns:
                    active_columns.append(col_name)

    query_result = db_obj.query_columns(colnames=active_columns,
                                        obs_metadata=obs_metadata,
                                        constraint=constraint,
                                        chunk_size=chunk_size)

    file_handle_dict = {}
    for file_name in catalog_class_dict:
        file_handle = open(file_name, write_mode)
        file_handle_dict[file_name] = file_handle
        catalog_dict[file_name].write_header(file_handle)

    list_of_file_names = catalog_dict.keys()

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

        for file_name in catalog_class_dict:
            catalog_dict[file_name]._write_current_chunk(file_handle_dict[file_name])

    for file_name in file_handle_dict:
        file_handle_dict[file_name].close()
