from __future__ import with_statement
import numpy
from lsst.sims.catalogs.generation.db import CompoundCatalogDBObject

class CompoundInstanceCatalog(object):

    def __init__(self, instanceCatalogList, obs_metadata=None, constraint=None,
                 compoundDBclass = None):

        self._compoundDBclass = compoundDBclass
        self._obs_metadata = obs_metadata
        self._dbo_list = []
        self._ic_list = instanceCatalogList
        self._constraint = constraint
        for ic in self._ic_list:
            self._dbo_list.append(ic.db_obj)

        assigned = [False]*len(self._dbo_list)
        self._dbObjectGroupList = []

        for ix in range(len(self._dbo_list)):
            for row in self._dbObjectGroupList:
                if self.areDBObjectsTheSame(self._dbo_list[ix], self._dbo_list[row[0]]):
                    row.append(ix)
                    assigned[ix] = True
                    break

            if not assigned[ix]:
                new_row = [ix]
                for iy in range(ix):
                    if not assigned[iy]:
                        if self.areDBObjectsTheSame(self._dbo_list[ix], self._dbo_list[iy]):
                            new_row.append(iy)

                self._dbObjectGroupList.append(new_row)


    def areDBObjectsTheSame(self, db1, db2):
        if db1.tableid != db2.tableid:
            return False
        if db1.host != db2.host:
            return False
        if db1.database != db2.database:
            return False
        if db1.port != db2.port:
            return False
        if db1.driver != db2.driver:
            return False
        return True


    def write_catalog(self, filename, chunk_size=None, write_header=True, write_mode='w'):

        for ic in self._ic_list:
            ic._write_pre_process()

        for row in self._dbObjectGroupList:
            if len(row)==1:
                self._ic_list[row[0]]._query_and_write(filename, chunk_size=chunk_size,
                                                       write_header=write_header, write_mode=write_mode,
                                                       obs_metadata=self._obs_metadata,
                                                       constraint=self._constraint)
                write_mode = 'a'
                write_header = False

        default_compound_dbo = None
        if self._compoundDBclass is not None:
            if not hasattr(self._compoundDBclass, '__getitem__'):
                default_compound_dbo = CompoundCatalogDBObject
            else:
                for dbo in self._compoundDBclass:
                    if dbo._table_restriction is None:
                        default_compound_dbo = dbo
                        break

                if default_compound_dbo is None:
                    default_compound_dbo is CompoundCatalogDBObject

        for row in self._dbObjectGroupList:
            if len(row)>1:
                dbObjList = [self._dbo_list[ix] for ix in row]
                catList = [self._ic_list[ix] for ix in row]

                if self._compoundDBclass is None:
                    compound_dbo = CompoundCatalogDBObject(dbObjList)
                elif not hasattr(self._compoundDBclass, '__getitem__'):
                    # if self._compoundDBclass is not a list
                    try:
                        compound_dbo = self._compoundDBclass(dbObjList)
                    except:
                        compound_dbo = default_compound_dbo(dbObjList)
                else:
                    compound_dbo = None
                    for candidate in self._compoundDBclass:
                        use_it = True
                        if False in [candidate._table_restriction is not None and dbo.tableid in candidate._table_restriction \
                                     for dbo in dbObjList]:

                            use_it = False

                        if use_it:
                            compound_dbo = candidate(dbObjList)
                            break

                    if compound_dbo is None:
                        compound_dbo = default_compound_dbo(dbObjList)


                self._write_compound(catList, compound_dbo, filename,
                                     chunk_size=chunk_size, write_header=write_header,
                                     write_mode=write_mode)
                write_mode = 'a'
                write_header = False


    def _write_compound(self, catList, compound_dbo, filename,
                        chunk_size=None, write_header=False, write_mode='a'):


        colnames = []
        master_colnames = []
        name_map = []
        dbObjNameList = [db.objid for db in compound_dbo._dbObjectList]
        for name, cat in zip(dbObjNameList, catList):
            localNames = []
            local_map = {}
            for colName in cat._active_columns:
                colnames.append('%s_%s' % (name, colName))
                localNames.append('%s_%s' % (name, colName))
                local_map['%s_%s' % (name, colName)] = colName
            master_colnames.append(localNames)
            name_map.append(local_map)


        master_results = compound_dbo.query_columns(colnames=colnames,
                                                    obs_metadata=self._obs_metadata,
                                                    constraint=self._constraint,
                                                    chunk_size=chunk_size)

        with open(filename, write_mode) as file_handle:
            if write_header:
                catList[0].write_header(file_handle)


            new_dtype_list = [None]*len(catList)

            first_chunk = True
            for chunk in master_results:
                for ix, (catName, cat) in enumerate(zip(dbObjNameList, catList)):

                    if first_chunk:
                        for iy, name in enumerate(master_colnames[ix]):
                            if name not in chunk.dtype.fields:
                                master_colnames[ix][iy] = name_map[ix][name]

                    local_recarray = chunk[master_colnames[ix]].view(numpy.recarray)
                    if new_dtype_list[ix] is None:
                        new_dtype = numpy.dtype([
                                                tuple([dd.replace(catName+'_','')] + [local_recarray.dtype[dd]]) \
                                                for dd in master_colnames[ix]
                                                ])
                        new_dtype_list[ix] = new_dtype

                    local_recarray.dtype = new_dtype_list[ix]
                    cat._write_recarray(local_recarray, file_handle)

                first_chunk = False
