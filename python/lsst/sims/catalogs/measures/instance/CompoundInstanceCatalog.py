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

        for row in self._dbObjectGroupList:
            if len(row)>1:
                dbObjList = [self._dbo_list[ix] for ix in row]
                catList = [self._ic_list[ix] for ix in row]

                if self._compoundDBclass is None:
                    compound_dbo = CompoundCatalogDBObject(dbObjList)
                else:
                    compound_dbo = self._compoundDBclass(dbObjList)

                self._write_compound(catList, compound_dbo, filename,
                                     chunk_size=chunk_size, write_header=write_header,
                                     write_mode=write_mode)
                write_mode = 'a'
                write_header = False


    def _write_compound(self, catList, compound_dbo, filename,
                        chunk_size=None, write_header=False, write_mode='a'):


        colnames = []
        master_colnames = []
        dbObjNameList = [db.objid for db in compound_dbo._dbObjectList]
        for name, cat in zip(dbObjNameList, catList):
            localNames = []
            for colName in cat._active_columns:
                colnames.append('%s_%s' % (name, colName))
                localNames.append('%s_%s' % (name, colName))
            master_colnames.append(localNames)


        master_results = compound_dbo.query_columns(colnames=colnames,
                                                    obs_metadata=self._obs_metadata,
                                                    constraint=self._constraint,
                                                    chunk_size=chunk_size)

        with open(filename, write_mode) as file_handle:
            if write_header:
                catList[0].write_header(file_handle)


            new_dtype_list = [None]*len(catList)

            for chunk in master_results:
                for ix, (name, cat) in enumerate(zip(dbObjNameList, catList)):
                    local_recarray = chunk[master_colnames[ix]].view(numpy.recarray)
                    if new_dtype_list[ix] is None:
                        new_dtype = numpy.dtype([
                                                tuple([dd.replace(name+'_','')] + [local_recarray.dtype[dd]]) \
                                                for dd in master_colnames[ix]
                                                ])
                        new_dtype_list[ix] = new_dtype

                    local_recarray.dtype = new_dtype_list[ix]

                    cat._write_recarray(local_recarray, file_handle)
