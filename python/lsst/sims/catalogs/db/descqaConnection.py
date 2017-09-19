"""
This script will define classes that look like CatalogDBObject, but use
DESCQA's generic-catalog-reader to load an arbitrary catalog
"""

from collections import OrderedDict
import numpy as np
import gc
import numbers

_GCR_IS_AVAILABLE = True

try:
    from GCR import load_catalog
except ImportError:
    _GCR_IS_AVAILABLE = False
    pass


from lsst.sims.utils import _angularSeparation

__all__ = ["DESCQAObject"]


class DESCQAChunkIterator(object):

    def __init__(self, descqa_obj, column_map, obs_metadata, colnames, chunk_size):
        self._descqa_obj = descqa_obj
        self._catsim_colnames = colnames
        self._chunk_size = chunk_size
        self._data = None
        self._continue = True
        self._column_map = column_map
        self._obs_metadata = obs_metadata

    def __iter__(self):
        return self

    def __next__(self):
        if self._data is None and self._continue:
            gcr_col_names = np.array([self._column_map[catsim_name][0] for catsim_name in self._catsim_colnames])
            gcr_col_names = np.unique(gcr_col_names)

            gcr_cat_data = self._descqa_obj.get_quantities(gcr_col_names)
            catsim_data = {}
            for catsim_name in self._catsim_colnames:
                gcr_name = self._column_map[catsim_name][0]
                catsim_data[catsim_name] = gcr_cat_data[gcr_name]
                if len(self._column_map[catsim_name])>1:
                    catsim_data[catsim_name] = self._column_map[catsim_name][1](catsim_data[catsim_name])

            dtype = np.dtype([(catsim_name, gcr_cat_data[self._column_map[catsim_name][0]].dtype)
                              for catsim_name in self._catsim_colnames])

            del gcr_cat_data
            gc.collect()

            # if an ObservationMetaData has been specified, cull
            # the data to be within the field of view
            if self._obs_metadata is not None:
                if self._obs_metadata._boundLength is not None:
                    if not isinstance(self._obs_metadata._boundLength, numbers.Number):
                        radius_rad = max(self._obs_metadata._boundLength[0],
                                         self._obs_metadata._boundLength[1])
                    else:
                        radius_rad = self._obs_metadata._boundLength

                    valid = np.where(_angularSeparation(catsim_data['raJ2000'],
                                                        catsim_data['decJ2000'],
                                                        self._obs_metadata._pointingRA,
                                                        self._obs_metadata._pointingDec) < radius_rad)

                    for name in catsim_data:
                        catsim_data[name] = catsim_data[name][valid]

            records = []
            for i_rec in range(len(catsim_data[self._catsim_colnames[0]])):
                rec = (tuple([catsim_data[name][i_rec]
                              for name in self._catsim_colnames]))
                records.append(rec)

            if len(records) == 0:
                self._data = np.recarray(shape=(0,len(catsim_data)), dtype=dtype)
            else:
                self._data = np.rec.array(records, dtype=dtype)
            self._start_row = 0

        if self._chunk_size is None and self._continue and len(self._data)>0:
            output = self._data
            self._data = None
            self._continue = False
            return output
        elif self._continue:
            if self._start_row<len(self._data):
                old_start = self._start_row
                self._start_row += self._chunk_size
                return self._data[old_start:self._start_row]
            else:
                self._data = None
                self._continue = False
                raise StopIteration

        raise StopIteration



class DESCQAObject(object):

    _id_col_key = None
    _object_type_id = None
    verbose = False

    def __init__(self, yaml_file_name):
        """
        Parameters
        ----------
        yaml_file_name is the name of the yaml file that will tell DESCQA
        how to load the catalog
        """

        global _GCR_IS_AVAILABLE
        if not _GCR_IS_AVAILABLE:
            raise RuntimeError("You cannot use DESQAObject\n"
                               "You do not have the generic catalog read "
                               "installed")

        self._catalog = load_catalog(yaml_file_name)

        self.columnMap = None
        self._make_column_map()

    @property
    def idColKey(self):
        return self._id_col_key

    @idColKey.setter
    def idColKey(self, val):
        self._id_col_key = val

    @property
    def objectTypeId(self):
        return self._object_type_id

    @objectTypeId.setter
    def objectTypeId(self, val):
        self._object_type_id = val

    def getIdColKey(self):
        return self.idColKey

    def getObjectTypeId(self):
        return self.objectTypeId

    def _make_column_map(self):
        """
        Slightly different from the database case.
        self.columnMap will be a dict keyed on the CatSim column name.
        The values will be tuples.  The first element of the tuple is the
        GCR column name corresponding to that CatSim column.  The second
        element is an (optional) transformation applied to the GCR column
        used to get it into units expected by CatSim.
        """
        self.columnMap = OrderedDict([(name, (name,))
                                      for name in self._catalog.list_all_quantities()])

        for column_tuple in self.columns:
            if len(column_tuple)>1:
                self.columnMap[column_tuple[0]] = column_tuple[1:]

    def query_columns(self, colnames=None, chunk_size=None,
                      obs_metadata=None, constraint=None, limit=None):
        """
        Parameters
        ----------
        colnames is a list of column names

        chunk_size is the number of rows to return at a time

        obs_metadata is an ObservationMetaData

        constraint is ignored

        limit is ignored
        """

        if colnames is None:
            colnames = [k for k in self.columnMap]

        return DESCQAChunkIterator(self._catalog, self.columnMap, obs_metadata,
                                   colnames, chunk_size)
