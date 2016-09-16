import numpy
from collections import OrderedDict
from lsst.sims.catalogs.db import CatalogDBObject

__all__ = ["CompoundCatalogDBObject"]

class CompoundCatalogDBObject(CatalogDBObject):
    """
    This is a class for taking several CatalogDBObject daughter classes that
    query the same table of the same database for the same rows (but different
    columns; note that the columns can be transformed by the CatalogDBObjects'
    self.columns member), and combining their queries into one.

    You feed the constructor a list of CatalogDBObject daughter classes.  The
    CompoundCatalogDBObject verifies that they all do, indeed, query the same table
    of the same database.  It then constructs its own self.columns member (note
    that CompoundCatalogDBObject is a daughter class of CatalogDBObject) which
    combines all of the requested data.

    When you call query_columns, a recarray will be returned as in a CatalogDBObject.
    Note, however, that the names of the columns of the recarray will be modified.
    If the first CatalogDBObject in the list of CatalogDBObjects passed to the constructor
    asks for a column named 'col1', that will be mapped to 'catName_col1' where 'catName'
    is the CatalogDBObject's objid member.  'col2' will be mapped to 'catName_col2', etc.
    In cases where the CatalogDBObject does not change the name of the column, the column
    will also be returned by its original, un-mangled name.

    In cases where a custom query_columns method must be implemented, this class
    can be sub-classed and the custom method added as a member method.  In that
    case, the _table_restriction member variable should be set to a list of table
    names corresponding to the tables for which this class was designed.  An
    exception will be raised if the user tries to use the CompoundCatalogDBObject
    class to query tables for which it was not written.  _table_restriction defaults
    to None, which means that the class is for use with any table.
    """

    # This member variable is an optional list of tables supported
    # by a specific CompoundCatalogDBObject sub-class.  If
    # _table_restriction==None, then any table is supported
    _table_restriction = None

    def __init__(self, catalogDbObjectClassList, connection=None):
        """
        @param [in] catalogDbObjectClassList is a list of CatalogDBObject
        daughter classes (not instantiations of those classes; the classes
        themselves) that all query the same database table

        Note: this is a list of classes, not a list of instantiations of those
        classes.  The connection to the database is established as soon as
        you instantiate a CatalogDBObject daughter class.  To avoid creating
        unnecessary database connections, CompoundCatalogDBObject will
        read in classes without an active connection and establish its
        own connection in this constructor.  This means that all connection
        parameters must be specified in the class definitions of the classes
        passed into catalogDbObjectClassList.

        @param [in] connection is an optional instantiation of DBConnection
        representing an active connection to the database required by
        this CompoundCatalogDBObject (prevents the CompoundCatalogDBObject
        from opening a redundant connection)
        """

        self._master_column_name_map = {}

        self._dbObjectClassList = catalogDbObjectClassList
        self._validate_input()

        self._nameList = []
        for ix in range(len(self._dbObjectClassList)):
            self._nameList.append(self._dbObjectClassList[ix].objid)

        self._make_columns()
        self._make_dbTypeMap()
        self._make_dbDefaultValues()

        dbo = self._dbObjectClassList[0](connection=connection)
        # need to instantiate the first one because sometimes
        # idColKey is not defined until instantiation
        # (see GalaxyTileObj in sims_catUtils/../baseCatalogModels/GalaxyModels.py

        self.tableid = dbo.tableid
        self.idColKey = dbo.idColKey
        self.raColName = dbo.raColName
        self.decColName = dbo.decColName

        if hasattr(dbo, 'driver'):
            driver = dbo.driver
        else:
            driver = None

        if hasattr(dbo, 'host'):
            host = dbo.host
        else:
            host = None

        if hasattr(dbo, 'port'):
            port = dbo.port
        else:
            port = None

        if hasattr(dbo, 'verbose'):
            verbose = dbo.verbose
        else:
            verbose = False

        super(CompoundCatalogDBObject, self).__init__(connection=dbo.connection)


    def _make_columns(self):
        """
        Construct the self.columns member by concatenating the self.columns
        from the input CatalogDBObjects and modifying the names of the returned
        columns to identify them with their specific CatalogDBObjects.
        """
        column_names = []
        self.columns= []
        root_and_transform_dict = {}
        master_name_list = []
        for dbo, dbName in zip(self._dbObjectClassList, self._nameList):
            for row in dbo.columns:

                # find the base column (on the database), and the transform
                # to be applied to that columns
                new_root_and_transform = row[1:]

                # check if there is, in fact, no transform being applied to the column
                if len(new_root_and_transform)==1:
                        new_root_and_transform = (row[1], None)

                if new_root_and_transform[0] is None:
                    new_root_and_transform = (row[0], None)

                if row[0] == row[1]:
                    new_root_and_transform = (row[1], None)

                # If that combination of transform and base column has already been discovered,
                # do nothing.  If not, create a new entry in self.columns.  This way, we only
                # query each column and transform combination once, no matter how many times
                # they are needed.
                if new_root_and_transform not in root_and_transform_dict:
                    master_name = None
                    i_name = 0
                    while master_name is None or master_name in master_name_list:
                        i_name += 1
                        master_name = str('master_%d_%s' % (i_name, row[0]))

                    new_row=[ww for ww in row]  # create a new row for self.columns
                    new_row[0]=master_name
                    if new_row[1] is None:
                        new_row[1] = row[0]

                    self.columns.append(tuple(new_row))
                    column_names.append(new_row[0])
                    master_name_list.append(master_name)
                    root_and_transform_dict[new_root_and_transform] = master_name
                else:
                    master_name = root_and_transform_dict[new_root_and_transform]

                # keep track of how individual CatalodDBObject columns map to
                # columns as returned by this CompoundCatalogDbObject
                self._master_column_name_map['%s_%s' % (str(dbName), row[0])] = master_name

                # 25 August 2015
                # This is a modification that needs to be made in order for this
                # class to work with GalaxyTileObj.  The column galaxytileid in
                # GalaxyTileObj is removed from the query by query_columns, but
                # somehow injected back in by the query procedure on fatboy. This
                # leads to confusion if you try to query something like
                # galaxyAgn_galaxytileid.  We deal with that by removing all column
                # names like 'galaxytileid' in query_columns, but leaving 'galaxytileid'
                # un-mangled in self.columns so that self.typeMap knows how to deal
                # with it when it comes back.
                if row[0] not in column_names and (row[1] is None or row[1]==row[0]):
                    self.columns.append(row)
                    column_names.append(row[0])


    def _make_dbTypeMap(self):
        """
        Construct the self.dbTypeMap member by concatenating the self.dbTypeMaps
        from the input CatalogDBObjects.
        """
        self.dbTypeMap = {}
        for dbo in self._dbObjectClassList:
            for col in dbo.dbTypeMap:
                if col not in self.dbTypeMap:
                    self.dbTypeMap[col] = dbo.dbTypeMap[col]


    def _make_dbDefaultValues(self):
        """
        Construct the self.dbDefaultValues member by concatenating the
        self.dbDefaultValues from the input CatalogDBObjects.
        """
        self.dbDefaultValues = {}
        for dbo, dbName in zip(self._dbObjectClassList, self._nameList):
            for col in dbo.dbDefaultValues:
                self.dbDefaultValues['%s_%s' % (dbName, col)] = dbo.dbDefaultValues[col]


    def _validate_input(self):
        """
        Verify that the CatalogDBObjects passed to the constructor
        do, indeed, query the same table of the same database.

        Also verify that this class is designed to query the tables
        it is being used on (in cases where a custom query_columns
        has been implemented).
        """
        hostList = []
        databaseList = []
        portList = []
        driverList = []
        tableList = []
        objidList = []
        for dbo in self._dbObjectClassList:

            if hasattr(dbo, 'host'):
                if dbo.host not in hostList:
                    hostList.append(dbo.host)

            if hasattr(dbo, 'database'):
                if dbo.database not in databaseList:
                    databaseList.append(dbo.database)

            if hasattr(dbo, 'port'):
                if dbo.port not in portList:
                    portList.append(dbo.port)

            if hasattr(dbo, 'driver'):
                if dbo.driver not in driverList:
                    driverList.append(dbo.driver)

            if hasattr(dbo, 'tableid'):
                if dbo.tableid not in tableList:
                    tableList.append(dbo.tableid)

            if hasattr(dbo, 'objid'):
                if dbo.objid not in objidList:
                    objidList.append(dbo.objid)
                else:
                    raise RuntimeError('WARNING the objid %s ' % dbo.objid \
                                         + 'is duplicated in your list of ' \
                                         + 'CatalogDBObjects\n' \
                                         + 'CompoundCatalogDBObject requires each' \
                                         + ' CatalogDBObject have a unique objid\n')

        acceptable = True
        msg = ''
        if len(hostList)>1:
            acceptable = False
            msg += ' hosts: ' + str(hostList) + '\n'

        if len(databaseList)!=1:
            acceptable = False
            msg += ' databases: ' + str(databaseList) + '\n'

        if len(portList)>1:
            acceptable = False
            msg += ' ports: ' + str(portList) + '\n'

        if len(driverList)>1:
            acceptable = False
            msg += ' drivers: ' + str(driverList) + '\n'

        if len(tableList)!=1:
            acceptable = False
            msg += ' tables: ' + str(tableList) + '\n'

        if not acceptable:
            raise RuntimeError('The CatalogDBObjects fed to ' \
                               + 'CompoundCatalogDBObject do not all ' \
                               + 'query the same table:\n' \
                               + msg)

        if self._table_restriction is not None and len(tableList)>0:
            if tableList[0] not in self._table_restriction:
                raise RuntimeError("This CompoundCatalogDBObject does not support " \
                                   + "the table '%s' " % tableList[0])

    @property
    def master_column_name_map(self):
        """
        This is a dict keyed on the names of columns from the individual
        CatalogDBObjects making up this CompoundCatalogDBObject (mangled to
        look like '%s_%s' % (db.objid, colname), returning the names of the
        columns as stored in the recarray returned by this CompoundCatalogDBObject
        (something like 'master_1_raJ2000').
        """
        return self._master_column_name_map
