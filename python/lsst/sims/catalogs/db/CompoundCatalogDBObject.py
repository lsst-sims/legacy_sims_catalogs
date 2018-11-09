from builtins import zip
from builtins import str
from builtins import range
from lsst.sims.catalogs.db import CatalogDBObject

__all__ = ["CompoundCatalogDBObject",
           "_CompoundCatalogDBObject_mixin"]


class _CompoundCatalogDBObject_mixin(object):
    """
    This mixin exists to separate out utility methods that we will
    need for the DESC DC2 CompoundCatalogDBObject
    """
    def _make_columns(self):
        """
        Construct the self.columns member by concatenating the self.columns
        from the input CatalogDBObjects and modifying the names of the returned
        columns to identify them with their specific CatalogDBObjects.
        """
        raw_column_names = []  # the names as they appear on the database
        processed_column_names = []  # the names as they appear in the CatalogDBObject
        prefix_column_names = []  # the names with the objid added
        raw_column_transform = []  # the full transform applied by the CatalogDBObject
        all_rows = []  # store the raw rows for the last block of code in this method

        for dbo, dbName in zip(self._dbObjectClassList, self._nameList):
            db_inst = dbo()
            for row in db_inst.columns:
                all_rows.append(row)
                raw_column_names.append(row[1])
                processed_column_names.append(row[0])
                prefix_column_names.append('%s_%s' % (dbName, row[0]))
                raw_column_transform.append(row[1:])


        self._compound_dbo_name_map = {}
        self.columns = []
        processed_columns_requiring_prefix = set()
        column_diagnostic = {}

        # Now we need to figure out which of the CatalogDBObject-mapped columns
        # actually need to be kept independent (i.e sedFilename for galaxy bulges will
        # not actually be referencing the same column as sedFilename for galaxy disks)
        # and which can be lumped together (i.e. redshift will be the same database
        # column for galaxy bulges and disks)
        for i_r1 in range(len(raw_column_names)):

            # if a processed column has already been determined to be degenerate,
            # just acknowledget that
            use_prefix = processed_column_names[i_r1] in processed_columns_requiring_prefix

            if not use_prefix:
                # If two CatalogDBObjects map different columns in the raw database to
                # the same transformed column name, then we need to keep the two
                # distinct in this CompoundCatalogDBObject; we do that by using the
                # prefix_name, which prepends the objid of the CatalogDBobject to the
                # transformed column
                for i_r2 in range(i_r1+1, len(raw_column_names)):
                    if (processed_column_names[i_r1] == processed_column_names[i_r2] and
                        raw_column_transform[i_r1] != raw_column_transform[i_r2]):

                        use_prefix = True
                        break

                if use_prefix:
                    processed_columns_requiring_prefix.add(processed_column_names[i_r1])

            # under what name will the column actually be queried
            if use_prefix:
                query_name = prefix_column_names[i_r1]
            else:
                query_name = processed_column_names[i_r1]


            if prefix_column_names[i_r1] in self._compound_dbo_name_map:
                raise RuntimeError("Trying to put %s in compound_db_name_map twice" %
                                   prefix_column_names[i_r1])

            # build the dict that maps the prefixed column names, which CompoundInstanceCatalog
            # will reference, to the names that are actually going to be queried from
            # this CompoundCatalogDBObject
            self._compound_dbo_name_map[prefix_column_names[i_r1]] = query_name

            # build the self.columns member variable of this CompoundCatalogDBObject
            column_row = [query_name]
            column_row += [ww for ww in raw_column_transform[i_r1]]

            # if no transformation was applied, we need to map the column
            # back to the database column of the same name
            if column_row[1] is None:
                column_row[1] = processed_column_names[i_r1]

            column_row = tuple(column_row)

            if column_row[0] in column_diagnostic:
                if column_row != column_diagnostic[column_row[0]]:
                    row1 = column_row
                    row2 = column_diagnostic[column_row[0]]
                    raise RuntimeError("Trying to change definition of columns "
                                       + "\n%s\n%s\n" % (row1, row2))

            if column_row not in self.columns:
                self.columns.append(column_row)
                column_diagnostic[column_row[0]] = column_row

        # 8 November 2018 (originally 25 August 2015)
        # This is a modification that needs to be made in order for this
        # class to work with GalaxyTileObj.  The column galaxytileid in
        # GalaxyTileObj is removed from the query by query_columns, but
        # somehow injected back in by the query procedure on fatboy. This
        # leads to confusion if you try to query something like
        # galaxyAgn_galaxytileid.  We deal with that by removing all column
        # names like 'galaxytileid' in query_columns, but leaving 'galaxytileid'
        # un-mangled in self.columns so that self.typeMap knows how to deal
        # with it when it comes back

        for column_row in all_rows:
            if (column_row[0] not in self._compound_dbo_name_map and
                (column_row[1] is None or column_row[1] == column_row[0])):

                # again: deal with cases where no transformation is applied
                if column_row[1] is None:
                    new_row = [ww for ww in column_row]
                    new_row[1] = new_row[0]
                    column_row = tuple(new_row)

                if column_row[0] in column_diagnostic:
                    if column_row != column_diagnostic[column_row[0]]:
                        row1 = column_row
                        row2 = column_diagnostic[column_row[0]]
                        raise RuntimeError("Trying to change definition of columns "
                                           + "\n%s\n%s\n" % (row1, row2))

                if column_row not in self.columns:
                    self.columns.append(column_row)
                    column_diagnostic[column_row[0]] = column_row

                if column_row[0] in self._compound_dbo_name_map:
                    if column_row[0] != self._column_dbo_name_map[column_row[0]]:
                        raise RuntimeError("Column name map conflict")

                self._compound_dbo_name_map[column_row[0]] = column_row[0]

    def name_map(self, name):
        """
        Map a column name with the CatalogDBObject's objid prepended to the
        name of the column that will actually be queried from the database
        """
        if not hasattr(self, '_compound_dbo_name_map'):
            raise RuntimeError("This CompoundCatalogDBObject does not have a name_map")
        return self._compound_dbo_name_map[name]


class CompoundCatalogDBObject(_CompoundCatalogDBObject_mixin, CatalogDBObject):
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
        # (see GalaxyTileObj in sims_catUtils/../baseCatalogModels/GalaxyModels.py)

        self.tableid = dbo.tableid
        self.idColKey = dbo.idColKey
        self.raColName = dbo.raColName
        self.decColName = dbo.decColName

        super(CompoundCatalogDBObject, self).__init__(connection=dbo.connection)

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
                    raise RuntimeError('The objid %s ' % dbo.objid +
                                       'is duplicated in your list of ' +
                                       'CatalogDBObjects\n' +
                                       'CompoundCatalogDBObject requires each' +
                                       ' CatalogDBObject have a unique objid\n')

        acceptable = True
        msg = ''
        if len(hostList) > 1:
            acceptable = False
            msg += ' hosts: ' + str(hostList) + '\n'

        if len(databaseList) != 1:
            acceptable = False
            msg += ' databases: ' + str(databaseList) + '\n'

        if len(portList) > 1:
            acceptable = False
            msg += ' ports: ' + str(portList) + '\n'

        if len(driverList) > 1:
            acceptable = False
            msg += ' drivers: ' + str(driverList) + '\n'

        if len(tableList) != 1:
            acceptable = False
            msg += ' tables: ' + str(tableList) + '\n'

        if not acceptable:
            raise RuntimeError('The CatalogDBObjects fed to ' +
                               'CompoundCatalogDBObject do not all ' +
                               'query the same table:\n' +
                               msg)

        if self._table_restriction is not None and len(tableList) > 0:
            if tableList[0] not in self._table_restriction:
                raise RuntimeError("This CompoundCatalogDBObject does not support " +
                                   "the table '%s' " % tableList[0])
