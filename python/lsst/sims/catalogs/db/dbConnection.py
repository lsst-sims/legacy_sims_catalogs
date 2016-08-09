import warnings
import numpy
import os
import inspect
from StringIO import StringIO
from collections import OrderedDict

from .utils import loadData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import expression
from sqlalchemy.engine import reflection, url
from sqlalchemy import (create_engine, MetaData,
                        Table, event)
from sqlalchemy import exc as sa_exc
from lsst.daf.persistence import DbAuth

#The documentation at http://docs.sqlalchemy.org/en/rel_0_7/core/types.html#sqlalchemy.types.Numeric
#suggests using the cdecimal module.  Since it is not standard, import decimal.
#TODO: test for cdecimal and use it if it exists.
import decimal

__all__ = ["ChunkIterator", "DBObject", "CatalogDBObject", "fileDBObject"]

def valueOfPi():
    """
    A function to return the value of pi.  This is needed for adding PI()
    to sqlite databases
    """
    return numpy.pi

def declareTrigFunctions(conn,connection_rec,connection_proxy):
    """
    A database event listener
    which will define the math functions necessary for evaluating the
    Haversine function in sqlite databases (where they are not otherwise
    defined)

    see:    http://docs.sqlalchemy.org/en/latest/core/events.html
    """

    conn.create_function("COS",1,numpy.cos)
    conn.create_function("SIN",1,numpy.sin)
    conn.create_function("ASIN",1,numpy.arcsin)
    conn.create_function("SQRT",1,numpy.sqrt)
    conn.create_function("POWER",2,numpy.power)
    conn.create_function("PI",0,valueOfPi)

#------------------------------------------------------------
# Iterator for database chunks

class ChunkIterator(object):
    """Iterator for query chunks"""
    def __init__(self, dbobj, query, chunk_size, arbitrarySQL = False):
        self.dbobj = dbobj
        self.exec_query = dbobj.connection.session.execute(query)
        self.chunk_size = chunk_size

        #arbitrarySQL exists in case a CatalogDBObject calls
        #get_arbitrary_chunk_iterator; in that case, we need to
        #be able to tell this object to call _postprocess_arbitrary_results,
        #rather than _postprocess_results
        self.arbitrarySQL = arbitrarySQL

    def __iter__(self):
        return self

    def next(self):
        if self.chunk_size is None and not self.exec_query.closed:
            chunk = self.exec_query.fetchall()
            return self._postprocess_results(chunk)
        elif self.chunk_size is not None:
            chunk = self.exec_query.fetchmany(self.chunk_size)
            return self._postprocess_results(chunk)
        else:
            raise StopIteration

    def _postprocess_results(self, chunk):
        if len(chunk)==0:
            raise StopIteration
        if self.arbitrarySQL:
            return self.dbobj._postprocess_arbitrary_results(chunk)
        else:
            return self.dbobj._postprocess_results(chunk)


class DBConnection(object):
    """
    This is a class that will hold the engine, session, and metadata for a
    DBObject.  This will allow multiple DBObjects to share the same
    sqlalchemy connection, when appropriate.
    """

    def __init__(self, database=None, driver=None, host=None, port=None, verbose=False):
        """
        @param [in] database is the name of the database file being connected to

        @param [in] driver is the dialect of the database (e.g. 'sqlite', 'mssql', etc.)

        @param [in] host is the URL of the remote host, if appropriate

        @param [in] port is the port on the remote host to connect to, if appropriate

        @param [in] verbose is a boolean controlling sqlalchemy's verbosity
        """

        self._database = database
        self._driver = driver
        self._host = host
        self._port = port
        self._verbose = verbose

        self._validate_conn_params()
        self._connect_to_engine()


    def _connect_to_engine(self):

        #DbAuth will not look up hosts that are None, '' or 0
        if self._host:
            try:
                authDict = {'username': DbAuth.username(self._host, str(self._port)),
                            'password': DbAuth.password(self._host, str(self._port))}
            except:
                if self._driver == 'mssql+pymssql':
                    print("\nFor more information on database authentication using the db-auth.paf"
                          " policy file see: "
                          "https://confluence.lsstcorp.org/display/SIM/Accessing+the+UW+CATSIM+Database\n")
                raise

            dbUrl = url.URL(self._driver,
                            host=self._host,
                            port=self._port,
                            database=self._database,
                            **authDict)
        else:
            dbUrl = url.URL(self._driver,
                            database=self._database)


        self._engine = create_engine(dbUrl, echo=self._verbose)

        if self._engine.dialect.name == 'sqlite':
            event.listen(self._engine, 'checkout', declareTrigFunctions)

        self._session = scoped_session(sessionmaker(autoflush=True,
                                                    bind=self._engine))
        self._metadata = MetaData(bind=self._engine)


    def _validate_conn_params(self):
        """Validate connection parameters

        - Check if user passed dbAddress instead of an database. Convert and warn.
        - Check that required connection paramters are present
        - Replace default host/port if driver is 'sqlite'
        """

        if self._database is None:
            raise AttributeError("Cannot instantiate DBConnection; database is 'None'")

        if '//' in self._database:
            warnings.warn("Database name '%s' is invalid but looks like a dbAddress. "
                          "Attempting to convert to database, driver, host, "
                          "and port parameters. Any usernames and passwords are ignored and must "
                          "be in the db-auth.paf policy file. "%(self.database), FutureWarning)

            dbUrl = url.make_url(self._database)
            dialect = dbUrl.get_dialect()
            self._driver = dialect.name + '+' + dialect.driver if dialect.driver else dialect.name
            for key, value in dbUrl.translate_connect_args().iteritems():
                if value is not None:
                    setattr(self, '_'+key, value)

        errMessage = "Please supply a 'driver' kwarg to the constructor or in class definition. "
        errMessage += "'driver' is formatted as dialect+driver, such as 'sqlite' or 'mssql+pymssql'."
        if not hasattr(self, '_driver'):
            raise AttributeError("%s has no attribute 'driver'. "%(self.__class__.__name__) + errMessage)
        elif self._driver is None:
            raise AttributeError("%s.driver is None. "%(self.__class__.__name__) + errMessage)

        errMessage = "Please supply a 'database' kwarg to the constructor or in class definition. "
        errMessage += " 'database' is the database name or the filename path if driver is 'sqlite'. "
        if not hasattr(self, '_database'):
            raise AttributeError("%s has no attribute 'database'. "%(self.__class__.__name__) + errMessage)
        elif self._database is None:
            raise AttributeError("%s.database is None. "%(self.__class__.__name__) + errMessage)

        if 'sqlite' in self._driver:
            #When passed sqlite database, override default host/port
            self._host = None
            self._port = None


    def __eq__(self, other):
        return (self._database is other._database) and \
               (self._driver is other._driver) and \
               (self._host is other._host) and \
               (self._port is other._port) and \
               (self._verbose is other._verbose)


    @property
    def engine(self):
        return self._engine

    @property
    def session(self):
        return self._session


    @property
    def metadata(self):
        return self._metadata

    @property
    def database(self):
        return self._database

    @property
    def driver(self):
        return self._driver

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def verbose(self):
        return self._verbose


class DBObject(object):

    def __init__(self, database=None, driver=None, host=None, port=None, verbose=False,
                 connection=None):
        """
        Initialize DBObject.

        @param [in] database is the name of the database file being connected to

        @param [in] driver is the dialect of the database (e.g. 'sqlite', 'mssql', etc.)

        @param [in] host is the URL of the remote host, if appropriate

        @param [in] port is the port on the remote host to connect to, if appropriate

        @param [in] verbose is a boolean controlling sqlalchemy's verbosity (default False)

        @param [in] connection is an optional instance of DBConnection, in the event that
        this DBObject can share a database connection with another DBObject.  This is only
        necessary or even possible in a few specialized cases and should be used carefully.
        """

        self.dtype = None
        #this is a cache for the query, so that any one query does not have to guess dtype multiple times

        if connection is None:
            #Explicit constructor to DBObject preferred
            kwargDict = dict(database=database,
                         driver=driver,
                         host=host,
                         port=port,
                         verbose=verbose)

            for key, value in kwargDict.iteritems():
                if value is not None or not hasattr(self, key):
                    setattr(self, key, value)

            self.connection = DBConnection(database=self.database, driver=self.driver, host=self.host,
                                           port=self.port, verbose=self.verbose)
        else:
            self.connection = connection
            self.database = connection.database
            self.driver = connection.driver
            self.host = connection.host
            self.port = connection.port
            self.verbose = connection.verbose



    def get_table_names(self):
        """Return a list of the names of the tables in the database"""
        return [str(xx) for xx in reflection.Inspector.from_engine(self.connection.engine).get_table_names()]

    def get_column_names(self, tableName=None):
        """
        Return a list of the names of the columns in the specified table.
        If no table is specified, return a dict of lists.  The dict will be keyed
        to the table names.  The lists will be of the column names in that table
        """
        tableNameList = self.get_table_names()
        if tableName is not None:
            if tableName not in tableNameList:
                return []
            else:
                return [str(xx['name']) for xx in reflection.Inspector.from_engine(self.connection.engine).get_columns(tableName)]
        else:
            columnDict = {}
            for name in tableNameList:
                columnList = [str(xx['name']) for xx in reflection.Inspector.from_engine(self.connection.engine).get_columns(name)]
                columnDict[name] = columnList
            return columnDict

    def _final_pass(self, results):
        """ Make final modifications to a set of data before returning it to the user

        **Parameters**

            * results : a structured array constructed from the result set from a query

        **Returns**

            * results : a potentially modified structured array.  The default is to do nothing.

        """
        return results

    def _postprocess_results(self, results):
        """
        This wrapper exists so that a ChunkIterator built from a DBObject
        can have the same API as a ChunkIterator built from a CatalogDBObject
        """
        return self._postprocess_arbitrary_results(results)

    def _postprocess_arbitrary_results(self, results):

        if self.dtype is None:
            """
            Determine the dtype from the data.
            Store it in a global variable so we do not have to repeat on every chunk.
            """
            dataString = ''
            for xx in results[0]:
                if dataString is not '':
                    dataString+=','
                dataString += str(xx)
            names = [str(ww) for ww in results[0].keys()]
            dataArr = numpy.genfromtxt(StringIO(dataString), dtype=None, names=names, delimiter=',')
            self.dtype = dataArr.dtype

        if len(results) == 0:
            return numpy.recarray((0,), dtype = self.dtype)

        retresults = numpy.rec.fromrecords([tuple(xx) for xx in results],dtype = self.dtype)
        return self._final_pass(retresults)

    def execute_arbitrary(self, query, dtype = None):
        """
        Executes an arbitrary query.  Returns a recarray of the results.

        dtype will be the dtype of the output recarray.  If it is None, then
        the code will guess the datatype and assign generic names to the columns
        """

        if not isinstance(query,str):
            raise RuntimeError("DBObject execute must be called with a string query")

        unacceptableCommands = ["delete","drop","insert","update"]
        for badCommand in unacceptableCommands:
            if query.lower().find(badCommand.lower())>=0:
                raise RuntimeError("query made to DBObject execute contained %s " % badCommand)

        self.dtype = dtype
        retresults = self._postprocess_arbitrary_results(self.connection.session.execute(query).fetchall())
        return retresults

    def get_arbitrary_chunk_iterator(self, query, chunk_size = None, dtype =None):
        """
        This wrapper exists so that CatalogDBObjects can refer to
        get_arbitrary_chunk_iterator and DBObjects can refer to
        get_chunk_iterator
        """
        return self.get_chunk_iterator(query, chunk_size = chunk_size, dtype = dtype)

    def get_chunk_iterator(self, query, chunk_size = None, dtype = None):
        """
        Take an arbitrary, user-specified query and return a ChunkIterator that
        executes that query

        dtype will tell the ChunkIterator what datatype to expect for this query.
        This information gets passed to _postprocess_results.

        If 'None', then _postprocess_results will just guess the datatype
        and return generic names for the columns.
        """
        self.dtype = dtype
        return ChunkIterator(self, query, chunk_size, arbitrarySQL = True)

class CatalogDBObjectMeta(type):
    """Meta class for registering new objects.

    When any new type of object class is created, this registers it
    in a `registry` class attribute, available to all derived instance
    catalog.
    """
    def __new__(cls, name, bases, dct):
        # check if attribute objid is specified.
        # If not, create a default
        if 'registry' in dct:
            warnings.warn("registry class attribute should not be "
                          "over-ridden in InstanceCatalog classes. "
                          "Proceed with caution")
        if 'objid' not in dct:
            dct['objid'] = name
        return super(CatalogDBObjectMeta, cls).__new__(cls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        # check if 'registry' is specified.
        # if not, then this is the base class: add the registry
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        else:
            # add this class to the registry
            if cls.objid in cls.registry:
                srcfile = inspect.getsourcefile(cls.registry[cls.objid])
                srcline = inspect.getsourcelines(cls.registry[cls.objid])[1]
                warnings.warn('duplicate object identifier %s specified. '%(cls.objid)+\
                              'This will override previous definition on line %i of %s'%
                              (srcline, srcfile))
            cls.registry[cls.objid] = cls

        # check if the list of unique ids is specified
        # if not, then this is the base class: add the list
        if not hasattr(cls, 'objectTypeIdList'):
            cls.objectTypeIdList = []
        else:
            if cls.skipRegistration:
                pass
            elif cls.objectTypeId is None:
                pass #Don't add typeIds that are None
            elif cls.objectTypeId in cls.objectTypeIdList:
                warnings.warn('Duplicate object type id %s specified: '%cls.objectTypeId+\
                              '\nOutput object ids may not be unique.\nThis may not be a problem if you do not '+\
                              'want globally unique id values')
            else:
                cls.objectTypeIdList.append(cls.objectTypeId)
        return super(CatalogDBObjectMeta, cls).__init__(name, bases, dct)

    def __str__(cls):
        dbObjects = cls.registry.keys()
        outstr = "++++++++++++++++++++++++++++++++++++++++++++++\n"+\
                 "Registered object types are:\n"
        for dbObject in dbObjects:
            outstr += "%s\n"%(dbObject)
        outstr += "\n\n"
        outstr += "To query the possible column names do:\n"
        outstr += "$> CatalogDBObject.from_objid([name]).show_mapped_columns()\n"
        outstr += "+++++++++++++++++++++++++++++++++++++++++++++"
        return outstr

class CatalogDBObject(DBObject):
    """Database Object base class

    """
    __metaclass__ = CatalogDBObjectMeta

    epoch = 2000.0
    skipRegistration = False
    objid = None
    tableid = None
    idColKey = None
    objectTypeId = None
    columns = None
    generateDefaultColumnMap = True
    dbDefaultValues = {}
    raColName = None
    decColName = None

    #Provide information if this object should be tested in the unit test
    doRunTest = False
    testObservationMetaData = None

    #: Mapping of DDL types to python types.  Strings are assumed to be 256 characters
    #: this can be overridden by modifying the dbTypeMap or by making a custom columns
    #: list.
    #: numpy doesn't know how to convert decimal.Decimal types, so I changed this to float
    #: TODO this doesn't seem to make a difference but make sure.
    dbTypeMap = {'BIGINT':(int,), 'BOOLEAN':(bool,), 'FLOAT':(float,), 'INTEGER':(int,),
                 'NUMERIC':(float,), 'SMALLINT':(int,), 'TINYINT':(int,), 'VARCHAR':(str, 256),
                 'TEXT':(str, 256), 'CLOB':(str, 256), 'NVARCHAR':(str, 256),
                 'NCLOB':(unicode, 256), 'NTEXT':(unicode, 256), 'CHAR':(str, 1), 'INT':(int,),
                 'REAL':(float,), 'DOUBLE':(float,), 'STRING':(str, 256), 'DOUBLE_PRECISION':(float,),
                 'DECIMAL':(float,)}

    @classmethod
    def from_objid(cls, objid, *args, **kwargs):
        """Given a string objid, return an instance of
        the appropriate CatalogDBObject class.
        """
        if objid not in cls.registry:
            raise RuntimeError('Attempting to construct an object that does not exist')
        cls = cls.registry.get(objid, CatalogDBObject)
        return cls(*args, **kwargs)

    def __init__(self, database=None, driver=None, host=None, port=None, verbose=False,
                 table=None, objid=None, idColKey=None, connection=None):
        if not verbose:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)

        if self.tableid is not None and table is not None:
            raise ValueError("Double-specified tableid in CatalogDBObject:"
                             " once in class definition, once in __init__")

        if table is not None:
            self.tableid = table

        if self.objid is not None and objid is not None:
            raise ValueError("Double-specified objid in CatalogDBObject:"
                             " once in class definition, once in __init__")

        if objid is not None:
            self.objid = objid

        if self.idColKey is not None and idColKey is not None:
            raise ValueError("Double-specified idColKey in CatalogDBObject:"
                             " once in class definition, once in __init__")

        if idColKey is not None:
            self.idColKey = idColKey

        if self.idColKey is None:
            self.idColKey = self.getIdColKey()
        if (self.objid is None) or (self.tableid is None) or (self.idColKey is None):
            raise ValueError("CatalogDBObject must be subclassed, and "
                             "define objid, tableid and idColKey.")

        if (self.objectTypeId is None) and verbose:
            warnings.warn("objectTypeId has not "
                          "been set.  Input files for phosim are not "
                          "possible.")

        super(CatalogDBObject, self).__init__(database=database, driver=driver, host=host, port=port,
                                              verbose=verbose, connection=connection)

        try:
            self._get_table()
        except sa_exc.OperationalError, e:
            if self.driver == 'mssql+pymssql':
                message = "\n To connect to the UW CATSIM database: "
                message += " Check that you have valid connection parameters, an open ssh tunnel "
                message += "and that your $HOME/.lsst/db-auth.paf contains the appropriate credientials. "
                message += "Please consult the following link for more information on access: "
                message += " https://confluence.lsstcorp.org/display/SIM/Accessing+the+UW+CATSIM+Database "
            else:
                message = ''
            raise RuntimeError("Failed to connect to %s: sqlalchemy.%s %s" % (self.connection.engine, e.message, message))

        #Need to do this after the table is instantiated so that
        #the default columns can be filled from the table object.
        if self.generateDefaultColumnMap:
            self._make_default_columns()
        # build column mapping and type mapping dicts from columns
        self._make_column_map()
        self._make_type_map()

    def show_mapped_columns(self):
        for col in self.columnMap.keys():
            print "%s -- %s"%(col, self.typeMap[col][0].__name__)

    def show_db_columns(self):
        for col in self.table.c.keys():
            print "%s -- %s"%(col, self.table.c[col].type.__visit_name__)


    def getCatalog(self, ftype, *args, **kwargs):
        try:
            from lsst.sims.catalogs.measures.instance import\
                    InstanceCatalog
            return InstanceCatalog.new_catalog(ftype, self, *args, **kwargs)
        except ImportError:
            raise ImportError("sims_catalogs_measures not set up.  Cannot get InstanceCatalog from the object.")

    def getIdColKey(self):
        return self.idColKey

    def getObjectTypeId(self):
        return self.objectTypeId

    def _get_table(self):
        self.table = Table(self.tableid, self.connection.metadata,
                           autoload=True)

    def _make_column_map(self):
        self.columnMap = OrderedDict([(el[0], el[1] if el[1] else el[0])
                                     for el in self.columns])
    def _make_type_map(self):
        self.typeMap = OrderedDict([(el[0], el[2:] if len(el)> 2 else (float,))
                                   for el in self.columns])

    def _make_default_columns(self):
        if self.columns:
            colnames = [el[0] for el in self.columns]
        else:
            self.columns = []
            colnames = []
        for col in self.table.c.keys():
            dbtypestr = self.table.c[col].type.__visit_name__
            dbtypestr = dbtypestr.upper()
            if col in colnames:
                if self.verbose: #Warn for possible column redefinition
                    warnings.warn("Database column, %s, overridden in self.columns... "%(col)+
                                  "Skipping default assignment.")
            elif dbtypestr in self.dbTypeMap:
                self.columns.append((col, col)+self.dbTypeMap[dbtypestr])
            else:
                if self.verbose:
                    warnings.warn("Can't create default column for %s.  There is no mapping "%(col)+
                                  "for type %s.  Modify the dbTypeMap, or make a custom columns "%(dbtypestr)+
                                  "list.")

    def _get_column_query(self, colnames=None):
        """Given a list of valid column names, return the query object"""
        if colnames is None:
            colnames = [k for k in self.columnMap]
        try:
            vals = [self.columnMap[k] for k in colnames]
        except KeyError:
            for col in colnames:
                if col in self.columnMap:
                    continue
                else:
                    warnings.warn("%s not in columnMap"%(col))
            raise ValueError('entries in colnames must be in self.columnMap')

        # Get the first query
        idColName = self.columnMap[self.idColKey]
        if idColName in vals:
            idLabel = self.idColKey
        else:
            idLabel = idColName

        query = self.connection.session.query(self.table.c[idColName].label(idLabel))

        for col, val in zip(colnames, vals):
            if val is idColName:
                continue
            #Check if the column is a default column (col == val)
            if col == val:
                #If column is in the table, use it.
                query = query.add_column(self.table.c[col].label(col))
            else:
                #If not assume the user specified the column correctly
                query = query.add_column(expression.literal_column(val).label(col))

        return query

    def filter(self, query, bounds):
        """Filter the query by the associated metadata"""
        if bounds is not None:
            on_clause = bounds.to_SQL(self.raColName,self.decColName)
            query = query.filter(on_clause)
        return query

    def _postprocess_results(self, results):
        """Post-process the query results to put them
        in a structured array.

        **Parameters**

            * results : a result set as returned by execution of the query

        **Returns**

            * _final_pass(retresults) : the result of calling the _final_pass method on a
              structured array constructed from the query data.
        """

        if len(results) > 0:
            cols = [str(k) for k in results[0].keys()]
        else:
            return results

        dtype = numpy.dtype([(k,)+self.typeMap[k] for k in cols])

        if len(set(cols)&set(self.dbDefaultValues)) > 0:

            results_array = []

            for result in results:
                results_array.append([
                                      result[colName] if result[colName] or colName not in self.dbDefaultValues
                                      else self.dbDefaultValues[colName] for colName in cols
                                     ])

        else:
            results_array = results
        retresults = numpy.rec.fromrecords(results_array, dtype=dtype)
        return self._final_pass(retresults)

    def query_columns(self, colnames=None, chunk_size=None,
                      obs_metadata=None, constraint=None, limit=None):
        """Execute a query

        **Parameters**

            * colnames : list or None
              a list of valid column names, corresponding to entries in the
              `columns` class attribute.  If not specified, all columns are
              queried.
            * chunk_size : int (optional)
              if specified, then return an iterator object to query the database,
              each time returning the next `chunk_size` elements.  If not
              specified, all matching results will be returned.
            * obs_metadata : object (optional)
              an observation metadata object which has a "filter" method, which
              will add a filter string to the query.
            * constraint : str (optional)
              a string which is interpreted as SQL and used as a predicate on the query
            * limit : int (optional)
              limits the number of rows returned by the query

        **Returns**

            * result : list or iterator
              If chunk_size is not specified, then result is a list of all
              items which match the specified query.  If chunk_size is specified,
              then result is an iterator over lists of the given size.

        """
        query = self._get_column_query(colnames)

        if obs_metadata is not None:
            query = self.filter(query, obs_metadata.bounds)

        if constraint is not None:
            query = query.filter(constraint)

        if limit is not None:
            query = query.limit(limit)

        return ChunkIterator(self, query, chunk_size)

class fileDBObject(CatalogDBObject):
    ''' Class to read a file into a database and then query it'''
    #Column names to index.  Specify compound indexes using tuples of column names
    indexCols = []
    def __init__(self, dataLocatorString, runtable=None, driver="sqlite", host=None, port=None, database=":memory:",
                dtype=None, numGuess=1000, delimiter=None, verbose=False, idColKey=None, **kwargs):
        """
        Initialize an object for querying databases loaded from a file

        Keyword arguments:
        @param dataLocatorString: Path to the file to load
        @param runtable: The name of the table to create.  If None, a random table name will be used.
        @param driver: name of database driver (e.g. 'sqlite', 'mssql+pymssql')
        @param host: hostname for database connection (None if sqlite)
        @param port: port for database connection (None if sqlite)
        @param database: name of database (filename if sqlite)
        @param dtype: The numpy dtype to use when loading the file.  If None, it the dtype will be guessed.
        @param numGuess: The number of lines to use in guessing the dtype from the file.
        @param delimiter: The delimiter to use when parsing the file default is white space.
        @param idColKey: The name of the column that uniquely identifies each row in the database
        """
        self.verbose = verbose

        if idColKey is not None:
            self.idColKey = idColKey

        if(self.objid is None) or (self.idColKey is None):
            raise ValueError("CatalogDBObject must be subclassed, and "
                             "define objid and tableid and idColKey.")

        if (self.objectTypeId is None) and self.verbose:
            warnings.warn("objectTypeId has not "
                          "been set.  Input files for phosim are not "
                          "possible.")

        if os.path.exists(dataLocatorString):
            self.driver = driver
            self.host = host
            self.port = port
            self.database = database
            self.connection = DBConnection(database=self.database, driver=self.driver, host=self.host,
                                           port=self.port, verbose=verbose)
            self.tableid = loadData(dataLocatorString, dtype, delimiter, runtable, self.idColKey,
                                    self.connection.engine, self.connection.metadata, numGuess,
                                    indexCols=self.indexCols, **kwargs)
            self._get_table()
        else:
            raise ValueError("Could not locate file %s."%(dataLocatorString))

        if self.generateDefaultColumnMap:
            self._make_default_columns()

        self._make_column_map()
        self._make_type_map()

    @classmethod
    def from_objid(cls, objid, *args, **kwargs):
        """Given a string objid, return an instance of
        the appropriate fileDBObject class.
        """
        cls = cls.registry.get(objid, CatalogDBObject)
        return cls(*args, **kwargs)
