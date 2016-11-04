from __future__ import with_statement
import numpy as np
from StringIO import StringIO
from sqlalchemy import (types as satypes, Column, Table, Index,
                        create_engine, MetaData)
import string
import random


def np_to_sql_type(input_type):
    """
    Returns the SQL data type (as encoded by sqlalchemy)
    corresponding to a numpy dtype

    input_type is an element of a numpy.dtype array
    """
    name = input_type.name
    size = input_type.itemsize
    if name.startswith('float'):
        return satypes.Float(precision=16)
    if name == 'int64':
        return satypes.BIGINT()
    if name == 'int32':
        return satypes.Integer()
    if name.startswith('string'):
        return satypes.String(length=size)

    raise RuntimeError("Do not know how to map %s to SQL" % str(input_type))


# from http://stackoverflow.com/questions/2257441/python-random-string-generation-with-upper-case-letters-and-digits
def id_generator(size=8, chars=string.ascii_lowercase):
    return ''.join(random.choice(chars) for x in range(size))


def make_engine(dbAddress):
    """create and connect to a database engine"""
    engine = create_engine(dbAddress, echo=False)
    metadata = MetaData(bind=engine)
    return engine, metadata


def guessDtype(dataPath, numGuess, delimiter, **kwargs):
    cnt = 0
    teststr = ''
    with open(dataPath) as fh:
        while cnt < numGuess:
            teststr += fh.readline()
            cnt += 1
    dataArr = np.genfromtxt(StringIO(teststr), dtype=None, names=True, delimiter=delimiter, **kwargs)
    return dataArr.dtype


def createSQLTable(dtype, tableid, idCol, metadata):
    """
    create a sqlalchemy Table object.

    Parameters
    ----------
    dtype is a numpy dtype describing the columns in the table
    tableid is the name of the table to be created
    idCol is the column on which to construct the Table's primary key
    metadata is the sqlalchemy MetaData object associated with the database connection

    Returns
    -------
    A sqlalchemy Table object with the columns specified by dtype
    """
    sqlColumns = []
    for itype in range(len(dtype)):
        sqlType = np_to_sql_type(dtype[itype])
        name = dtype.names[itype]
        sqlColumns.append(Column(name, sqlType, primary_key = (idCol == name)))

    if tableid is None:
        tableid = id_generator()
    datatable = Table(tableid, metadata, *sqlColumns)
    metadata.create_all()
    return datatable


def loadTable(datapath, datatable, delimiter, dtype, engine,
              indexCols=[], skipLines=1, chunkSize=100000, **kwargs):
    cnt = 0
    with open(datapath) as fh:
        while cnt < skipLines:
            fh.readline()
            cnt += 1
        cnt = 0
        tmpstr = ''
        for l in fh:
            tmpstr += l
            cnt += 1
            if cnt%chunkSize == 0:
                print "Loading chunk #%i"%(int(cnt/chunkSize))
                dataArr = np.genfromtxt(StringIO(tmpstr), dtype=dtype, delimiter=delimiter, **kwargs)
                engine.execute(datatable.insert(),
                               [dict((name, np.asscalar(l[name])) for name in l.dtype.names)
                                for l in dataArr])
                tmpstr = ''
        # Clean up the last chunk
        if len(tmpstr) > 0:
            dataArr = np.genfromtxt(StringIO(tmpstr), dtype=dtype, delimiter=delimiter, **kwargs)
            try:
                engine.execute(datatable.insert(),
                               [dict((name, np.asscalar(l[name])) for name in l.dtype.names)
                                for l in dataArr])
            # If the file only has one line, the result of genfromtxt is a 0-d array, so cannot be iterated
            except TypeError:
                engine.execute(datatable.insert(),
                               [dict((name, np.asscalar(dataArr[name])) for name in dataArr.dtype.names), ])

    for col in indexCols:
        if hasattr(col, "__iter__"):
            print "Creating index on %s"%(",".join(col))
            colArr = (datatable.c[c] for c in col)
            i = Index('%sidx'%''.join(col), *colArr)
        else:
            print "Creating index on %s"%(col)
            i = Index('%sidx'%col, datatable.c[col])

        i.create(engine)


def loadData(dataPath, dtype, delimiter, tableId, idCol, engine, metaData, numGuess, append=False, **kwargs):
    if dtype is None:
        dtype = guessDtype(dataPath, numGuess, delimiter)

    tableExists = False

    if tableId is not None:
        tableExists = engine.dialect.has_table(engine.connect(), tableId)
    if append and tableId is None:
        raise ValueError("Cannot append if the table name is missing")
    elif tableExists and not append:
        raise ValueError("Append is False but table exists")
    elif not tableExists:
        dataTable = createSQLTable(dtype, tableId, idCol, metaData)
    else:
        dataTable = Table(tableId, metaData, autoload=True)
    loadTable(dataPath, dataTable, delimiter, dtype, engine, **kwargs)
    return dataTable.name
