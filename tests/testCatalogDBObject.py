from __future__ import with_statement
from __future__ import print_function
from builtins import zip
from builtins import str
from builtins import super
import os
import sqlite3
import sys
import json

import unittest
import numpy as np
import tempfile
import shutil
import lsst.utils.tests
from lsst.sims.utils.CodeUtilities import sims_clean_up
from lsst.sims.utils import ObservationMetaData
from lsst.sims.catalogs.db import CatalogDBObject, fileDBObject
import lsst.sims.catalogs.utils.testUtils as tu
from lsst.sims.catalogs.utils.testUtils import myTestStars, myTestGals
from lsst.sims.utils import haversine

ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class dbForQueryColumnsTest(CatalogDBObject):
    objid = 'queryColumnsNonsense'
    tableid = 'queryColumnsTest'
    idColKey = 'i1'
    dbDefaultValues = {'i2': -1, 'i3': -2}


class myNonsenseDB(CatalogDBObject):
    objid = 'Nonsense'
    tableid = 'test'
    idColKey = 'NonsenseId'
    driver = 'sqlite'
    raColName = 'ra'
    decColName = 'dec'
    columns = [('NonsenseId', 'id', int),
               ('NonsenseRaJ2000', 'ra*%f'%(np.pi/180.)),
               ('NonsenseDecJ2000', 'dec*%f'%(np.pi/180.)),
               ('NonsenseMag', 'mag', float)]


class myNonsenseDB_noConnection(CatalogDBObject):
    """
    In order to test that we can pass a DBConnection in
    through the constructor
    """
    objid = 'Nonsense_noConnection'
    tableid = 'test'
    idColKey = 'NonsenseId'
    raColName = 'ra'
    decColName = 'dec'
    columns = [('NonsenseId', 'id', int),
               ('NonsenseRaJ2000', 'ra*%f'%(np.pi/180.)),
               ('NonsenseDecJ2000', 'dec*%f'%(np.pi/180.)),
               ('NonsenseMag', 'mag', float)]


class myNonsenseFileDB(fileDBObject):
    objid = 'fileNonsense'
    tableid = 'test'
    idColKey = 'NonsenseId'
    raColName = 'ra'
    decColName = 'dec'
    columns = [('NonsenseId', 'id', int),
               ('NonsenseRaJ2000', 'ra*%f'%(np.pi/180.)),
               ('NonsenseDecJ2000', 'dec*%f'%(np.pi/180.)),
               ('NonsenseMag', 'mag', float)]


class testCatalogDBObjectTestStars(myTestStars):
    objid = 'testCatalogDBObjectTeststars'
    driver = 'sqlite'


class testCatalogDBObjectTestGalaxies(myTestGals):
    objid = 'testCatalogDBObjectTestgals'
    driver = 'sqlite'


class CatalogDBObjectTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = tempfile.mkdtemp(dir=ROOT,
                                           prefix='CatalogDBObjectTestCase')
        # Delete the test database if it exists and start fresh.
        cls.dbo_db_name = os.path.join(cls.scratch_dir, 'testCatalogDBObjectDatabase.db')
        if os.path.exists(cls.dbo_db_name):
            print("deleting database")
            os.unlink(cls.dbo_db_name)
        tu.makeStarTestDB(filename=cls.dbo_db_name, size=5000, seedVal=1)
        tu.makeGalTestDB(filename=cls.dbo_db_name, size=5000, seedVal=1)

        #Create a database from generic data stored in testData/CatalogsGenerationTestData.txt
        #This will be used to make sure that circle and box spatial bounds yield the points
        #they are supposed to.
        dataDir = os.path.join(ROOT, 'testData')
        cls.nonsense_db_name = os.path.join(cls.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        if os.path.exists(cls.nonsense_db_name):
            os.unlink(cls.nonsense_db_name)

        conn = sqlite3.connect(cls.nonsense_db_name)
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE test (id int, ra real, dec real, mag real)''')
            conn.commit()
        except:
            raise RuntimeError("Error creating database table test.")

        try:
            c.execute('''CREATE TABLE test2 (id int, mag real)''')
            conn.commit()
        except:
            raise RuntimeError("Error creating database table test2.")

        with open(os.path.join(dataDir, 'CatalogsGenerationTestData.txt'), 'r') as inFile:
            for line in inFile:
                values = line.split()
                cmd = '''INSERT INTO test VALUES (%s, %s, %s, %s)''' % \
                      (values[0], values[1], values[2], values[3])
                c.execute(cmd)
                if int(values[0])%2 == 0:
                    cmd = '''INSERT INTO test2 VALUES (%s, %s)''' % (values[0], str(2.0*float(values[3])))
                    c.execute(cmd)

            conn.commit()

        try:
            c.execute('''CREATE TABLE queryColumnsTest (i1 int, i2 int, i3 int)''')
            conn.commit()
        except:
            raise RuntimeError("Error creating database table queryColumnsTest.")

        with open(os.path.join(dataDir, 'QueryColumnsTestData.txt'), 'r') as inputFile:
            for line in inputFile:
                vv = line.split()
                cmd = '''INSERT INTO queryColumnsTest VALUES (%s, %s, %s)''' % (vv[0], vv[1], vv[2])
                c.execute(cmd)

        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        sims_clean_up()
        if os.path.exists(cls.dbo_db_name):
            os.unlink(cls.dbo_db_name)
        if os.path.exists(cls.nonsense_db_name):
            os.unlink(cls.nonsense_db_name)
        if os.path.exists(cls.scratch_dir):
            shutil.rmtree(cls.scratch_dir)

    def setUp(self):
        self.obsMd = ObservationMetaData(pointingRA=210.0, pointingDec=-60.0, boundLength=1.75,
                                         boundType='circle', mjd=52000., bandpassName='r')

        self.filepath = os.path.join(ROOT,
                                     'testData', 'CatalogsGenerationTestData.txt')

        """
        baselineData will store another copy of the data that should be stored in
        testCatalogDBObjectNonsenseDB.db.  This will give us something to test database queries
        against when we ask for all of the objects within a certain box or circle.
        """

        self.dtype = [('id', int), ('ra', float), ('dec', float), ('mag', float)]
        self.baselineData = np.loadtxt(self.filepath, dtype=self.dtype)

    def tearDown(self):
        del self.obsMd
        del self.filepath
        del self.dtype
        del self.baselineData

    def testObsMD(self):
        self.assertEqual(self.obsMd.bandpass, 'r')
        self.assertAlmostEqual(self.obsMd.mjd.TAI, 52000., 6)

    def testDbObj(self):
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        mystars = testCatalogDBObjectTestStars(database=db_name)
        mygals = testCatalogDBObjectTestGalaxies(database=db_name)
        result = mystars.query_columns(obs_metadata=self.obsMd)
        tu.writeResult(result, "/dev/null")
        result = mygals.query_columns(obs_metadata=self.obsMd)
        tu.writeResult(result, "/dev/null")

    def testRealQueryConstraints(self):
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        mystars = testCatalogDBObjectTestStars(database=db_name)
        mycolumns = ['id', 'raJ2000', 'decJ2000', 'umag', 'gmag', 'rmag', 'imag', 'zmag', 'ymag']

        # recall that ra and dec are stored in degrees in the data base
        myquery = mystars.query_columns(colnames = mycolumns,
                                        constraint = 'ra < 90. and ra > 45.')

        tol = 1.0e-3
        ct = 0
        for chunk in myquery:
            for star in chunk:
                ct += 1
                self.assertLess(np.degrees(star[1]), 90.0+tol)
                self.assertGreater(np.degrees(star[1]), 45.0-tol)
        self.assertGreater(ct, 0)

    def testNonsenseCircularConstraints(self):
        """
        Test that a query performed on a circle bound gets all of the objects (and only all
        of the objects) within that circle
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)

        radius = 20.0
        raCenter = 210.0
        decCenter = -60.0

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        circObsMd = ObservationMetaData(boundType='circle', pointingRA=raCenter, pointingDec=decCenter,
                                        boundLength=radius, mjd=52000., bandpassName='r')

        circQuery = myNonsense.query_columns(colnames = mycolumns, obs_metadata=circObsMd, chunk_size=100)

        raCenter = np.radians(raCenter)
        decCenter = np.radians(decCenter)
        radius = np.radians(radius)

        goodPoints = []

        ct = 0
        for chunk in circQuery:
            for row in chunk:
                ct += 1
                distance = haversine(raCenter, decCenter, row[1], row[2])

                self.assertLess(distance, radius)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # store a list of which objects fell within our circle bound
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that all of the points not returned by the query were, in fact, outside of
            # the circle bound
            ct += 1
            distance = haversine(raCenter, decCenter, np.radians(entry[1]), np.radians(entry[2]))
            self.assertGreater(distance, radius)
        self.assertGreater(ct, 0)

    def testNonsenseSelectOnlySomeColumns(self):
        """
        Test a query performed only a subset of the available columns
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseMag']

        query = myNonsense.query_columns(colnames=mycolumns, constraint = 'ra < 45.', chunk_size=100)

        goodPoints = []

        ct = 0
        for chunk in query:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], 45.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[2], 3)

        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            self.assertGreater(entry[1], 45.0)
            ct += 1
        self.assertGreater(ct, 0)

    def testNonsenseBoxConstraints(self):
        """
        Test that a query performed on a box bound gets all of the points (and only all of the
        points) inside that box bound.
        """

        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0

        raCenter = 0.5*(raMin+raMax)
        decCenter = 0.5*(decMin+decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingDec=decCenter, pointingRA=raCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = myNonsense.query_columns(obs_metadata=boxObsMd, chunk_size=100, colnames=mycolumns)

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of which points were returned by teh query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)

        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query are, in fact, outside of the
            # box bound

            switch = (entry[1] > raMax or entry[1] < raMin or entry[2] > decMax or entry[2] < decMin)
            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

    def testNonsenseArbitraryConstraints(self):
        """
        Test a query with a user-specified constraint on the magnitude column
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0
        raCenter = 0.5*(raMin + raMax)
        decCenter = 0.5*(decMin + decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingRA=raCenter, pointingDec=decCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = myNonsense.query_columns(colnames = mycolumns,
                                            obs_metadata=boxObsMd, chunk_size=100,
                                            constraint = 'mag > 11.0')

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1

                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)
                self.assertGreater(row[3], 11.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of the points returned by the query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)

        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query did, in fact, violate one of the
            # constraints of the query (either the box bound or the magnitude cut off)
            switch = (entry[1] > raMax or entry[1] < raMin or
                      entry[2] > decMax or entry[2] < decMin or entry[3] < 11.0)

            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

    def testArbitraryQuery(self):
        """
        Test method to directly execute an arbitrary SQL query (inherited from DBObject class)
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)
        query = 'SELECT test.id, test.mag, test2.id, test2.mag FROM test, test2 WHERE test.id=test2.id'
        results = myNonsense.execute_arbitrary(query)
        self.assertEqual(len(results), 1250)
        for row in results:
            self.assertEqual(row[0], row[2])
            self.assertAlmostEqual(row[1], 0.5*row[3], 6)
        self.assertGreater(len(results), 0)

    def testArbitraryChunkIterator(self):
        """
        Test method to create a ChunkIterator from an arbitrary SQL query (inherited from DBObject class)
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)
        query = 'SELECT test.id, test.mag, test2.id, test2.mag FROM test, test2 WHERE test.id=test2.id'
        dtype = np.dtype([('id1', int), ('mag1', float), ('id2', int), ('mag2', float)])
        results = myNonsense.get_chunk_iterator(query, chunk_size=100, dtype=dtype)
        i = 0
        for chunk in results:
            for row in chunk:
                self.assertEqual(row[0], row[2])
                self.assertAlmostEqual(row[1], 0.5*row[3], 6)
                i += 1
        self.assertEqual(i, 1250)

    def testChunking(self):
        """
        Test that a query with a specified chunk_size does, in fact, return chunks of that size
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        mystars = testCatalogDBObjectTestStars(database=db_name)
        mycolumns = ['id', 'raJ2000', 'decJ2000', 'umag', 'gmag']
        myquery = mystars.query_columns(colnames = mycolumns, chunk_size = 1000)

        ct = 0
        for chunk in myquery:
            self.assertEqual(chunk.size, 1000)
            for row in chunk:
                ct += 1
                self.assertEqual(len(row), 5)
        self.assertGreater(ct, 0)

    def testClassVariables(self):
        """
        Make sure that the daughter classes of CatalogDBObject properly overwrite the member
        variables of CatalogDBObject
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        mystars = testCatalogDBObjectTestStars(database=db_name)
        mygalaxies = testCatalogDBObjectTestGalaxies(database=db_name)

        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense = myNonsenseDB(database=db_name)

        self.assertEqual(mystars.raColName, 'ra')
        self.assertEqual(mystars.decColName, 'decl')
        self.assertEqual(mystars.idColKey, 'id')
        self.assertEqual(mystars.driver, 'sqlite')
        self.assertEqual(mystars.database, self.dbo_db_name)
        self.assertEqual(mystars.appendint, 1023)
        self.assertEqual(mystars.tableid, 'stars')
        self.assertFalse(hasattr(mystars, 'spatialModel'),
                         msg="mystars has attr 'spatialModel', which it should not")
        self.assertEqual(mystars.objid, 'testCatalogDBObjectTeststars')

        self.assertEqual(mygalaxies.raColName, 'ra')
        self.assertEqual(mygalaxies.decColName, 'decl')
        self.assertEqual(mygalaxies.idColKey, 'id')
        self.assertEqual(mygalaxies.driver, 'sqlite')
        self.assertEqual(mygalaxies.database, self.dbo_db_name)
        self.assertEqual(mygalaxies.appendint, 1022)
        self.assertEqual(mygalaxies.tableid, 'galaxies')
        self.assertTrue(hasattr(mygalaxies, 'spatialModel'),
                        msg="mygalaxies does not have attr 'spatialModel', which it should")
        self.assertEqual(mygalaxies.spatialModel, 'SERSIC2D')
        self.assertEqual(mygalaxies.objid, 'testCatalogDBObjectTestgals')

        self.assertEqual(myNonsense.raColName, 'ra')
        self.assertEqual(myNonsense.decColName, 'dec')
        self.assertEqual(myNonsense.idColKey, 'NonsenseId')
        self.assertEqual(myNonsense.driver, 'sqlite')
        self.assertEqual(myNonsense.database, self.nonsense_db_name)
        self.assertFalse(hasattr(myNonsense, 'appendint'),
                         msg="myNonsense has attr 'appendint', which it should not")
        self.assertEqual(myNonsense.tableid, 'test')
        self.assertFalse(hasattr(myNonsense, 'spatialModel'),
                         msg="myNonsense has attr 'spatialModel', which it should not")
        self.assertEqual(myNonsense.objid, 'Nonsense')

        self.assertIn('teststars', CatalogDBObject.registry)
        self.assertIn('testgals', CatalogDBObject.registry)
        self.assertIn('testCatalogDBObjectTeststars', CatalogDBObject.registry)
        self.assertIn('testCatalogDBObjectTestgals', CatalogDBObject.registry)
        self.assertIn('Nonsense', CatalogDBObject.registry)

        colsShouldBe = [('id', None, int), ('raJ2000', 'ra*%f'%(np.pi/180.)),
                        ('decJ2000', 'decl*%f'%(np.pi/180.)),
                        ('parallax', 'parallax*%.15f'%(np.pi/(648000000.0))),
                        ('properMotionRa', 'properMotionRa*%.15f'%(np.pi/180.)),
                        ('properMotionDec', 'properMotionDec*%.15f'%(np.pi/180.)),
                        ('umag', None), ('gmag', None), ('rmag', None), ('imag', None),
                        ('zmag', None), ('ymag', None),
                        ('magNorm', 'mag_norm', float)]

        for (col, coltest) in zip(mystars.columns, colsShouldBe):
            self.assertEqual(col, coltest)

        colsShouldBe = [('NonsenseId', 'id', int),
                        ('NonsenseRaJ2000', 'ra*%f'%(np.pi/180.)),
                        ('NonsenseDecJ2000', 'dec*%f'%(np.pi/180.)),
                        ('NonsenseMag', 'mag', float)]

        for (col, coltest) in zip(myNonsense.columns, colsShouldBe):
            self.assertEqual(col, coltest)

        colsShouldBe = [('id', None, int),
                        ('raJ2000', 'ra*%f' % (np.pi/180.)),
                        ('decJ2000', 'decl*%f'%(np.pi/180.)),
                        ('umag', None),
                        ('gmag', None),
                        ('rmag', None),
                        ('imag', None),
                        ('zmag', None),
                        ('ymag', None),
                        ('magNormAgn', 'mag_norm_agn', None),
                        ('magNormDisk', 'mag_norm_disk', None),
                        ('magNormBulge', 'mag_norm_bulge', None),
                        ('redshift', None),
                        ('a_disk', None),
                        ('b_disk', None),
                        ('a_bulge', None),
                        ('b_bulge', None)]

        for (col, coltest) in zip(mygalaxies.columns, colsShouldBe):
            self.assertEqual(col, coltest)

    def testQueryColumnsDefaults(self):
        """
        Test that dbDefaultValues get properly applied when query_columns is called
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')

        db = dbForQueryColumnsTest(database=db_name,driver='sqlite')
        colnames = ['i1', 'i2', 'i3']
        results = db.query_columns(colnames)
        controlArr = [(1, -1, 2), (3, 4, -2), (5, 6, 7)]

        ct = 0
        for chunk in results:
            for ix, line in enumerate(chunk):
                ct += 1
                self.assertEqual(line[0], controlArr[ix][0])
                self.assertEqual(line[1], controlArr[ix][1])
                self.assertEqual(line[2], controlArr[ix][2])

        self.assertGreater(ct, 0)

    # The tests below all replicate tests above, except with CatalogDBObjects whose
    # connection was passed directly in from the constructor, in order to make sure
    # that passing a connection in works.

    def testNonsenseCircularConstraints_passConnection(self):
        """
        Test that a query performed on a circle bound gets all of the objects (and only all
        of the objects) within that circle.

        Pass connection directly in to the constructor.
        """

        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(database=db_name)

        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)

        radius = 20.0
        raCenter = 210.0
        decCenter = -60.0

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        circObsMd = ObservationMetaData(boundType='circle', pointingRA=raCenter, pointingDec=decCenter,
                                        boundLength=radius, mjd=52000., bandpassName='r')

        circQuery = myNonsense.query_columns(colnames = mycolumns, obs_metadata=circObsMd, chunk_size=100)

        raCenter = np.radians(raCenter)
        decCenter = np.radians(decCenter)
        radius = np.radians(radius)

        goodPoints = []

        ct = 0
        for chunk in circQuery:
            for row in chunk:
                ct += 1
                distance = haversine(raCenter, decCenter, row[1], row[2])

                self.assertLess(distance, radius)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # store a list of which objects fell within our circle bound
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that all of the points not returned by the query were, in fact, outside of
            # the circle bound
            distance = haversine(raCenter, decCenter, np.radians(entry[1]), np.radians(entry[2]))
            self.assertGreater(distance, radius)
            ct += 1
        self.assertGreater(ct, 0)

    def testNonsenseSelectOnlySomeColumns_passConnection(self):
        """
        Test a query performed only a subset of the available columns

        Pass connection directly in to the constructor.
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(database=db_name)

        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseMag']

        query = myNonsense.query_columns(colnames=mycolumns, constraint = 'ra < 45.', chunk_size=100)

        goodPoints = []

        ct = 0
        for chunk in query:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], 45.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[2], 3)

        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            self.assertGreater(entry[1], 45.0)
            ct += 1
        self.assertGreater(ct, 0)

    def testNonsenseBoxConstraints_passConnection(self):
        """
        Test that a query performed on a box bound gets all of the points (and only all of the
        points) inside that box bound.

        Pass connection directly in to the constructor.
        """

        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(database=db_name)
        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0

        raCenter = 0.5*(raMin+raMax)
        decCenter = 0.5*(decMin+decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingDec=decCenter, pointingRA=raCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = myNonsense.query_columns(obs_metadata=boxObsMd, chunk_size=100, colnames=mycolumns)

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of which points were returned by teh query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query are, in fact, outside of the
            # box bound

            switch = (entry[1] > raMax or entry[1] < raMin or entry[2] > decMax or entry[2] < decMin)
            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

    def testNonsenseArbitraryConstraints_passConnection(self):
        """
        Test a query with a user-specified constraint on the magnitude column

        Pass connection directly in to the constructor.
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(database=db_name)
        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0
        raCenter = 0.5*(raMin + raMax)
        decCenter = 0.5*(decMin + decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingRA=raCenter, pointingDec=decCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = myNonsense.query_columns(colnames = mycolumns,
                                            obs_metadata=boxObsMd, chunk_size=100,
                                            constraint = 'mag > 11.0')

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1

                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)
                self.assertGreater(row[3], 11.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of the points returned by the query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query did, in fact, violate one of the
            # constraints of the query (either the box bound or the magnitude cut off)
            switch = (entry[1] > raMax or entry[1] < raMin or
                      entry[2] > decMax or entry[2] < decMin or entry[3] < 11.0)

            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

    def testArbitraryQuery_passConnection(self):
        """
        Test method to directly execute an arbitrary SQL query (inherited from DBObject class)

        Pass connection directly in to the constructor.
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(db_name)
        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)
        query = 'SELECT test.id, test.mag, test2.id, test2.mag FROM test, test2 WHERE test.id=test2.id'
        results = myNonsense.execute_arbitrary(query)
        self.assertEqual(len(results), 1250)
        ct = 0
        for row in results:
            ct += 1
            self.assertEqual(row[0], row[2])
            self.assertAlmostEqual(row[1], 0.5*row[3], 6)
        self.assertGreater(ct, 0)

    def testArbitraryChunkIterator_passConnection(self):
        """
        Test method to create a ChunkIterator from an arbitrary SQL query (inherited from DBObject class)

        Pass connection directly in to the constructor.
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectNonsenseDB.db')
        myNonsense_base = myNonsenseDB(database=db_name)
        myNonsense = myNonsenseDB_noConnection(connection=myNonsense_base.connection)
        query = 'SELECT test.id, test.mag, test2.id, test2.mag FROM test, test2 WHERE test.id=test2.id'
        dtype = np.dtype([('id1', int), ('mag1', float), ('id2', int), ('mag2', float)])
        results = myNonsense.get_chunk_iterator(query, chunk_size=100, dtype=dtype)
        i = 0
        for chunk in results:
            for row in chunk:
                self.assertEqual(row[0], row[2])
                self.assertAlmostEqual(row[1], 0.5*row[3], 6)
                i += 1
        self.assertEqual(i, 1250)

    def testPassingConnectionDifferentTables(self):
        """
        Test that we can pass a DBConnection between DBObjects that connect to different
        tables on the same database
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        dbo1 = testCatalogDBObjectTestStars(database=db_name)
        cols = ['raJ2000', 'decJ2000', 'umag']
        results = dbo1.query_columns(cols)
        ct = 0
        for chunk in results:
            for line in chunk:
                ct += 1
        self.assertGreater(ct, 0)

        ct = 0
        db02 = testCatalogDBObjectTestGalaxies(connection=dbo1.connection)
        cols = ['raJ2000', 'decJ2000', 'redshift']
        results = db02.query_columns(cols)
        for chunk in results:
            for line in chunk:
                ct += 1
        self.assertGreater(ct, 0)

    def test_dtype_detection(self):
        """
        Test that, if we execute different queries on the same CatalogDBObject,
        the dtype is correctly detected
        """
        db_name = os.path.join(self.scratch_dir, 'testCatalogDBObjectDatabase.db')
        db = testCatalogDBObjectTestStars(database=db_name)
        results = db.query_columns(colnames=['ra', 'id', 'varParamStr'], chunk_size=1000)
        n_chunks = 0
        for chunk in results:
            n_chunks += 1
            self.assertGreater(len(chunk), 0)
            self.assertEqual(str(chunk.dtype['ra']), 'float64')
            self.assertEqual(str(chunk.dtype['id']), 'int64')
            if sys.version_info.major == 2:
                self.assertEqual(str(chunk.dtype['varParamStr']), '|S256')
            else:
                self.assertEqual(str(chunk.dtype['varParamStr']), '<U256')
            self.assertEqual(len(chunk.dtype.names), 3)
        self.assertGreater(n_chunks, 0)

        results = db.query_columns(colnames=['ra', 'id', 'ebv'], chunk_size=1000)
        n_chunks = 0
        for chunk in results:
            n_chunks += 1
            self.assertGreater(len(chunk), 0)
            self.assertEqual(str(chunk.dtype['ra']), 'float64')
            self.assertEqual(str(chunk.dtype['id']), 'int64')
            self.assertEqual(str(chunk.dtype['ebv']), 'float64')
            self.assertEqual(len(chunk.dtype.names), 3)
        self.assertGreater(n_chunks, 0)

        # test that running query_columns() after execute_arbitrary()
        # still gives the correct dtype
        cmd = 'SELECT id, ra, varParamStr, umag FROM stars'
        results = db.execute_arbitrary(cmd)
        self.assertGreater(len(results), 0)
        self.assertEqual(str(results.dtype['ra']), 'float64')
        self.assertEqual(str(results.dtype['id']), 'int64')

        # The specific dtype for varParamStr is different from above
        # because, with execute_arbitrary(), the dtype detects the
        # exact length of the string.  With query_columns() it uses
        # a value that is encoded in CatalogDBObject
        if sys.version_info.major == 2:
            self.assertEqual(str(results.dtype['varParamStr']), '|S102')
        else:
            self.assertEqual(str(results.dtype['varParamStr']), '<U101')

        # verify that json can load varParamStr as a dict (indicating that
        # the whole string was loaded properly
        for val in results['varParamStr']:
            test_dict = json.loads(val)
            self.assertIsInstance(test_dict, dict)

        self.assertEqual(str(results.dtype['umag']), 'float64')
        self.assertEqual(len(results.dtype.names), 4)

        results = db.query_columns(colnames=['zmag', 'id', 'rmag'], chunk_size=1000)
        n_chunks = 0
        for chunk in results:
            n_chunks += 1
            self.assertGreater(len(chunk), 0)
            self.assertEqual(str(chunk.dtype['zmag']), 'float64')
            self.assertEqual(str(chunk.dtype['id']), 'int64')
            self.assertEqual(str(chunk.dtype['rmag']), 'float64')
            self.assertEqual(len(chunk.dtype.names), 3)
        self.assertGreater(n_chunks, 0)

        # now try it specifying the dtype
        dtype = np.dtype([('id', int), ('ra', float), ('varParamStr', 'S102'), ('umag', float)])
        cmd = 'SELECT id, ra, varParamStr, umag FROM stars'
        results = db.execute_arbitrary(cmd, dtype=dtype)
        self.assertGreater(len(results), 0)
        self.assertEqual(str(results.dtype['ra']), 'float64')
        self.assertEqual(str(results.dtype['id']), 'int64')

        # The specific dtype for varParamStr is different from above
        # because, with execute_arbitrary(), the dtype detects the
        # exact length of the string.  With query_columns() it uses
        # a value that is encoded in CatalogDBObject
        self.assertEqual(str(results.dtype['varParamStr']), '|S102')
        self.assertEqual(str(results.dtype['umag']), 'float64')
        self.assertEqual(len(results.dtype.names), 4)

        results = db.query_columns(colnames=['zmag', 'id', 'rmag'], chunk_size=1000)
        n_chunks = 0
        for chunk in results:
            n_chunks += 1
            self.assertGreater(len(chunk), 0)
            self.assertEqual(str(chunk.dtype['zmag']), 'float64')
            self.assertEqual(str(chunk.dtype['id']), 'int64')
            self.assertEqual(str(chunk.dtype['rmag']), 'float64')
            self.assertEqual(len(chunk.dtype.names), 3)
        self.assertGreater(n_chunks, 0)


class fileDBObjectTestCase(unittest.TestCase):
    """
    This class will re-implement the tests from CatalogDBObjectTestCase,
    except that it will use a Nonsense CatalogDBObject loaded with fileDBObject
    to make sure that fileDBObject properly loads the file into a
    database.
    """

    @classmethod
    def tearDownClass(self):
        sims_clean_up()

    def setUp(self):
        self.testDataFile = os.path.join(
            ROOT, 'testData', 'CatalogsGenerationTestData.txt')
        self.testHeaderFile = os.path.join(
            ROOT, 'testData', 'CatalogsGenerationTestDataHeader.txt')

        self.myNonsense = fileDBObject.from_objid('fileNonsense', self.testDataFile,
                                                  dtype = np.dtype([('id', int), ('ra', float),
                                                                    ('dec', float), ('mag', float)]),
                                                  skipLines = 0)
        # note that skipLines defaults to 1 so, if you do not include this, you will
        # lose the first line of your input file (which maybe you want to do if that
        # is a header)

        self.myNonsenseHeader = fileDBObject.from_objid('fileNonsense', self.testHeaderFile)
        # this time, make fileDBObject learn the dtype from a header

        """
        baselineData will store another copy of the data that should be stored in
        testCatalogDBObjectNonsenseDB.db.  This will give us something to test database queries
        against when we ask for all of the objects within a certain box or circle bound
        """
        self.dtype = [('id', int), ('ra', float), ('dec', float), ('mag', float)]
        self.baselineData = np.loadtxt(self.testDataFile, dtype=self.dtype)

    def tearDown(self):
        del self.testDataFile
        del self.testHeaderFile
        del self.myNonsense
        del self.myNonsenseHeader
        del self.dtype
        del self.baselineData

    def testDatabaseName(self):
        self.assertEqual(self.myNonsense.database, ':memory:')

    def testNonsenseCircularConstraints(self):
        """
        Test that a query performed on a circle bound gets all of the objects (and only all
        of the objects) within that circle
        """

        radius = 20.0
        raCenter = 210.0
        decCenter = -60.0

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        circObsMd = ObservationMetaData(boundType='circle', pointingRA=raCenter, pointingDec=decCenter,
                                        boundLength=radius, mjd=52000., bandpassName='r')

        circQuery = self.myNonsense.query_columns(colnames = mycolumns, obs_metadata=circObsMd,
                                                  chunk_size=100)

        raCenter = np.radians(raCenter)
        decCenter = np.radians(decCenter)
        radius = np.radians(radius)

        goodPoints = []

        ct = 0
        for chunk in circQuery:
            for row in chunk:
                ct += 1
                distance = haversine(raCenter, decCenter, row[1], row[2])

                self.assertLess(distance, radius)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # store a list of which objects fell within our circle bound
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that all of the points not returned by the query were, in fact, outside of
            # the circle bound
            distance = haversine(raCenter, decCenter, np.radians(entry[1]), np.radians(entry[2]))
            self.assertGreater(distance, radius)
            ct += 1
        self.assertGreater(ct, 0)

        # make sure that the CatalogDBObject which used a header gets the same result
        headerQuery = self.myNonsenseHeader.query_columns(colnames = mycolumns,
                                                          obs_metadata=circObsMd,
                                                          chunk_size=100)
        goodPointsHeader = []
        for chunk in headerQuery:
            for row in chunk:
                distance = haversine(raCenter, decCenter, row[1], row[2])
                dex = np.where(self.baselineData['id'] == row[0])[0][0]
                goodPointsHeader.append(row[0])
                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)

        self.assertEqual(len(goodPoints), len(goodPointsHeader))
        for xx in goodPoints:
            self.assertIn(xx, goodPointsHeader)

    def testNonsenseSelectOnlySomeColumns(self):
        """
        Test a query performed only a subset of the available columns
        """

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseMag']

        query = self.myNonsense.query_columns(colnames=mycolumns, constraint = 'ra < 45.', chunk_size=100)

        goodPoints = []

        ct = 0
        for chunk in query:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], 45.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[2], 3)

        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            self.assertGreater(entry[1], 45.0)
            ct += 1
        self.assertGreater(ct, 0)

        headerQuery = self.myNonsenseHeader.query_columns(colnames=mycolumns,
                                                          constraint = 'ra < 45.',
                                                          chunk_size=100)
        goodPointsHeader = []
        for chunk in headerQuery:
            for row in chunk:
                dex = np.where(self.baselineData['id'] == row[0])[0][0]
                goodPointsHeader.append(row[0])
                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[2], 3)

        self.assertEqual(len(goodPoints), len(goodPointsHeader))
        for xx in goodPoints:
            self.assertIn(xx, goodPointsHeader)

    def testNonsenseBoxConstraints(self):
        """
        Test that a query performed on a box bound gets all of the points (and only all of the
        points) inside that box bound.
        """

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0
        raCenter = 0.5*(raMin + raMax)
        decCenter = 0.5*(decMin + decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingRA=raCenter, pointingDec=decCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = self.myNonsense.query_columns(obs_metadata=boxObsMd, chunk_size=100, colnames=mycolumns)

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1
                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of which points were returned by teh query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query are, in fact, outside of the
            # box bound

            switch = (entry[1] > raMax or entry[1] < raMin or entry[2] > decMax or entry[2] < decMin)
            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

        headerQuery = self.myNonsenseHeader.query_columns(obs_metadata=boxObsMd,
                                                          chunk_size=100, colnames=mycolumns)
        goodPointsHeader = []
        for chunk in headerQuery:
            for row in chunk:
                dex = np.where(self.baselineData['id'] == row[0])[0][0]
                goodPointsHeader.append(row[0])
                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)

        self.assertEqual(len(goodPoints), len(goodPointsHeader))
        for xx in goodPoints:
            self.assertIn(xx, goodPointsHeader)

    def testNonsenseArbitraryConstraints(self):
        """
        Test a query with a user-specified constraint on the magnitude column
        """

        raMin = 50.0
        raMax = 150.0
        decMax = 30.0
        decMin = -20.0
        raCenter = 0.5*(raMin + raMax)
        decCenter = 0.5*(decMin + decMax)

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']

        boxObsMd = ObservationMetaData(boundType='box', pointingRA=raCenter, pointingDec=decCenter,
                                       boundLength=np.array([0.5*(raMax-raMin), 0.5*(decMax-decMin)]),
                                       mjd=52000., bandpassName='r')

        boxQuery = self.myNonsense.query_columns(colnames = mycolumns,
                                                 obs_metadata=boxObsMd, chunk_size=100,
                                                 constraint = 'mag > 11.0')

        raMin = np.radians(raMin)
        raMax = np.radians(raMax)
        decMin = np.radians(decMin)
        decMax = np.radians(decMax)

        goodPoints = []

        ct = 0
        for chunk in boxQuery:
            for row in chunk:
                ct += 1

                self.assertLess(row[1], raMax)
                self.assertGreater(row[1], raMin)
                self.assertLess(row[2], decMax)
                self.assertGreater(row[2], decMin)
                self.assertGreater(row[3], 11.0)

                dex = np.where(self.baselineData['id'] == row[0])[0][0]

                # keep a list of the points returned by the query
                goodPoints.append(row[0])

                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)
        self.assertGreater(ct, 0)

        ct = 0
        for entry in [xx for xx in self.baselineData if xx[0] not in goodPoints]:
            # make sure that the points not returned by the query did, in fact, violate one of the
            # constraints of the query (either the box bound or the magnitude cut off)
            switch = (entry[1] > raMax or entry[1] < raMin or
                      entry[2] > decMax or entry[2] < decMin or
                      entry[3] < 11.0)

            self.assertTrue(switch, msg='query failed to find a star that was within bounds')
            ct += 1
        self.assertGreater(ct, 0)

        headerQuery = self.myNonsenseHeader.query_columns(colnames = mycolumns,
                                                          obs_metadata=boxObsMd, chunk_size=100,
                                                          constraint='mag > 11.0')
        goodPointsHeader = []
        for chunk in headerQuery:
            for row in chunk:
                dex = np.where(self.baselineData['id'] == row[0])[0][0]
                goodPointsHeader.append(row[0])
                self.assertAlmostEqual(np.radians(self.baselineData['ra'][dex]), row[1], 3)
                self.assertAlmostEqual(np.radians(self.baselineData['dec'][dex]), row[2], 3)
                self.assertAlmostEqual(self.baselineData['mag'][dex], row[3], 3)

        self.assertEqual(len(goodPoints), len(goodPointsHeader))
        for xx in goodPoints:
            self.assertIn(xx, goodPointsHeader)

    def testChunking(self):
        """
        Test that a query with a specified chunk_size does, in fact, return chunks of that size
        """

        mycolumns = ['NonsenseId', 'NonsenseRaJ2000', 'NonsenseDecJ2000', 'NonsenseMag']
        myquery = self.myNonsense.query_columns(colnames = mycolumns, chunk_size = 100)

        ct = 0
        for chunk in myquery:
            self.assertEqual(chunk.size, 100)
            for row in chunk:
                ct += 1
                self.assertEqual(len(row), 4)
        self.assertGreater(ct, 0)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    setup_module(None)
    unittest.main()
