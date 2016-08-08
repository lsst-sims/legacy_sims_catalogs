from __future__ import with_statement
import unittest
import numpy
import os
import lsst.utils.tests as utilsTests
from lsst.utils import getPackageDir

from lsst.sims.utils import ObservationMetaData
from lsst.sims.catalogs.generation.db import fileDBObject, \
                                      CompoundCatalogDBObject, \
                                      CatalogDBObject

class dbClass1(CatalogDBObject):
    objid = 'class1'
    idColKey = 'id'
    tableid = 'test'
    columns = [('aa', 'a'),
               ('bb', 'd', str, 20)]

    dbDefaultValues = {'ee': -1}


class dbClass2(CatalogDBObject):
    objid = 'class2'
    idColKey = 'id'
    tableid = 'test'
    columns = [('aa', '2.0*b'),
               ('bb', 'a')]

    dbDefaultValues = {'ee': -2}


class dbClass3(CatalogDBObject):
    objid = 'class3'
    idColKey = 'id'
    tableid = 'test'
    columns = [('aa', 'c-3.0'),
               ('bb', 'a'),
               ('cc', '3.0*b')]

    dbDefaultValues = {'ee': -3}


class dbClass4(CatalogDBObject):
    objid = 'class4'
    idColKey = 'id'
    tableid = 'otherTest'
    columns = [('aa', 'c-3.0'),
               ('bb', 'a'),
               ('cc', '3.0*b')]

    dbDefaultValues = {'ee': -3}


class dbClass5(CatalogDBObject):
    objid = 'class4'
    idColKey = 'id'
    tableid = 'otherTest'
    columns = [('aa', 'c-3.0'),
               ('bb', 'a'),
               ('cc', '3.0*b')]

    dbDefaultValues = {'ee': -3}


class dbClass6(CatalogDBObject):
    objid = 'class6'
    idColKey = 'id'
    tableid = 'test'
    columns = [('a', None),
               ('b',None)]


class specificCompoundObj_otherTest(CompoundCatalogDBObject):
    _table_restriction = ['otherTest']


class specificCompoundObj_test(CompoundCatalogDBObject):
    _table_restriction = ['test']


class universalCompoundObj(CompoundCatalogDBObject):
    _table_restriction = ['test', 'otherTest']


class CompoundCatalogDBObjectTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        numpy.random.seed(42)
        dtype = numpy.dtype([
                            ('a', numpy.float),
                            ('b', numpy.float),
                            ('c', numpy.float),
                            ('d', str, 20)
                            ])

        nSamples=100
        aList = numpy.random.random_sample(nSamples)*10.0
        bList = numpy.random.random_sample(nSamples)*(-1.0)
        cList = numpy.random.random_sample(nSamples)*10.0-5.0
        ww = 'a'
        dList = []
        for ix in range(nSamples):
            ww += 'b'
            dList.append(ww)

        cls.controlArray = numpy.rec.fromrecords([
                                                 (aa, bb, cc, dd) \
                                                 for aa, bb, cc ,dd in \
                                                 zip(aList, bList, cList, dList)
                                                 ], dtype=dtype)


        baseDir = os.path.join(getPackageDir('sims_catalogs_generation'),
                               'tests', 'scratchSpace')

        cls.textFileName = os.path.join(baseDir, 'compound_test_data.txt')
        if os.path.exists(cls.textFileName):
            os.unlink(cls.textFileName)

        with open(cls.textFileName, 'w') as output:
            output.write('# id a b c d\n')
            for ix, (aa, bb, cc, dd) in enumerate(zip(aList, bList, cList, dList)):
                output.write('%d %e %e %e %s\n' % (ix, aa, bb, cc, dd))



        cls.dbName =  os.path.join(baseDir, 'compoundCatalogTestDB.db')
        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)

        cls.otherDbName = os.path.join(baseDir, 'otherDb.db')
        if os.path.exists(cls.otherDbName):
            os.unlink(cls.otherDbName)

        dtype = numpy.dtype([
                            ('id', numpy.int),
                            ('a', numpy.float),
                            ('b', numpy.float),
                            ('c', numpy.float),
                            ('d', str, 20)
                            ])

        fdbo = fileDBObject(cls.textFileName, runtable='test',
                            database=cls.dbName, dtype=dtype,
                            idColKey='id')

        fdbo = fileDBObject(cls.textFileName, runtable='test',
                            database=cls.otherDbName, dtype=dtype,
                            idColKey='id')

        fdbo = fileDBObject(cls.textFileName, runtable='otherTest',
                            database=cls.dbName, dtype=dtype,
                            idColKey='id')

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.textFileName):
            os.unlink(cls.textFileName)
        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)
        if os.path.exists(cls.otherDbName):
            os.unlink(cls.otherDbName)


    def testExceptions(self):
        """
        Verify that CompoundCatalogDBObject raises an exception
        when you violate its API
        """

        # test case where they are querying the same table, but different
        # databases
        class testDbClass1(dbClass1):
            database = self.otherDbName
            driver = 'sqlite'

        class testDbClass2(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass1()
        db2 = testDbClass2()

        with self.assertRaises(RuntimeError) as context:
            compound = CompoundCatalogDBObject([db1, db2])

        self.assertTrue("['%s', '%s']" % (self.otherDbName, self.dbName) \
                        in context.exception.message)

        # test case where they are querying the same database, but different
        # tables

        class testDbClass3(dbClass4):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass4(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        with self.assertRaises(RuntimeError) as context:
            compound = CompoundCatalogDBObject([testDbClass3, testDbClass4])

        self.assertTrue("['otherTest', 'test']" in context.exception.message)

        # test case where the CatalogDBObjects have the same objid
        class testDbClass5(dbClass4):
            database = self.dbName
            driver = 'sqlite'
            objid = 'dummy'

        class testDbClass6(dbClass5):
            database = self.dbName
            driver = 'sqlite'
            objid = 'dummy'

        with self.assertRaises(RuntimeError) as context:
            compound = CompoundCatalogDBObject([testDbClass5, testDbClass6])

        self.assertTrue("objid dummy is duplicated" in context.exception.message)

        # test case where CompoundCatalogDBObject does not support the
        # tables being queried
        class testDbClass7(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass8(dbClass2):
            database = self.dbName
            driver ='sqlite'

        with self.assertRaises(RuntimeError) as context:
            compound = specificCompoundObj_otherTest([testDbClass7, testDbClass8])

        msg = "This CompoundCatalogDBObject does not support the table 'test'"
        self.assertTrue(msg in context.exception.message)



    def testCompoundCatalogDBObject(self):
        """
        Verify that CompoundCatalogDBObject returns the expected
        columns.
        """

        class testDbClass9(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass10(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass11(dbClass3):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass9
        db2 = testDbClass10
        db3 = testDbClass11

        dbList = [db1, db2, db3]
        compoundDb = CompoundCatalogDBObject(dbList)

        colNames = ['%s_aa' % db1.objid, '%s_bb' % db1.objid,
                    '%s_aa' % db2.objid, '%s_bb' % db2.objid,
                    '%s_aa' % db3.objid, '%s_bb' % db3.objid,
                    '%s_cc' % db3.objid]

        results = compoundDb.query_columns(colnames=colNames)

        for chunk in results:
            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db1.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_equal(chunk['%s_bb' % db1.objid],
                                             self.controlArray['d'])

            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db2.objid],
                                                    2.0*self.controlArray['b'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db2.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db3.objid],
                                                    self.controlArray['c']-3.0,
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db3.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_cc' % db3.objid],
                                                    3.0*self.controlArray['b'],
                                                    decimal=6)

    def testTableRestriction(self):
        """
        Verify that _table_restriction works the way it should in CompoundCatalogDBObject
        """

        class testDbClass12(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass13(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass12
        db2 = testDbClass13
        dbList = [db1, db2]
        compoundDb = specificCompoundObj_test(dbList)

        colNames = ['%s_aa' % db1.objid, '%s_bb' % db1.objid,
                    '%s_aa' % db2.objid, '%s_bb' % db2.objid]

        results = compoundDb.query_columns(colnames=colNames)

        for chunk in results:
            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db1.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_equal(chunk['%s_bb' % db1.objid],
                                             self.controlArray['d'])

            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db2.objid],
                                                    2.0*self.controlArray['b'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db2.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)


    def testUniversalTableRestriction(self):
        """
        Verify that _table_restriction with multiple tables also works
        """

        class testDbClass14(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass15(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass16(dbClass3):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass14
        db2 = testDbClass15
        db3 = testDbClass16
        dbList = [db1, db2, db3]
        compoundDb = universalCompoundObj(dbList)

        colNames = ['%s_aa' % db1.objid, '%s_bb' % db1.objid,
                    '%s_aa' % db2.objid, '%s_bb' % db2.objid,
                    '%s_aa' % db3.objid, '%s_bb' % db3.objid,
                    '%s_cc' % db3.objid]

        results = compoundDb.query_columns(colnames=colNames)

        for chunk in results:
            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db1.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_equal(chunk['%s_bb' % db1.objid],
                                             self.controlArray['d'])

            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db2.objid],
                                                    2.0*self.controlArray['b'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db2.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db3.objid],
                                                    self.controlArray['c']-3.0,
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db3.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_cc' % db3.objid],
                                                    3.0*self.controlArray['b'],
                                                    decimal=6)



    def testChunks(self):
        """
        Verify that CompoundCatalogDBObject handles chunk_size correctly
        """

        class testDbClass17(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass18(dbClass2):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass19(dbClass3):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass17
        db2 = testDbClass18
        db3 = testDbClass19
        dbList = [db1, db2, db3]
        compoundDb = CompoundCatalogDBObject(dbList)

        colNames = ['id',
                    '%s_aa' % db1.objid, '%s_bb' % db1.objid,
                    '%s_aa' % db2.objid, '%s_bb' % db2.objid,
                    '%s_aa' % db3.objid, '%s_bb' % db3.objid,
                    '%s_cc' % db3.objid]

        results = compoundDb.query_columns(colnames=colNames, chunk_size=10)

        ct = 0

        for chunk in results:
            ct += len(chunk['%s_aa' % db1.objid])
            rows = chunk['id']
            self.assertTrue(len(rows)<=10)

            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db1.objid],
                                                    self.controlArray['a'][rows],
                                                    decimal=6)

            numpy.testing.assert_array_equal(chunk['%s_bb' % db1.objid],
                                             self.controlArray['d'][rows])

            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db2.objid],
                                                    2.0*self.controlArray['b'][rows],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db2.objid],
                                                    self.controlArray['a'][rows],
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db3.objid],
                                                    self.controlArray['c'][rows]-3.0,
                                                    decimal=6)


            numpy.testing.assert_array_almost_equal(chunk['%s_bb' % db3.objid],
                                                    self.controlArray['a'][rows],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_cc' % db3.objid],
                                                    3.0*self.controlArray['b'][rows],
                                                    decimal=6)

        self.assertEqual(ct, 100)


    def testNoneMapping(self):
        """
        Test that Nones are handled correctly in the CatalogDBObject
        column mappings
        """

        class testDbClass20(dbClass1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass21(dbClass6):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass20
        db2 = testDbClass21
        colNames = ['%s_aa' % db1.objid, '%s_bb' % db1.objid,
                    '%s_a' % db2.objid, '%s_b' % db2.objid]

        compoundDb = CompoundCatalogDBObject([db1, db2])
        results = compoundDb.query_columns(colnames=colNames)

        for chunk in results:
            numpy.testing.assert_array_almost_equal(chunk['%s_aa' % db1.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_equal(chunk['%s_bb' % db1.objid],
                                             self.controlArray['d'])

            numpy.testing.assert_array_almost_equal(chunk['%s_a' % db2.objid],
                                                    self.controlArray['a'],
                                                    decimal=6)

            numpy.testing.assert_array_almost_equal(chunk['%s_b' % db2.objid],
                                                    self.controlArray['b'],
                                                    decimal=6)


class testStarDB1(CatalogDBObject):
    tableid = 'test'
    raColName = 'ra'
    decColName = 'dec'
    idColKey = 'id'
    objid = 'testStar1'
    columns = [('id', None),
               ('raJ2000', 'ra'),
               ('decJ2000', 'dec'),
               ('magMod', 'mag')]


class testStarDB2(CatalogDBObject):
    tableid = 'test'
    raColName = 'ra'
    decColName = 'dec'
    idColKey = 'id'
    objid = 'testStar2'
    columns = [('id', None),
               ('raJ2000', '2.0*ra'),
               ('decJ2000', '2.0*dec'),
               ('magMod', '2.0*mag')]


class CompoundWithObsMetaData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.baseDir = os.path.join(getPackageDir('sims_catalogs_generation'),
                                   'tests', 'scratchSpace')


        cls.textFileName = os.path.join(cls.baseDir, 'compound_obs_metadata_text_data.txt')

        numpy.random.seed(42)
        nSamples = 100
        raList = numpy.random.random_sample(nSamples)*360.0
        decList = numpy.random.random_sample(nSamples)*180.0 - 90.0
        magList = numpy.random.random_sample(nSamples)*15.0 + 7.0

        dtype = numpy.dtype([
                            ('ra', numpy.float),
                            ('dec', numpy.float),
                            ('mag', numpy.float)
                            ])

        cls.controlArray = numpy.rec.fromrecords([
                                                  (r, d, m) \
                                                  for r, d, m in \
                                                  zip(raList, decList, magList)
                                                  ], dtype=dtype)

        dbDtype = numpy.dtype([
                              ('id', numpy.int),
                              ('ra', numpy.float),
                              ('dec', numpy.float),
                              ('mag', numpy.float)
                              ])

        if os.path.exists(cls.textFileName):
            os.unlink(cls.textFileName)

        with open(cls.textFileName, 'w') as output:
            output.write('# id ra dec mag\n')
            for ix, (r, d, m) in enumerate(zip(raList, decList, magList)):
                output.write('%d %.20f %.20f %.20f\n' % (ix, r, d, m))

        cls.dbName = os.path.join(cls.baseDir, 'compound_obs_metadata_db.db')

        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)

        fdbo = fileDBObject(cls.textFileName, runtable='test',
                            database=cls.dbName, dtype=dbDtype,
                            idColKey='id')

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.textFileName):
            os.unlink(cls.textFileName)

        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)


    def testObsMetaData(self):
        """
        Test that CompoundCatalogDBObject can handle ObservationMetaData
        properly
        """

        obs = ObservationMetaData(pointingRA = 180.0,
                                  pointingDec = 0.0,
                                  boundType = 'box',
                                  boundLength = (80.0, 25.0),
                                  mjd=53580.0)


        class testDbClass22(testStarDB1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass23(testStarDB2):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass22
        db2 = testDbClass23

        compoundDb = CompoundCatalogDBObject([db1, db2])

        colnames = ['%s_id' % db1.objid,
                    '%s_raJ2000' % db1.objid, '%s_decJ2000' % db1.objid,
                    '%s_magMod' % db1.objid,
                    '%s_raJ2000' % db2.objid, '%s_decJ2000' % db2.objid,
                    '%s_magMod' % db2.objid]

        results = compoundDb.query_columns(colnames=colnames,
                                           obs_metadata=obs)

        good_rows = []
        for chunk in results:
            for line in chunk:
                ix = line['id']
                good_rows.append(ix)
                self.assertAlmostEqual(line['%s_raJ2000' % db1.objid], self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db1.objid], self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db1.objid], self.controlArray['mag'][ix], 10)
                self.assertAlmostEqual(line['%s_raJ2000' % db2.objid], 2.0*self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db2.objid], 2.0*self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db2.objid], 2.0*self.controlArray['mag'][ix], 10)
                self.assertTrue(self.controlArray['ra'][ix]>100.0)
                self.assertTrue(self.controlArray['ra'][ix]<260.0)
                self.assertTrue(self.controlArray['dec'][ix]>-25.0)
                self.assertTrue(self.controlArray['dec'][ix]<25.0)

        bad_rows = [ix for ix in range(self.controlArray.shape[0]) if ix not in good_rows]

        in_bounds = [rr>100.0 and rr<260.0 and dd>-25.0 and dd<25.0 \
                      for (rr, dd) in \
                      zip(self.controlArray['ra'][bad_rows], self.controlArray['dec'][bad_rows])]

        self.assertFalse(True in in_bounds)
        self.assertTrue(len(good_rows)>0)
        self.assertTrue(len(bad_rows)>0)


    def testContraint(self):
        """
        Test that CompoundCatalogDBObject runs correctly with a constraint
        """

        class testDbClass24(testStarDB1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass25(testStarDB2):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass24
        db2 = testDbClass25

        compoundDb = CompoundCatalogDBObject([db1, db2])

        colnames = ['%s_id' % db1.objid,
                    '%s_raJ2000' % db1.objid, '%s_decJ2000' % db1.objid,
                    '%s_magMod' % db1.objid,
                    '%s_raJ2000' % db2.objid, '%s_decJ2000' % db2.objid,
                    '%s_magMod' % db2.objid]

        results = compoundDb.query_columns(colnames=colnames,
                                           constraint='mag<11.0')

        good_rows = []
        for chunk in results:
            for line in chunk:
                ix = line['id']
                good_rows.append(ix)
                self.assertAlmostEqual(line['%s_raJ2000' % db1.objid], self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db1.objid], self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db1.objid], self.controlArray['mag'][ix], 10)
                self.assertAlmostEqual(line['%s_raJ2000' % db2.objid], 2.0*self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db2.objid], 2.0*self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db2.objid], 2.0*self.controlArray['mag'][ix], 10)
                self.assertTrue(self.controlArray['mag'][ix]<11.0)


        bad_rows = [ix for ix in range(self.controlArray.shape[0]) if ix not in good_rows]

        in_bounds = [mm<11.0 for mm in self.controlArray['mag'][bad_rows]]

        self.assertFalse(True in in_bounds)
        self.assertTrue(len(good_rows)>0)
        self.assertTrue(len(bad_rows)>0)
        self.assertEqual(len(good_rows)+len(bad_rows), self.controlArray.shape[0])


    def testObsMetadataAndConstraint(self):
        """
        Test that CompoundCatalogDBObject correctly handles an ObservationMetaData
        and a constraint at the same time
        """
        obs = ObservationMetaData(pointingRA = 180.0,
                                  pointingDec = 0.0,
                                  boundType = 'box',
                                  boundLength = (80.0, 25.0),
                                  mjd=53580.0)

        class testDbClass26(testStarDB1):
            database = self.dbName
            driver = 'sqlite'

        class testDbClass27(testStarDB2):
            database = self.dbName
            driver = 'sqlite'

        db1 = testDbClass26
        db2 = testDbClass27

        compoundDb = CompoundCatalogDBObject([db1, db2])

        colnames = ['%s_id' % db1.objid,
                    '%s_raJ2000' % db1.objid, '%s_decJ2000' % db1.objid,
                    '%s_magMod' % db1.objid,
                    '%s_raJ2000' % db2.objid, '%s_decJ2000' % db2.objid,
                    '%s_magMod' % db2.objid]

        results = compoundDb.query_columns(colnames=colnames,
                                           obs_metadata=obs,
                                           constraint='mag>15.0')

        good_rows = []
        for chunk in results:
            for line in chunk:
                ix = line['id']
                good_rows.append(ix)
                self.assertAlmostEqual(line['%s_raJ2000' % db1.objid], self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db1.objid], self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db1.objid], self.controlArray['mag'][ix], 10)
                self.assertAlmostEqual(line['%s_raJ2000' % db2.objid], 2.0*self.controlArray['ra'][ix], 10)
                self.assertAlmostEqual(line['%s_decJ2000' % db2.objid], 2.0*self.controlArray['dec'][ix], 10)
                self.assertAlmostEqual(line['%s_magMod' % db2.objid], 2.0*self.controlArray['mag'][ix], 10)
                self.assertTrue(self.controlArray['ra'][ix]>100.0)
                self.assertTrue(self.controlArray['ra'][ix]<260.0)
                self.assertTrue(self.controlArray['dec'][ix]>-25.0)
                self.assertTrue(self.controlArray['dec'][ix]<25.0)
                self.assertTrue(self.controlArray['mag'][ix]>15.0)

        bad_rows = [ix for ix in range(self.controlArray.shape[0]) if ix not in good_rows]

        in_bounds = [rr>100.0 and rr<260.0 and dd>-25.0 and dd<25.0 and mm>150.0 \
                      for (rr, dd, mm) in \
                      zip(self.controlArray['ra'][bad_rows], self.controlArray['dec'][bad_rows], \
                          self.controlArray['mag'][bad_rows])]

        self.assertFalse(True in in_bounds)
        self.assertTrue(len(good_rows)>0)
        self.assertTrue(len(bad_rows)>0)
        self.assertEqual(len(good_rows)+len(bad_rows), self.controlArray.shape[0])



def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(CompoundCatalogDBObjectTestCase)
    suites += unittest.makeSuite(CompoundWithObsMetaData)


    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
