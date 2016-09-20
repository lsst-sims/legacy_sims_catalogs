from __future__ import with_statement
import os
import numpy as np
import sqlite3
import unittest
import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.utils import ObservationMetaData
from lsst.sims.catalogs.db import CatalogDBObject
from lsst.sims.catalogs.utils import myTestStars, makeStarTestDB
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.utils import Site


def setup_module(module):
    lsst.utils.tests.init()


def createCannotBeNullTestDB(filename=None, add_nans=True):
    """
    Create a database to test the 'cannot_be_null' functionality in InstanceCatalog

    This method will return the contents of the database as a recarray for baseline comparison
    in the unit tests.
    """

    if filename is None:
        dbName = 'cannotBeNullTest.db'
    else:
        dbName = filename

    rng = np.random.RandomState(32)
    dtype = np.dtype([('id', int), ('n1', np.float64), ('n2', np.float64), ('n3', np.float64),
                      ('n4', (str, 40)), ('n5', (unicode, 40))])
    output = None

    if os.path.exists(dbName):
        os.unlink(dbName)

    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE testTable (id int, n1 float, n2 float, n3 float, n4 text, n5 text)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")

    for ii in range(100):

        values = rng.random_sample(3)
        for i in range(len(values)):
            draw = rng.random_sample(1)
            if draw[0] < 0.5 and add_nans:
                values[i] = None

        draw = rng.random_sample(1)
        if draw[0] < 0.5:
            w1 = 'None'
        else:
            w1 = 'word'

        draw = rng.random_sample(1)
        if draw[0] < 0.5:
            w2 = unicode('None')
        else:
            w2 = unicode('word')

        if output is None:
            output = np.array([(ii, values[0], values[1], values[2], w1, w2)], dtype=dtype)
        else:
            size = output.size
            output.resize(size+1)
            output[size] = (ii, values[0], values[1], values[2], w1, w2)

        if np.isnan(values[0]) and add_nans:
            v0 = 'NULL'
        else:
            v0 = str(values[0])

        if np.isnan(values[1]) and add_nans:
            v1 = 'NULL'
        else:
            v1 = str(values[1])

        if np.isnan(values[2]) and add_nans:
            v2 = 'NULL'
        else:
            v2 = str(values[2])

        cmd = '''INSERT INTO testTable VALUES (%s, %s, %s, %s, '%s', '%s')''' % (ii, v0, v1, v2, w1, w2)
        c.execute(cmd)

    conn.commit()
    conn.close()
    return output


class myCannotBeNullDBObject(CatalogDBObject):
    driver = 'sqlite'
    database = 'cannotBeNullTest.db'
    tableid = 'testTable'
    objid = 'cannotBeNull'
    idColKey = 'id'
    columns = [('n5', 'n5', unicode, 40)]


class floatCannotBeNullCatalog(InstanceCatalog):
    """
    This catalog class will not write rows with a null value in the n2 column
    """
    column_outputs = ['id', 'n1', 'n2', 'n3', 'n4', 'n5']
    cannot_be_null = ['n2']


class strCannotBeNullCatalog(InstanceCatalog):
    """
    This catalog class will not write rows with a null value in the n2 column
    """
    column_outputs = ['id', 'n1', 'n2', 'n3', 'n4', 'n5']
    cannot_be_null = ['n4']


class unicodeCannotBeNullCatalog(InstanceCatalog):
    """
    This catalog class will not write rows with a null value in the n2 column
    """
    column_outputs = ['id', 'n1', 'n2', 'n3', 'n4', 'n5']
    cannot_be_null = ['n5']


class severalCannotBeNullCatalog(InstanceCatalog):
    """
    This catalog class will not write rows with null values in the n2 or n4 columns
    """
    column_outputs = ['id', 'n1', 'n2', 'n3', 'n4', 'n5']
    cannot_be_null = ['n2', 'n4']


class CanBeNullCatalog(InstanceCatalog):
    """
    This catalog class will write all rows to the catalog
    """
    column_outputs = ['id', 'n1', 'n2', 'n3', 'n4', 'n5']
    catalog_type = 'canBeNull'


class testStellarCatalogClass(InstanceCatalog):
    column_outputs = ['raJ2000', 'decJ2000']
    default_formats = {'f': '%le'}


class cartoonValueCatalog(InstanceCatalog):
    column_outputs = ['n1', 'n2']
    default_formats = {'f': '%le'}

    def get_difference(self):
        x = self.column_by_name('n1')
        y = self.column_by_name('n3')
        return x-y


class InstanceCatalogMetaDataTest(unittest.TestCase):
    """
    This class will test how Instance catalog handles the metadata
    class variables (pointingRA, pointingDec, etc.)
    """

    @classmethod
    def setUpClass(cls):
        if os.path.exists('testInstanceCatalogDatabase.db'):
            os.unlink('testInstanceCatalogDatabase.db')

        makeStarTestDB(filename='testInstanceCatalogDatabase.db')

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('testInstanceCatalogDatabase.db'):
            os.unlink('testInstanceCatalogDatabase.db')

    def setUp(self):
        self.myDB = myTestStars(driver='sqlite', database='testInstanceCatalogDatabase.db')

    def tearDown(self):
        del self.myDB

    def testObsMetaDataAssignment(self):
        """
        Test that you get an error when you pass something that is not
        ObservationMetaData as obs_metadata
        """

        xx = 5.0
        self.assertRaises(ValueError, testStellarCatalogClass, self.myDB, obs_metadata=xx)

    def testColumnArg(self):
        """
        A unit test to make sure that the code allowing you to add
        new column_outputs to an InstanceCatalog using its constructor
        works properly.
        """
        scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')

        mjd = 5120.0
        RA = 1.5
        Dec = -1.1
        rotSkyPos = -10.0

        testSite = Site(longitude=2.0, latitude=-1.0, height=4.0,
                        temperature=100.0, pressure=500.0, humidity=0.1,
                        lapseRate=0.1)

        testObsMD = ObservationMetaData(site=testSite,
                                        mjd=mjd, pointingRA=RA,
                                        pointingDec=Dec,
                                        rotSkyPos=rotSkyPos,
                                        bandpassName = 'z')

        # make sure the correct column names are returned
        # according to class definition
        testCat = testStellarCatalogClass(self.myDB, obs_metadata=testObsMD)
        columnsShouldBe = ['raJ2000', 'decJ2000']
        for col in testCat.iter_column_names():
            if col in columnsShouldBe:
                columnsShouldBe.remove(col)
            else:
                raise(RuntimeError, 'column %s returned; should not be there' % col)

        self.assertEqual(len(columnsShouldBe), 0)

        # make sure that new column names can be added
        newColumns = ['properMotionRa', 'properMotionDec']
        testCat = testStellarCatalogClass(self.myDB, obs_metadata=testObsMD, column_outputs=newColumns)
        columnsShouldBe = ['raJ2000', 'decJ2000', 'properMotionRa', 'properMotionDec']
        for col in testCat.iter_column_names():
            if col in columnsShouldBe:
                columnsShouldBe.remove(col)
            else:
                raise(RuntimeError, 'column %s returned; should not be there' % col)

        self.assertEqual(len(columnsShouldBe), 0)

        # make sure that, if we include a duplicate column in newColumns,
        # the column is not duplicated
        newColumns = ['properMotionRa', 'properMotionDec', 'raJ2000']
        testCat = testStellarCatalogClass(self.myDB, obs_metadata=testObsMD, column_outputs=newColumns)
        columnsShouldBe = ['raJ2000', 'decJ2000', 'properMotionRa', 'properMotionDec']

        for col in columnsShouldBe:
            self.assertIn(col, testCat._actually_calculated_columns)

        generatedColumns = []
        for col in testCat.iter_column_names():
            generatedColumns.append(col)
            if col in columnsShouldBe:
                columnsShouldBe.remove(col)
            else:
                raise(RuntimeError, 'column %s returned; should not be there' % col)

        self.assertEqual(len(columnsShouldBe), 0)
        self.assertEqual(len(generatedColumns), 4)

        cat_name = os.path.join(scratch_dir, 'testArgCatalog.txt')
        testCat.write_catalog(cat_name)
        with open(cat_name, 'r') as inCat:
            lines = inCat.readlines()
        header = lines[0]
        header = header.strip('#')
        header = header.strip('\n')
        header = header.split(', ')
        self.assertIn('raJ2000', header)
        self.assertIn('decJ2000', header)
        self.assertIn('properMotionRa', header)
        self.assertIn('properMotionDec', header)
        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def testArgValues(self):
        """
        Test that columns added using the contructor ags return the correct value
        """
        dbName = 'valueTestDB.db'
        baselineData = createCannotBeNullTestDB(filename=dbName, add_nans=False)
        db = myCannotBeNullDBObject(driver='sqlite', database=dbName)
        dtype = np.dtype([('n1', float), ('n2', float), ('n3', float), ('difference', float)])
        cat = cartoonValueCatalog(db, column_outputs = ['n3', 'difference'])

        columns = ['n1', 'n2', 'n3', 'difference']
        for col in columns:
            self.assertIn(col, cat._actually_calculated_columns)

        scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')
        cat_name = os.path.join(scratch_dir, 'cartoonValCat.txt')
        cat.write_catalog(cat_name)
        testData = np.genfromtxt(cat_name, dtype=dtype, delimiter=',')
        for testLine, controlLine in zip(testData, baselineData):
            self.assertAlmostEqual(testLine[0], controlLine['n1'], 6)
            self.assertAlmostEqual(testLine[1], controlLine['n2'], 6)
            self.assertAlmostEqual(testLine[2], controlLine['n3'], 6)
            self.assertAlmostEqual(testLine[3], controlLine['n1']-controlLine['n3'], 6)

        if os.path.exists(dbName):
            os.unlink(dbName)
        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def testAllCalculatedColumns(self):
        """
        Unit test to make sure that _actually_calculated_columns contains all of the dependent columns
        """
        class otherCartoonValueCatalog(InstanceCatalog):
            column_outputs = ['n1', 'n2', 'difference']

            def get_difference(self):
                n1 = self.column_by_name('n1')
                n3 = self.column_by_name('n3')
                return n1-n3

        dbName = 'valueTestDB.db'
        createCannotBeNullTestDB(filename=dbName, add_nans=False)
        db = myCannotBeNullDBObject(driver='sqlite', database=dbName)
        cat = otherCartoonValueCatalog(db)
        columns = ['n1', 'n2', 'n3', 'difference']
        for col in columns:
            self.assertIn(col, cat._actually_calculated_columns)

        if os.path.exists('valueTestDB.db'):
            os.unlink('valueTestDB.db')


class InstanceCatalogCannotBeNullTest(unittest.TestCase):

        def setUp(self):
            self.baselineOutput = createCannotBeNullTestDB()

        def tearDown(self):
            del self.baselineOutput
            if os.path.exists('cannotBeNullTest.db'):
                os.unlink('cannotBeNullTest.db')

        def testCannotBeNull(self):
            """
            Test to make sure that the code for filtering out rows with null values
            in key rows works.
            """

            scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')

            # each of these classes flags a different column with a different datatype as cannot_be_null
            availableCatalogs = [floatCannotBeNullCatalog, strCannotBeNullCatalog, unicodeCannotBeNullCatalog]
            dbobj = CatalogDBObject.from_objid('cannotBeNull')

            for catClass in availableCatalogs:
                cat = catClass(dbobj)
                fileName = os.path.join(scratch_dir, 'cannotBeNullTestFile.txt')
                cat.write_catalog(fileName)
                dtype = np.dtype([('id', int), ('n1', np.float64), ('n2', np.float64), ('n3', np.float64),
                                  ('n4', (str, 40)), ('n5', (unicode, 40))])
                testData = np.genfromtxt(fileName, dtype=dtype, delimiter=',')

                ct_good = 0  # a counter to keep track of the rows read in from the catalog
                ct_total = len(self.baselineOutput)

                for i in range(len(self.baselineOutput)):

                    # self.baselineOutput contains all of the rows from the dbobj
                    # first, we must assess whether or not the row we are currently
                    # testing would, in fact, pass the cannot_be_null test
                    validLine = True
                    if (isinstance(self.baselineOutput[cat.cannot_be_null[0]][i], str) or
                        isinstance(self.baselineOutput[cat.cannot_be_null[0]][i], unicode)):

                        if self.baselineOutput[cat.cannot_be_null[0]][i].strip().lower() == 'none':
                            validLine = False
                    else:
                        if np.isnan(self.baselineOutput[cat.cannot_be_null[0]][i]):
                            validLine = False

                    if validLine:
                        # if the row in self.baslineOutput should be in the catalog, we now check
                        # that baseline and testData agree on column values (there are some gymnastics
                        # here because you cannot do an == on NaN's
                        for (k, xx) in enumerate(self.baselineOutput[i]):
                            if k < 4:
                                if not np.isnan(xx):
                                    msg = ('k: %d -- %s %s -- %s' %
                                           (k, str(xx), str(testData[ct_good][k]), cat.cannot_be_null))
                                    self.assertAlmostEqual(xx, testData[ct_good][k], 3, msg=msg)
                                else:
                                    np.testing.assert_equal(testData[ct_good][k], np.NaN)
                            else:
                                msg = ('%s (%s) is not %s (%s)' %
                                       (xx, type(xx), testData[ct_good][k], type(testData[ct_good][k])))
                                self.assertEqual(xx.strip(), testData[ct_good][k].strip(), msg=msg)
                        ct_good += 1

                self.assertEqual(ct_good, len(testData))  # make sure that we tested all of the testData rows
                msg = '%d >= %d' % (ct_good, ct_total)
                self.assertLess(ct_good, ct_total, msg=msg)  # make sure that some rows did not make it into the catalog

            if os.path.exists(fileName):
                os.unlink(fileName)

        def testCannotBeNull_pre_screen(self):
            """
            Check that writing a catalog with self._pre_screen = True produces
            the same results as writing one with self._pre_creen = False, except
            with a smaller self._current_chunk.
            """

            scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')

            # each of these classes flags a different column with a different datatype as cannot_be_null
            availableCatalogs = [floatCannotBeNullCatalog, strCannotBeNullCatalog, unicodeCannotBeNullCatalog,
                                 severalCannotBeNullCatalog]
            dbobj = CatalogDBObject.from_objid('cannotBeNull')

            for catClass in availableCatalogs:
                cat = catClass(dbobj)
                cat._pre_screen = True
                control_cat = catClass(dbobj)
                fileName = os.path.join(scratch_dir, 'cannotBeNullTestFile_prescreen.txt')
                control_fileName = os.path.join(scratch_dir, 'cannotBeNullTestFile_prescreen_control.txt')
                cat.write_catalog(fileName)
                control_cat.write_catalog(control_fileName)

                # make sure that pre-screened catalog passed fewer rows into
                # self._current_chunk than did the non-pre-screened catalog
                self.assertGreater(control_cat._current_chunk.size, cat._current_chunk.size)

                with open(fileName, 'r') as test_file:
                    test_lines = test_file.readlines()
                    with open(control_fileName, 'r') as control_file:
                        control_lines = control_file.readlines()
                        for line in control_lines:
                            self.assertIn(line, test_lines)
                        for line in test_lines:
                            self.assertIn(line, control_lines)

                if os.path.exists(fileName):
                    os.unlink(fileName)
                if os.path.exists(control_fileName):
                    os.unlink(control_fileName)

        def testCanBeNull(self):
            """
            Test to make sure that we can still write all rows to catalogs,
            even those with null values in key columns
            """
            scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')
            dbobj = CatalogDBObject.from_objid('cannotBeNull')
            cat = dbobj.getCatalog('canBeNull')
            fileName = os.path.join(scratch_dir, 'canBeNullTestFile.txt')
            cat.write_catalog(fileName)
            dtype = np.dtype([('id', int), ('n1', np.float64), ('n2', np.float64), ('n3', np.float64),
                              ('n4', (str, 40)), ('n5', (unicode, 40))])
            testData = np.genfromtxt(fileName, dtype=dtype, delimiter=',')

            for i in range(len(self.baselineOutput)):
                # make sure that all of the rows in self.baselineOutput are represented in
                # testData
                for (k, xx) in enumerate(self.baselineOutput[i]):
                    if k < 4:
                        if not np.isnan(xx):
                            self.assertAlmostEqual(xx, testData[i][k], 3)
                        else:
                            np.testing.assert_equal(testData[i][k], np.NaN)
                    else:
                        msg = '%s is not %s' % (xx, testData[i][k])
                        self.assertEqual(xx.strip(), testData[i][k].strip(), msg=msg)

            self.assertEqual(i, 99)
            self.assertEqual(len(testData), len(self.baselineOutput))

            if os.path.exists(fileName):
                os.unlink(fileName)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
