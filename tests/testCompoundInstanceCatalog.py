from __future__ import with_statement
from builtins import zip
from builtins import range
from builtins import object
from builtins import super
import os
import numpy as np
import unittest
import tempfile
import shutil
import lsst.utils.tests
from lsst.sims.utils.CodeUtilities import sims_clean_up
from lsst.sims.utils import ObservationMetaData
from lsst.sims.catalogs.db import fileDBObject, CatalogDBObject, CompoundCatalogDBObject
from lsst.sims.catalogs.definitions import InstanceCatalog, CompoundInstanceCatalog

ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class negativeRaCompound(CompoundCatalogDBObject):

    def _final_pass(self, results):
        for name in results.dtype.fields:
            if 'raJ2000' in name:
                results[name] *= -1.0

        return results


class negativeDecCompound_table2(CompoundCatalogDBObject):

    _table_restriction = ['table2']

    def _final_pass(self, results):
        for name in results.dtype.fields:
            if 'decJ2000' in name:
                results[name] *= -1.0

        return results


class cartoonDBbase(object):
    driver = 'sqlite'
    database = None


class table1DB1(CatalogDBObject, cartoonDBbase):
    tableid = 'table1'
    objid = 'table1DB1'
    idColKey = 'id'
    raColName = 'ra'
    decColName = 'dec'

    columns = [('raJ2000', 'ra'),
               ('decJ2000', 'dec'),
               ('mag', None, np.float),
               ('dmag', None, np.float),
               ('dra', None, np.float),
               ('ddec', None, np.float)]


class table1DB2(CatalogDBObject, cartoonDBbase):
    tableid = 'table1'
    objid = 'table1DB2'
    idColKey = 'id'
    raColName = 'ra'
    decColName = 'dec'

    columns = [('raJ2000', '2.0*ra'),
               ('decJ2000', '2.0*dec'),
               ('mag', None, np.float),
               ('dmag', None, np.float),
               ('dra', None, np.float),
               ('ddec', None, np.float)]


class table2DB1(CatalogDBObject, cartoonDBbase):
    tableid = 'table2'
    objid = 'table2DB1'
    idColKey = 'id'
    raColName = 'ra'
    decColName = 'dec'

    columns = [('raJ2000', 'ra'),
               ('decJ2000', 'dec'),
               ('mag', None, np.float)]


class table2DB2(CatalogDBObject, cartoonDBbase):
    tableid = 'table2'
    objid = 'table2DB2'
    idColKey = 'id'
    raColName = 'ra'
    decColName = 'dec'

    columns = [('raJ2000', '2.0*ra'),
               ('decJ2000', '2.0*dec'),
               ('mag', None, np.float)]


class Cat1(InstanceCatalog):
    delimiter = ' '
    default_formats = {'f': '%.12f'}
    column_outputs = ['testId', 'raObs', 'decObs', 'final_mag']

    def get_testId(self):
        return self.column_by_name('id')+1000

    def get_raObs(self):
        return self.column_by_name('raJ2000')

    def get_decObs(self):
        return self.column_by_name('decJ2000')

    def get_final_mag(self):
        return self.column_by_name('mag') + self.column_by_name('dmag')


class Cat2(Cat1):

    def get_testId(self):
        return self.column_by_name('id')+2000

    def get_raObs(self):
        return self.column_by_name('raJ2000') + self.column_by_name('dra')

    def get_decObs(self):
        return self.column_by_name('decJ2000') + self.column_by_name('ddec')


class Cat3(Cat1):

    def get_testId(self):
        return self.column_by_name('id')+3000

    def get_final_mag(self):
        return self.column_by_name('mag')


class Cat4(Cat3):

    def get_testId(self):
        return self.column_by_name('id')+4000


class CompoundCatalogTest(unittest.TestCase):

    longMessage = True

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = tempfile.mkdtemp(dir=ROOT, prefix="CompoundCatalogTest")

        cls.table1FileName = os.path.join(cls.scratch_dir, 'compound_table1.txt')
        cls.table2FileName = os.path.join(cls.scratch_dir, 'compound_table2.txt')

        if os.path.exists(cls.table1FileName):
            os.unlink(cls.table1FileName)
        if os.path.exists(cls.table2FileName):
            os.unlink(cls.table2FileName)

        dtype1 = np.dtype([('ra', np.float),
                           ('dec', np.float),
                           ('mag', np.float),
                           ('dmag', np.float),
                           ('dra', np.float),
                           ('ddec', np.float)])

        dbDtype1 = np.dtype([('id', np.int),
                             ('ra', np.float),
                             ('dec', np.float),
                             ('mag', np.float),
                             ('dmag', np.float),
                             ('dra', np.float),
                             ('ddec', np.float)])

        nPts = 100
        np.random.seed(42)
        raList = np.random.random_sample(nPts)*360.0
        decList = np.random.random_sample(nPts)*180.0-90.0
        magList = np.random.random_sample(nPts)*10.0+15.0
        dmagList = np.random.random_sample(nPts)*10.0 - 5.0
        draList = np.random.random_sample(nPts)*5.0 - 2.5
        ddecList = np.random.random_sample(nPts)*(-2.0) - 4.0

        cls.table1Control = np.rec.fromrecords([(r, d, mm, dm, dr, dd)
                                                for r, d, mm, dm, dr, dd
                                                in zip(raList, decList,
                                                       magList, dmagList,
                                                       draList, ddecList)],
                                               dtype=dtype1)

        with open(cls.table1FileName, 'w') as output:
            output.write("# id ra dec mag dmag dra ddec\n")
            for ix, (r, d, mm, dm, dr, dd) in \
                enumerate(zip(raList, decList, magList, dmagList, draList, ddecList)):

                output.write('%d %.12f %.12f %.12f %.12f %.12f %.12f\n'
                             % (ix, r, d, mm, dm, dr, dd))

        dtype2 = np.dtype([('ra', np.float),
                           ('dec', np.float),
                           ('mag', np.float)])

        dbDtype2 = np.dtype([('id', np.int),
                             ('ra', np.float),
                             ('dec', np.float),
                             ('mag', np.float)])

        ra2List = np.random.random_sample(nPts)*360.0
        dec2List = np.random.random_sample(nPts)*180.0-90.0
        mag2List = np.random.random_sample(nPts)*10+18.0

        cls.table2Control = np.rec.fromrecords([(r, d, m)
                                                for r, d, m in
                                                zip(ra2List, dec2List, mag2List)], dtype=dtype2)

        with open(cls.table2FileName, 'w') as output:
            output.write('# id ra dec mag\n')
            for ix, (r, d, m) in enumerate(zip(ra2List, dec2List, mag2List)):
                output.write('%d %.12f %.12f %.12f\n' % (ix, r, d, m))

        cls.dbName = os.path.join(cls.scratch_dir, 'compound_db.db')
        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)

        fileDBObject(cls.table1FileName, runtable='table1',
                     database=cls.dbName, dtype=dbDtype1,
                     idColKey='id')

        fileDBObject(cls.table2FileName, runtable='table2',
                     database=cls.dbName, dtype=dbDtype2,
                     idColKey='id')

    @classmethod
    def tearDownClass(cls):
        sims_clean_up()
        if os.path.exists(cls.table1FileName):
            os.unlink(cls.table1FileName)
        if os.path.exists(cls.table2FileName):
            os.unlink(cls.table2FileName)
        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)
        if os.path.exists(cls.scratch_dir):
            shutil.rmtree(cls.scratch_dir)

    def setUp(self):
        table1DB1.database = self.dbName
        table1DB2.database = self.dbName
        table2DB1.database = self.dbName
        table2DB2.database = self.dbName

    def tearDown(self):
        table1DB1.database = None
        table1DB2.database = None
        table2DB1.database = None
        table2DB2.database = None

    def testCompoundCatalog(self):
        """
        Test that a CompoundInstanceCatalog produces the expected output
        """
        fileName = os.path.join(self.scratch_dir, 'simplest_compound_catalog.txt')

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1])

        compoundCat.write_catalog(fileName)

        self.assertEqual(len(compoundCat._dbObjectGroupList), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[0]), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[1]), 1)
        self.assertIn(0, compoundCat._dbObjectGroupList[0])
        self.assertIn(1, compoundCat._dbObjectGroupList[0])
        self.assertIn(2, compoundCat._dbObjectGroupList[1])

        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        for line in testData:
            if line[0] < 2000:
                ix = line[0]-1000
                self.assertAlmostEqual(line[1], self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 3000:
                ix = line[0] - 2000
                self.assertAlmostEqual(2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            else:
                ix = line[0]-3000
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testSharedVariables(self):
        """
        Test that, if I set a transformations dict in the CompoundInstanceCatalog, that
        gets propagated to all of the InstanceCatalogs inside it.
        """
        fileName = os.path.join(self.scratch_dir, 'transformed_compound_catalog.txt')

        class TransformedCompoundCatalog(CompoundInstanceCatalog):
            transformations = {'raObs': np.degrees, 'decObs': np.degrees}

        compoundCat = TransformedCompoundCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1])

        compoundCat.write_catalog(fileName)

        self.assertEqual(len(compoundCat._dbObjectGroupList), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[0]), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[1]), 1)
        self.assertIn(0, compoundCat._dbObjectGroupList[0])
        self.assertIn(1, compoundCat._dbObjectGroupList[0])
        self.assertIn(2, compoundCat._dbObjectGroupList[1])

        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        for line in testData:
            if line[0] < 2000:
                ix = line[0]-1000
                self.assertAlmostEqual(line[1], np.degrees(self.table1Control['ra'][ix]), 6)
                self.assertAlmostEqual(line[2], np.degrees(self.table1Control['dec'][ix]), 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 3000:
                ix = line[0] - 2000
                self.assertAlmostEqual(line[1],
                                       np.degrees(2.0*self.table1Control['ra'][ix] +
                                                  self.table1Control['dra'][ix]), 6)
                self.assertAlmostEqual(line[2],
                                       np.degrees(2.0*self.table1Control['dec'][ix] +
                                                  self.table1Control['ddec'][ix]), 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix] + self.table1Control['dmag'][ix],
                                       line[3], 6)
            else:
                ix = line[0]-3000
                self.assertAlmostEqual(line[1], np.degrees(self.table2Control['ra'][ix]), 6)
                self.assertAlmostEqual(line[2], np.degrees(self.table2Control['dec'][ix]), 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testObservationMetaData(self):
        """
        Test that CompoundInstanceCatalog handles ObservationMetaData
        properly
        """
        fileName = os.path.join(self.scratch_dir, 'compound_obs_metadata_test_cat.txt')
        obs = ObservationMetaData(pointingRA = 180.0,
                                  pointingDec = 0.0,
                                  boundType = 'box',
                                  boundLength = (80.0, 25.0),
                                  mjd=53850.0)

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1],
                                              obs_metadata=obs)

        compoundCat.write_catalog(fileName)
        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        table1_good_rows = []
        table2_good_rows = []
        for line in testData:
            if line[0] < 2000:
                ix = line[0] - 1000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['ra'][ix], 100.0)
                self.assertLess(self.table1Control['ra'][ix], 260.0)
                self.assertGreater(self.table1Control['dec'][ix], -25.0)
                self.assertLess(self.table1Control['dec'][ix], 25.0)
            elif line[0] < 3000:
                ix = line[0] - 2000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['ra'][ix], 100.0)
                self.assertLess(self.table1Control['ra'][ix], 260.0)
                self.assertGreater(self.table1Control['dec'][ix], -25.0)
                self.assertLess(self.table1Control['dec'][ix], 25.0)
            else:
                ix = line[0]-3000
                if ix not in table2_good_rows:
                    table2_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)
                self.assertGreater(self.table2Control['ra'][ix], 100.0)
                self.assertLess(self.table2Control['ra'][ix], 260.0)
                self.assertGreater(self.table2Control['dec'][ix], -25.0)
                self.assertLess(self.table2Control['dec'][ix], 25.0)

        table1_bad_rows = [ii for ii in range(self.table1Control.shape[0]) if ii not in table1_good_rows]
        table2_bad_rows = [ii for ii in range(self.table2Control.shape[0]) if ii not in table2_good_rows]

        in_bounds = [rr > 100.0 and rr < 260.0 and dd > -25.0 and dd < 25.0
                     for rr, dd in zip(self.table1Control['ra'][table1_bad_rows],
                                       self.table1Control['dec'][table1_bad_rows])]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table1_bad_rows')

        in_bounds = [rr > 100.0 and rr < 260.0 and dd > -25.0 and dd < 25.0
                     for rr, dd in zip(self.table2Control['ra'][table2_bad_rows],
                                       self.table2Control['dec'][table2_bad_rows])]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table2_bad_rows')

        self.assertGreater(len(table1_good_rows), 0)
        self.assertGreater(len(table2_good_rows), 0)
        self.assertGreater(len(table1_bad_rows), 0)
        self.assertGreater(len(table2_bad_rows), 0)
        self.assertEqual(len(table1_good_rows)+len(table1_bad_rows), self.table1Control.shape[0])
        self.assertEqual(len(table2_good_rows)+len(table2_bad_rows), self.table2Control.shape[0])

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testConstraint(self):
        """
        Test that CompoundInstanceCatalog handles constraint
        properly
        """
        fileName = os.path.join(self.scratch_dir, 'compound_constraint_test_cat.txt')

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1],
                                              constraint='mag>20.0')

        compoundCat.write_catalog(fileName)
        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        table1_good_rows = []
        table2_good_rows = []
        for line in testData:
            if line[0] < 2000:
                ix = line[0]-1000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['mag'][ix], 20.0)
            elif line[0] < 3000:
                ix = line[0] - 2000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['mag'][ix], 20.0)
            else:
                ix = line[0]-3000
                if ix not in table2_good_rows:
                    table2_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)
                self.assertGreater(self.table2Control['mag'][ix], 20.0)

        table1_bad_rows = [ii for ii in range(self.table1Control.shape[0]) if ii not in table1_good_rows]
        table2_bad_rows = [ii for ii in range(self.table2Control.shape[0]) if ii not in table2_good_rows]

        in_bounds = [mm > 20.0 for mm in self.table1Control['mag'][table1_bad_rows]]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table1_bad_rows')

        in_bounds = [mm > 20.0 for mm in self.table2Control['mag'][table2_bad_rows]]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table2_bad_rows')

        self.assertGreater(len(table1_good_rows), 0)
        self.assertGreater(len(table2_good_rows), 0)
        self.assertGreater(len(table1_bad_rows), 0)
        self.assertGreater(len(table2_bad_rows), 0)
        self.assertEqual(len(table1_good_rows)+len(table1_bad_rows), self.table1Control.shape[0])
        self.assertEqual(len(table2_good_rows)+len(table2_bad_rows), self.table2Control.shape[0])

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testObservationMetaDataAndConstraint(self):
        """
        Test that CompoundInstanceCatalog handles ObservationMetaData
        and a constraint properly
        """
        fileName = os.path.join(self.scratch_dir, 'compound_obs_metadata_test_cat.txt')
        obs = ObservationMetaData(pointingRA = 180.0,
                                  pointingDec = 0.0,
                                  boundType = 'box',
                                  boundLength = (80.0, 25.0),
                                  mjd=53850.0)

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1],
                                              obs_metadata=obs,
                                              constraint='mag>20.0')

        compoundCat.write_catalog(fileName)
        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        table1_good_rows = []
        table2_good_rows = []
        for line in testData:
            if line[0] < 2000:
                ix = line[0] - 1000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['ra'][ix], 100.0)
                self.assertLess(self.table1Control['ra'][ix], 260.0)
                self.assertGreater(self.table1Control['dec'][ix], -25.0)
                self.assertLess(self.table1Control['dec'][ix], 25.0)
                self.assertGreater(self.table1Control['mag'][ix], 20.0)
            elif line[0] < 3000:
                ix = line[0] - 2000
                if ix not in table1_good_rows:
                    table1_good_rows.append(ix)
                self.assertAlmostEqual(2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
                self.assertGreater(self.table1Control['ra'][ix], 100.0)
                self.assertLess(self.table1Control['ra'][ix], 260.0)
                self.assertGreater(self.table1Control['dec'][ix], -25.0)
                self.assertLess(self.table1Control['dec'][ix], 25.0)
                self.assertGreater(self.table1Control['mag'][ix], 20.0)
            else:
                ix = line[0]-3000
                if ix not in table2_good_rows:
                    table2_good_rows.append(ix)
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)
                self.assertGreater(self.table2Control['ra'][ix], 100.0)
                self.assertLess(self.table2Control['ra'][ix], 260.0)
                self.assertGreater(self.table2Control['dec'][ix], -25.0)
                self.assertLess(self.table2Control['dec'][ix], 25.0)
                self.assertGreater(self.table2Control['mag'][ix], 20.0)

        table1_bad_rows = [ii for ii in range(self.table1Control.shape[0]) if ii not in table1_good_rows]
        table2_bad_rows = [ii for ii in range(self.table2Control.shape[0]) if ii not in table2_good_rows]

        in_bounds = [rr > 100.0 and rr < 260.0 and dd > -25.0 and dd < 25.0 and mm > 20.0
                     for rr, dd, mm in zip(self.table1Control['ra'][table1_bad_rows],
                                           self.table1Control['dec'][table1_bad_rows],
                                           self.table1Control['mag'][table1_bad_rows])]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table1_bad_rows')

        in_bounds = [rr > 100.0 and rr < 260.0 and dd > -25.0 and dd < 25.0 and mm > 20.0
                     for rr, dd, mm in zip(self.table2Control['ra'][table2_bad_rows],
                                           self.table2Control['dec'][table2_bad_rows],
                                           self.table2Control['mag'][table2_bad_rows])]

        self.assertNotIn(True, in_bounds, msg='failed to assemble table2_bad_rows')

        self.assertGreater(len(table1_good_rows), 0)
        self.assertGreater(len(table2_good_rows), 0)
        self.assertGreater(len(table1_bad_rows), 0)
        self.assertGreater(len(table2_bad_rows), 0)
        self.assertEqual(len(table1_good_rows)+len(table1_bad_rows), self.table1Control.shape[0])
        self.assertEqual(len(table2_good_rows)+len(table2_bad_rows), self.table2Control.shape[0])

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testCustomCompoundCatalogDBObject(self):
        """
        Test that CompoundInstanceCatalog behaves properly when passed a
        custom CompoundCatalogDBObject
        """
        fileName = os.path.join(self.scratch_dir, 'simplest_compound_catalog.txt')

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3], [table1DB1, table1DB2, table2DB1],
                                              compoundDBclass=negativeRaCompound)

        compoundCat.write_catalog(fileName)

        self.assertEqual(len(compoundCat._dbObjectGroupList), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[0]), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[1]), 1)
        self.assertIn(0, compoundCat._dbObjectGroupList[0])
        self.assertIn(1, compoundCat._dbObjectGroupList[0])
        self.assertIn(2, compoundCat._dbObjectGroupList[1])

        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        for line in testData:
            if line[0] < 2000:
                ix = line[0] - 1000
                self.assertAlmostEqual(line[1], -1.0*self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 3000:
                ix = line[0]-2000
                self.assertAlmostEqual(-2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            else:
                ix = line[0]-3000
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testDefaultCustomCompoundCatalogDBObject(self):
        """
        Test that CompoundInstanceCatalog can properly parse multiple CompoundCatalogDBobjects
        """
        fileName = os.path.join(self.scratch_dir, 'simplest_compound_catalog.txt')

        # negativeDecComopound_table2 should not come into play, since the
        # multiple queries are directed at table1
        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3],
                                              [table1DB1, table1DB2, table2DB1],
                                              compoundDBclass=[negativeDecCompound_table2,
                                                               negativeRaCompound])

        compoundCat.write_catalog(fileName)

        self.assertEqual(len(compoundCat._dbObjectGroupList), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[0]), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[1]), 1)
        self.assertIn(0, compoundCat._dbObjectGroupList[0])
        self.assertIn(1, compoundCat._dbObjectGroupList[0])
        self.assertIn(2, compoundCat._dbObjectGroupList[1])

        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        for line in testData:
            if line[0] < 2000:
                ix = line[0]-1000
                self.assertAlmostEqual(line[1], -1.0*self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 3000:
                ix = line[0]-2000
                self.assertAlmostEqual(-2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            else:
                ix = line[0]-3000
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)

        if os.path.exists(fileName):
            os.unlink(fileName)

    def testCustomCompoundCatalogDBObjectList(self):
        """
        Test that CompoundInstanceCatalog behaves properly when there are
        two sets of multiple queries, one to table1, one to table2
        """
        fileName = os.path.join(self.scratch_dir, 'simplest_compound_catalog.txt')

        compoundCat = CompoundInstanceCatalog([Cat1, Cat2, Cat3, Cat4],
                                              [table1DB1, table1DB2, table2DB1, table2DB2],
                                              compoundDBclass=[negativeRaCompound,
                                                               negativeDecCompound_table2])

        compoundCat.write_catalog(fileName)

        self.assertEqual(len(compoundCat._dbObjectGroupList), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[0]), 2)
        self.assertEqual(len(compoundCat._dbObjectGroupList[1]), 2)
        self.assertIn(0, compoundCat._dbObjectGroupList[0])
        self.assertIn(1, compoundCat._dbObjectGroupList[0])
        self.assertIn(2, compoundCat._dbObjectGroupList[1])
        self.assertIn(3, compoundCat._dbObjectGroupList[1])

        dtype = np.dtype([('id', np.int),
                          ('raObs', np.float),
                          ('decObs', np.float),
                          ('final_mag', np.float)])

        testData = np.genfromtxt(fileName, dtype=dtype)

        for line in testData:
            if line[0] < 2000:
                ix = line[0]-1000
                self.assertAlmostEqual(line[1], -1.0*self.table1Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], self.table1Control['dec'][ix], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 3000:
                ix = line[0] - 2000
                self.assertAlmostEqual(-2.0*self.table1Control['ra'][ix]+self.table1Control['dra'][ix],
                                       line[1], 6)
                self.assertAlmostEqual(2.0*self.table1Control['dec'][ix]+self.table1Control['ddec'][ix],
                                       line[2], 6)
                self.assertAlmostEqual(self.table1Control['mag'][ix]+self.table1Control['dmag'][ix],
                                       line[3], 6)
            elif line[0] < 4000:
                ix = line[0] - 3000
                self.assertAlmostEqual(line[1], self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], -1.0*self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)
            else:
                ix = line[0] - 4000
                self.assertAlmostEqual(line[1], 2.0*self.table2Control['ra'][ix], 6)
                self.assertAlmostEqual(line[2], -2.0*self.table2Control['dec'][ix], 6)
                self.assertAlmostEqual(line[3], self.table2Control['mag'][ix], 6)

        if os.path.exists(fileName):
            os.unlink(fileName)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    setup_module(None)
    unittest.main()
