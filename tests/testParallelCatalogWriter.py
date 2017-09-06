from __future__ import with_statement
from builtins import range
from builtins import super
import unittest
import sqlite3
import os
import numpy as np
import tempfile
import shutil

import lsst.utils.tests
from lsst.sims.utils.CodeUtilities import sims_clean_up
from lsst.sims.catalogs.definitions import parallelCatalogWriter
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.catalogs.decorators import compound, cached
from lsst.sims.catalogs.db import CatalogDBObject


ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class DbClass(CatalogDBObject):
    tableid = 'test'

    host = None
    port = None
    driver = 'sqlite'
    objid = 'parallel_writer_test_db'
    idColKey = 'id'


class ParallelCatClass1(InstanceCatalog):
    column_outputs = ['id', 'test1', 'ii']
    cannot_be_null = ['valid1']

    @compound('test1', 'valid1')
    def get_values(self):
        ii = self.column_by_name('ii')
        return np.array([self.column_by_name('id')**2,
                         np.where(ii%2 == 1, ii, None)])


class ParallelCatClass2(InstanceCatalog):
    column_outputs = ['id', 'test2', 'ii']
    cannot_be_null = ['valid2']

    @compound('test2', 'valid2')
    def get_values(self):
        ii = self.column_by_name('id')
        return np.array([self.column_by_name('id')**3,
                         np.where(ii%2 == 1, ii, None)])


class ParallelCatClass3(InstanceCatalog):
    column_outputs = ['id', 'test3', 'ii']
    cannot_be_null = ['valid3']

    @cached
    def get_test3(self):
        return self.column_by_name('id')**4

    @cached
    def get_valid3(self):
        ii = self.column_by_name('id')
        return np.where(ii%5 == 0, ii, None)


class ControlCatalog(InstanceCatalog):
    column_outputs = ['id', 'ii']


class ParallelWriterTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = tempfile.mkdtemp(dir=ROOT, prefix="ParallelWriterTestCase")

        cls.db_name = os.path.join(cls.scratch_dir, 'parallel_test_db.db')
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

        rng = np.random.RandomState(88)
        conn = sqlite3.connect(cls.db_name)
        c = conn.cursor()
        c.execute('''CREATE TABLE test (id int, ii int)''')
        for ii in range(100):
            c.execute('''INSERT INTO test VALUES(%i, %i)''' % (ii, rng.random_integers(0, 100)))

        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        sims_clean_up()
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)
        if os.path.exists(cls.scratch_dir):
            shutil.rmtree(cls.scratch_dir)

    def test_parallel_writing(self):
        """
        Test that parallelCatalogWriter gets the right columns in it
        """
        db_name = os.path.join(self.scratch_dir, 'parallel_test_db.db')
        db = DbClass(database=db_name)

        class_dict = {os.path.join(self.scratch_dir, 'par_test1.txt'): ParallelCatClass1(db),
                      os.path.join(self.scratch_dir, 'par_test2.txt'): ParallelCatClass2(db),
                      os.path.join(self.scratch_dir, 'par_test3.txt'): ParallelCatClass3(db)}

        for file_name in class_dict:
            if os.path.exists(file_name):
                os.unlink(file_name)

        parallelCatalogWriter(class_dict)

        dtype = np.dtype([('id', int), ('test', int), ('ii', int)])
        data1 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test1.txt'), dtype=dtype, delimiter=',')
        data2 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test2.txt'), dtype=dtype, delimiter=',')
        data3 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test3.txt'), dtype=dtype, delimiter=',')

        # verify that the contents of the catalogs fit with the constraints in cannot_be_null
        self.assertEqual(len(np.where(data1['ii']%2 == 0)[0]), 0)
        self.assertEqual(len(np.where(data2['id']%2 == 0)[0]), 0)
        self.assertEqual(len(np.where(data3['id']%5 != 0)[0]), 0)

        # verify that the added value columns came out to the correct value
        np.testing.assert_array_equal(data1['id']**2, data1['test'])
        np.testing.assert_array_equal(data2['id']**3, data2['test'])
        np.testing.assert_array_equal(data3['id']**4, data3['test'])

        # now verify that all of the rows that were excluded from our catalogs
        # really should have been excluded

        control_cat = ControlCatalog(db)
        iterator = control_cat.iter_catalog()
        ct = 0
        ct_in_1 = 0
        ct_in_2 = 0
        ct_in_3 = 0
        for control_data in iterator:
            ct += 1

            if control_data[1] % 2 == 0:
                self.assertNotIn(control_data[0], data1['id'])
            else:
                ct_in_1 += 1
                self.assertIn(control_data[0], data1['id'])
                dex = np.where(data1['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data1['ii'][dex])

            if control_data[0] % 2 == 0:
                self.assertNotIn(control_data[0], data2['id'])
            else:
                ct_in_2 += 1
                self.assertIn(control_data[0], data2['id'])
                dex = np.where(data2['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data2['ii'][dex])

            if control_data[0] % 5 != 0:
                self.assertNotIn(control_data[0], data3['id'])
            else:
                ct_in_3 += 1
                self.assertIn(control_data[0], data3['id'])
                dex = np.where(data3['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data3['ii'][dex])

        self.assertEqual(ct_in_1, len(data1['id']))
        self.assertEqual(ct_in_2, len(data2['id']))
        self.assertEqual(ct_in_3, len(data3['id']))
        self.assertEqual(ct, 100)

        for file_name in class_dict:
            if os.path.exists(file_name):
                os.unlink(file_name)

    def test_parallel_writing_chunk_size(self):
        """
        Test that parallelCatalogWriter gets the right columns in it
        when chunk_size is not None (this is a repeat of test_parallel_writing)
        """
        db_name = os.path.join(self.scratch_dir, 'parallel_test_db.db')
        db = DbClass(database=db_name)

        class_dict = {os.path.join(self.scratch_dir, 'par_test1.txt'): ParallelCatClass1(db),
                      os.path.join(self.scratch_dir, 'par_test2.txt'): ParallelCatClass2(db),
                      os.path.join(self.scratch_dir, 'par_test3.txt'): ParallelCatClass3(db)}

        for file_name in class_dict:
            if os.path.exists(file_name):
                os.unlink(file_name)

        parallelCatalogWriter(class_dict, chunk_size=7)

        dtype = np.dtype([('id', int), ('test', int), ('ii', int)])
        data1 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test1.txt'), dtype=dtype, delimiter=',')
        data2 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test2.txt'), dtype=dtype, delimiter=',')
        data3 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test3.txt'), dtype=dtype, delimiter=',')

        # verify that the contents of the catalogs fit with the constraints in cannot_be_null
        self.assertEqual(len(np.where(data1['ii']%2 == 0)[0]), 0)
        self.assertEqual(len(np.where(data2['id']%2 == 0)[0]), 0)
        self.assertEqual(len(np.where(data3['id']%5 != 0)[0]), 0)

        # verify that the added value columns came out to the correct value
        np.testing.assert_array_equal(data1['id']**2, data1['test'])
        np.testing.assert_array_equal(data2['id']**3, data2['test'])
        np.testing.assert_array_equal(data3['id']**4, data3['test'])

        # now verify that all of the rows that were excluded from our catalogs
        # really should have been excluded

        control_cat = ControlCatalog(db)
        iterator = control_cat.iter_catalog()
        ct = 0
        ct_in_1 = 0
        ct_in_2 = 0
        ct_in_3 = 0
        for control_data in iterator:
            ct += 1

            if control_data[1] % 2 == 0:
                self.assertNotIn(control_data[0], data1['id'])
            else:
                ct_in_1 += 1
                self.assertIn(control_data[0], data1['id'])
                dex = np.where(data1['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data1['ii'][dex])

            if control_data[0] % 2 == 0:
                self.assertNotIn(control_data[0], data2['id'])
            else:
                ct_in_2 += 1
                self.assertIn(control_data[0], data2['id'])
                dex = np.where(data2['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data2['ii'][dex])

            if control_data[0] % 5 != 0:
                self.assertNotIn(control_data[0], data3['id'])
            else:
                ct_in_3 += 1
                self.assertIn(control_data[0], data3['id'])
                dex = np.where(data3['id'] == control_data[0])[0][0]
                self.assertEqual(control_data[1], data3['ii'][dex])

        self.assertEqual(ct_in_1, len(data1['id']))
        self.assertEqual(ct_in_2, len(data2['id']))
        self.assertEqual(ct_in_3, len(data3['id']))
        self.assertEqual(ct, 100)

        for file_name in class_dict:
            if os.path.exists(file_name):
                os.unlink(file_name)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    setup_module(None)
    unittest.main()
