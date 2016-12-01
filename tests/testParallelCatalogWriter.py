from __future__ import with_statement
import unittest
import sqlite3
import os
import numpy as np

import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.catalogs.definitions import parallelCatalogWriter
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.catalogs.decorators import compound, cached
from lsst.sims.catalogs.db import CatalogDBObject


def setup_module(module):
    lsst.utils.tests.init()


class ParallelWriterTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = os.path.join(getPackageDir('sims_catalogs'),
                                       'tests', 'scratchSpace')

        cls.db_name = os.path.join(cls.scratch_dir, 'parallel_test_db.db')
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

        rng = np.random.RandomState(88)
        conn = sqlite3.connect(cls.db_name)
        c = conn.cursor()
        c.execute('''CREATE TABLE test (id int, ii int)''')
        for ii in range(100):
            c.execute('''INSERT INTO test VALUES(%i, %i)''' % (ii, rng.random_integers(0,100)))

        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

    def test_parallel_writing(self):
        """
        Test that parallelCatalogWriter gets the right columns in it
        """
        class DbClass(CatalogDBObject):
            tableid = 'test'
            database = self.db_name
            host = None
            port = None
            driver = 'sqlite'
            objid = 'paralle_writer_test_db'
            idColKey = 'id'

        class CatClass1(InstanceCatalog):
            column_outputs = ['id', 'test1', 'ii']
            cannot_be_null = ['valid1']

            @compound('test1', 'valid1')
            def get_values(self):
                ii = self.column_by_name('ii')
                return np.array([self.column_by_name('id')**2,
                                 np.where(ii%2==1, ii, None)])

        class CatClass2(InstanceCatalog):
            column_outputs = ['id', 'test2', 'ii']
            cannot_be_null = ['valid2']

            @compound('test2', 'valid2')
            def get_values(self):
                ii = self.column_by_name('id')
                return np.array([self.column_by_name('id')**3,
                                 np.where(ii%2==1, ii, None)])

        class CatClass3(InstanceCatalog):
            column_outputs = ['id', 'test3', 'ii']
            cannot_be_null = ['valid3']

            @cached
            def get_test3(self):
                return self.column_by_name('id')**4

            @cached
            def get_valid3(self):
                ii = self.column_by_name('id')
                return np.where(ii%5 == 0, ii, None)

        db = DbClass()

        class_dict = {os.path.join(self.scratch_dir, 'par_test1.txt'): CatClass1(db),
                      os.path.join(self.scratch_dir, 'par_test2.txt'): CatClass2(db),
                      os.path.join(self.scratch_dir, 'par_test3.txt'): CatClass3(db)}

        for file_name in class_dict:
            if os.path.exists(file_name):
                os.unlink(file_name)

        parallelCatalogWriter(class_dict)

        dtype = np.dtype([('id', int), ('test', int), ('ii', int)])
        data1 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test1.txt'), dtype=dtype, delimiter=',')
        data2 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test2.txt'), dtype=dtype, delimiter=',')
        data3 = np.genfromtxt(os.path.join(self.scratch_dir, 'par_test3.txt'), dtype=dtype, delimiter=',')

        # verify that all three catalogs got the same objects
        np.testing.assert_array_equal(data1['id'], data2['id'])
        np.testing.assert_array_equal(data2['id'], data3['id'])

        # verify that the contents of the catalogs fit with the constraints in cannot_be_null
        self.assertEqual(len(np.where(data1['id']%2 == 0)[0]), 0)
        self.assertEqual(len(np.where(data1['id']%5 != 0)[0]), 0)
        self.assertEqual(len(np.where(data1['ii']%2 == 0)[0]), 0)

        # verify that the added value columns came out to the correct value
        np.testing.assert_array_equal(data1['id']**2, data1['test'])
        np.testing.assert_array_equal(data2['id']**3, data2['test'])
        np.testing.assert_array_equal(data3['id']**4, data3['test'])

        # now verify that all of the rows that were excluded from our catalogs
        # really should have been excluded
        class ControlCatalog(InstanceCatalog):
            column_outputs = ['id', 'ii']

        control_cat = ControlCatalog(db)
        iterator = control_cat.iter_catalog()
        ct = 0
        ct_in = 0
        for control_data in iterator:
            ct += 1
            if control_data[0] in data1['id']:
                ct_in += 1
            else:
                is_valid = ((control_data[0]%2 == 1) and
                            (control_data[0]%5 == 0) and
                            (control_data[1]%2 == 1))
                
                self.assertFalse(is_valid, msg='Column filtering missed a row')

        self.assertEqual(ct_in, len(data1['id']))
        self.assertEqual(ct, 100)

class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
