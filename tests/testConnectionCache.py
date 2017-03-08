from builtins import next
from builtins import range
import unittest
import sqlite3
import os
import numpy as np

import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.utils.CodeUtilities import sims_clean_up
from lsst.sims.catalogs.db import CatalogDBObject, DBObject


def setup_module(module):
    lsst.utils.tests.init()


class CachingTestCase(unittest.TestCase):
    """
    This class will contain tests to make sure that CatalogDBObject is
    correctly using its _connection_cache
    """

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = os.path.join(getPackageDir("sims_catalogs"),
                                       "tests", "scratchSpace")
        cls.db_name = os.path.join(cls.scratch_dir, "connection_cache_test_db.db")
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

        conn = sqlite3.connect(cls.db_name)
        c = conn.cursor()
        c.execute('''CREATE TABLE test (id int, i1 int, i2 int)''')
        for ii in range(5):
            c.execute('''INSERT INTO test VALUES (%i, %i, %i)''' % (ii, ii*ii, -ii))
        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        sims_clean_up()
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

    def test_catalog_db_object_cacheing(self):
        """
        Test that opening multiple CatalogDBObjects that connect to the same
        database only results in one connection being opened and used.  We
        will test this by instantiating two CatalogDBObjects and a DBObject
        that connect to the same database.  We will then test that the two
        CatalogDBObjects' connections are identical, but that the DBObject has
        its own connection.
        """

        self.assertEqual(len(CatalogDBObject._connection_cache), 0)

        class DbClass1(CatalogDBObject):
            database = self.db_name
            port = None
            host = None
            driver = 'sqlite'
            tableid = 'test'
            idColKey = 'id'
            objid = 'test_db_class_1'

            columns = [('identification', 'id')]

        class DbClass2(CatalogDBObject):
            database = self.db_name
            port = None
            host = None
            driver = 'sqlite'
            tableid = 'test'
            idColKey = 'id'
            objid = 'test_db_class_2'

            columns = [('other', 'i1')]

        db1 = DbClass1()
        db2 = DbClass2()
        self.assertEqual(id(db1.connection), id(db2.connection))
        self.assertEqual(len(CatalogDBObject._connection_cache), 1)

        db3 = DBObject(database=self.db_name, driver='sqlite', host=None, port=None)
        self.assertNotEqual(id(db1.connection), id(db3.connection))

        self.assertEqual(len(CatalogDBObject._connection_cache), 1)

        # check that if we had passed db1.connection to a DBObject,
        # the connections would be identical
        db4 = DBObject(connection=db1.connection)
        self.assertEqual(id(db4.connection), id(db1.connection))

        self.assertEqual(len(CatalogDBObject._connection_cache), 1)

        # verify that db1 and db2 are both useable
        results = db1.query_columns(colnames=['id', 'i1', 'i2', 'identification'])
        results = next(results)
        self.assertEqual(len(results), 5)
        np.testing.assert_array_equal(results['id'], list(range(5)))
        np.testing.assert_array_equal(results['id'], results['identification'])
        np.testing.assert_array_equal(results['id']**2, results['i1'])
        np.testing.assert_array_equal(results['id']*(-1), results['i2'])

        results = db2.query_columns(colnames=['id', 'i1', 'i2', 'other'])
        results = next(results)
        self.assertEqual(len(results), 5)
        np.testing.assert_array_equal(results['id'], list(range(5)))
        np.testing.assert_array_equal(results['id']**2, results['i1'])
        np.testing.assert_array_equal(results['i1'], results['other'])
        np.testing.assert_array_equal(results['id']*(-1), results['i2'])


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
