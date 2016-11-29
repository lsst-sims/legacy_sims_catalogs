from __future__ import with_statement
import unittest
import numpy as np
import os

import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.catalogs.definitions import InstanceCatalog, CompoundInstanceCatalog
from lsst.sims.catalogs.db import fileDBObject, CatalogDBObject


def setup_module(module):
    lsst.utils.tests.init()


class CompoundInstanceCatalogTestCase(unittest.TestCase):
    """
    This class will contain tests that will help us verify that using
    cannot_be_null to filter the contents of a CompoundInstanceCatalog
    works as it should.
    """

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')

        cls.db_src_name = os.path.join(cls.scratch_dir, 'compound_cat_filter_db.txt')
        if os.path.exists(cls.db_src_name):
            os.unlink(cls.db_src_name)

        cls.db_name = os.path.join(cls.scratch_dir, 'compound_cat_filter_db.db')
        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

        with open(cls.db_src_name, 'w') as output_file:
            output_file.write('#a header\n')
            for ii in range(10):
                output_file.write('%d %d %d %d\n' % (ii, ii+1, ii+2, ii+3))

        dtype = np.dtype([('id', int), ('ip1', int), ('ip2', int), ('ip3', int)])
        fileDBObject(cls.db_src_name, runtable='test', dtype=dtype,
                     idColKey='id', database=cls.db_name)

    @classmethod
    def tearDownClass(cls):

        if os.path.exists(cls.db_src_name):
            os.unlink(cls.db_src_name)

        if os.path.exists(cls.db_name):
            os.unlink(cls.db_name)

    def test_compound_cat(self):
        """
        Test that a CompoundInstanceCatalog made up of InstanceCatalog classes that
        each filter on a different condition gives the correct outputs.
        """

        class CatClass1(InstanceCatalog):
            column_outputs = ['id', 'ip1t']
            cannot_be_null = ['ip1t']

            def get_ip1t(self):
                base = self.column_by_name('ip1')
                output = []
                for bb in base:
                    if bb%2 == 0:
                        output.append(bb)
                    else:
                        output.append(None)
                return np.array(output)

        class CatClass2(InstanceCatalog):
            column_outputs = ['id', 'ip2t']
            cannot_be_null = ['ip2t']

            def get_ip2t(self):
                base = self.column_by_name('ip2')
                ii = self.column_by_name('id')
                return np.where(ii < 4, base, None)

        class CatClass3(InstanceCatalog):
            column_outputs = ['id', 'ip3t']
            cannot_be_null = ['ip3t']

            def get_ip3t(self):
                base = self.column_by_name('ip3')
                ii = self.column_by_name('id')
                return np.where(ii > 5, base, None)

        class DbClass(CatalogDBObject):
            host = None
            port = None
            database = self.db_name
            driver = 'sqlite'
            tableid = 'test'
            objid = 'silliness'
            idColKey = 'id'

        class DbClass1(DbClass):
            objid = 'silliness1'

        class DbClass2(DbClass):
            objid = 'silliness2'

        class DbClass3(DbClass):
            objid = 'silliness3'

        cat = CompoundInstanceCatalog([CatClass1, CatClass2, CatClass3],
                                      [DbClass1, DbClass2, DbClass3])

        cat_name = os.path.join(self.scratch_dir, "compound_filter_output.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat.write_catalog(cat_name)

        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        self.assertEqual(len(input_lines), 14)

        # given that we know what the contents of each sub-catalog should be
        # and how they should be ordered, loop through the lines of the output
        # catalog, verifying that every line is where it ought to be
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            elif i_line < 6:
                ii = 2*(i_line-1) + 1
                self.assertEqual(line, '%d, %d\n' % (ii, ii+1))
            elif i_line < 10:
                ii = i_line - 6
                self.assertEqual(line, '%d, %d\n' % (ii, ii+2))
            else:
                ii = i_line - 10
                self.assertEqual(line, '%d, %d\n' % (ii+6, ii+6+3))

        if os.path.exists(cat_name):
            os.unlink(cat_name)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
