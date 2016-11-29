from __future__ import with_statement
import unittest
import numpy as np
import os

import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.catalogs.definitions import InstanceCatalog, CompoundInstanceCatalog
from lsst.sims.catalogs.db import fileDBObject, CatalogDBObject
from lsst.sims.catalogs.decorators import cached, compound


def setup_module(module):
    lsst.utils.tests.init()


class InstanceCatalogTestCase(unittest.TestCase):
    """
    This class will contain tests that will help us verify
    that using cannot_be_null to filter the contents of an
    InstanceCatalog works as it should.
    """

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = os.path.join(getPackageDir('sims_catalogs'), 'tests', 'scratchSpace')

        cls.db_src_name = os.path.join(cls.scratch_dir, 'inst_cat_filter_db.txt')
        if os.path.exists(cls.db_src_name):
            os.unlink(cls.db_src_name)

        with open(cls.db_src_name, 'w') as output_file:
            output_file.write('#a header\n')
            for ii in range(10):
                output_file.write('%d %d %d %d\n' % (ii, ii+1, ii+2, ii+3))

        dtype = np.dtype([('id', int), ('ip1', int), ('ip2', int), ('ip3', int)])
        cls.db = fileDBObject(cls.db_src_name, runtable='test', dtype=dtype,
                              idColKey='id')

    @classmethod
    def tearDownClass(cls):

        del cls.db

        if os.path.exists(cls.db_src_name):
            os.unlink(cls.db_src_name)

    def test_single_filter(self):
        """
        Test filtering on a single column
        """

        class FilteredCat(InstanceCatalog):
            column_outputs = ['id', 'ip1', 'ip2', 'ip3t']
            cannot_be_null = ['ip3t']

            @cached
            def get_ip3t(self):
                base = self.column_by_name('ip3')
                ii = self.column_by_name('id')
                return np.where(ii < 5, base, None)

        cat_name = os.path.join(self.scratch_dir, 'inst_single_filter_cat.txt')
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat(self.db)
        cat.write_catalog(cat_name)
        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        # verify that the catalog contains the expected data
        self.assertEqual(len(input_lines), 6)  # 5 data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = i_line - 1
                self.assertEqual(line,
                                 '%d, %d, %d, %d\n' % (ii, ii+1, ii+2, ii+3))

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_two_filters(self):
        """
        Test a case where we filter on two columns.
        """
        class FilteredCat2(InstanceCatalog):
            column_outputs = ['id', 'ip1', 'ip2t', 'ip3t']
            cannot_be_null = ['ip2t', 'ip3t']

            @cached
            def get_ip2t(self):
                base = self.column_by_name('ip2')
                return np.where(base % 2 == 0, base, None)

            @cached
            def get_ip3t(self):
                base = self.column_by_name('ip3')
                return np.where(base % 3 == 0, base, None)

        cat_name = os.path.join(self.scratch_dir, "inst_two_filter_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat2(self.db)
        cat.write_catalog(cat_name)

        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        self.assertEqual(len(input_lines), 3)  # two data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = (i_line - 1)*6
                ip1 = ii + 1
                ip2 = ii + 2
                ip3 = ii + 3
                self.assertEqual(line,
                                 '%d, %d, %d, %d\n' % (ii, ip1, ip2, ip3))

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_post_facto_filters(self):
        """
        Test a case where filters are declared after instantiation
        """
        class FilteredCat3(InstanceCatalog):
            column_outputs = ['id', 'ip1', 'ip2t', 'ip3t']

            @cached
            def get_ip2t(self):
                base = self.column_by_name('ip2')
                return np.where(base % 2 == 0, base, None)

            @cached
            def get_ip3t(self):
                base = self.column_by_name('ip3')
                return np.where(base % 3 == 0, base, None)

        cat_name = os.path.join(self.scratch_dir, "inst_post_facto_filter_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat3(self.db)
        cat.cannot_be_null = ['ip2t', 'ip3t']
        cat.write_catalog(cat_name)

        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        self.assertEqual(len(input_lines), 3)  # two data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = (i_line - 1)*6
                ip1 = ii + 1
                ip2 = ii + 2
                ip3 = ii + 3
                self.assertEqual(line,
                                 '%d, %d, %d, %d\n' % (ii, ip1, ip2, ip3))

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_compound_column(self):
        """
        Test filtering on a catalog with a compound column that is calculated
        after the filter column (example code has shown this to be a difficult case)
        """

        class FilteredCat4(InstanceCatalog):
            column_outputs = ['id', 'a', 'b', 'c', 'filter_col']
            cannot_be_null = ['filter_col']

            @compound('a', 'b', 'c')
            def get_alphabet(self):
                ii = self.column_by_name('ip3')
                return np.array([ii*ii, ii*ii*ii, ii*ii*ii*ii])

            @cached
            def get_filter_col(self):
                base = self.column_by_name('a')
                return np.where(base % 3 == 0, base/2.0, None)

        cat_name = os.path.join(self.scratch_dir, "inst_compound_column_filter_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat4(self.db)
        cat.write_catalog(cat_name)

        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        # verify that the catalog contains expected data
        self.assertEqual(len(input_lines), 5)  # 4 data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = (i_line - 1)*3
                ip3 = ii + 3
                self.assertEqual(line, '%d, %d, %d, %d, %.1f\n'
                                        % (ii, ip3*ip3, ip3*ip3*ip3, ip3*ip3*ip3*ip3, 0.5*(ip3*ip3)))

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_filter_on_compound_column(self):
        """
        Test filtering on a catalog that filters on a compound column
        (example code has shown this to be a difficult case)
        """

        class FilteredCat5(InstanceCatalog):
            column_outputs = ['id', 'a', 'b', 'c']
            cannot_be_null = ['c']

            @compound('a', 'b', 'c')
            def get_alphabet(self):
                ii = self.column_by_name('ip3')
                c = ii*ii*ii*ii
                return np.array([ii*ii, ii*ii*ii,
                                 np.where(c % 3 == 0, c, None)])

        cat_name = os.path.join(self.scratch_dir, "inst_actual_compound_column_filter_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat5(self.db)
        cat.write_catalog(cat_name)

        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        # verify that the catalog contains expected data
        self.assertEqual(len(input_lines), 5)  # 4 data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = (i_line - 1)*3
                ip3 = ii + 3
                self.assertEqual(line, '%d, %d, %d, %d\n'
                                        % (ii, ip3*ip3, ip3*ip3*ip3, ip3*ip3*ip3*ip3))

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_empty_chunk(self):
        """
        Test that catalog filtering behaves correctly, even when the first
        chunk is empty
        """

        class FilteredCat6(InstanceCatalog):
            column_outputs = ['id', 'filter']
            cannot_be_null = ['filter']

            @cached
            def get_filter(self):
                ii = self.column_by_name('ip1')
                return np.where(ii>5, ii, None)

        cat_name = os.path.join(self.scratch_dir, "inst_empty_chunk_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = FilteredCat6(self.db)
        cat.write_catalog(cat_name, chunk_size=2)

        # check that the catalog contains the correct information
        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()

        self.assertEqual(len(input_lines), 6)  # 5 data lines and a header
        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            else:
                ii = 4 + i_line
                self.assertEqual(line, '%d, %d\n' % (ii, ii+1))

        if os.path.exists(cat_name):
            os.unlink(cat_name)


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

            @cached
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

            @cached
            def get_ip2t(self):
                base = self.column_by_name('ip2')
                ii = self.column_by_name('id')
                return np.where(ii < 4, base, None)

        class CatClass3(InstanceCatalog):
            column_outputs = ['id', 'ip3t']
            cannot_be_null = ['ip3t']

            @cached
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

    def test_compound_cat_compound_column(self):
        """
        Test filtering a CompoundInstanceCatalog on a compound column
        """

        class CatClass4(InstanceCatalog):
            column_outputs = ['id', 'a', 'b']
            cannot_be_null = ['a']

            @compound('a', 'b')
            def get_alphabet(self):
                a = self.column_by_name('ip1')
                a = a*a
                b = self.column_by_name('ip2')
                b = b*0.25
                return np.array([np.where(a % 2 == 0, a, None), b])

        class CatClass5(InstanceCatalog):
            column_outputs = ['id', 'a', 'b', 'filter']
            cannot_be_null = ['b', 'filter']

            @compound('a', 'b')
            def get_alphabet(self):
                ii = self.column_by_name('ip1')
                return np.array([self.column_by_name('ip2')**3,
                                 np.where(ii % 2 == 1, ii, None)])

            @cached
            def get_filter(self):
                ii = self.column_by_name('ip1')
                return np.where(ii % 3 != 0, ii, None)

        class DbClass(CatalogDBObject):
            host = None
            port = None
            database = self.db_name
            driver = 'sqlite'
            tableid = 'test'
            idColKey = 'id'

        class DbClass4(DbClass):
            objid = 'silliness4'

        class DbClass5(DbClass):
            objid = 'silliness5'

        cat_name = os.path.join(self.scratch_dir, "compound_cat_compound_filter_cat.txt")
        if os.path.exists(cat_name):
            os.unlink(cat_name)

        cat = CompoundInstanceCatalog([CatClass4, CatClass5], [DbClass4, DbClass5])
        cat.write_catalog(cat_name)

        # now make sure that the catalog contains the expected data
        with open(cat_name, 'r') as input_file:
            input_lines = input_file.readlines()
        self.assertEqual(len(input_lines), 9)  # 8 data lines and a header

        first_cat_lines = ['1, 4, 0.75\n', '3, 16, 1.25\n',
                           '5, 36, 1.75\n', '7, 64, 2.25\n',
                           '9, 100, 2.75\n']

        second_cat_lines = ['0, 8, 1, 1\n', '4, 216, 5, 5\n',
                            '6, 512, 7, 7\n']

        for i_line, line in enumerate(input_lines):
            if i_line is 0:
                continue
            elif i_line < 6:
                self.assertEqual(line, first_cat_lines[i_line-1])
            else:
                self.assertEqual(line, second_cat_lines[i_line-6])

        if os.path.exists(cat_name):
            os.unlink(cat_name)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
