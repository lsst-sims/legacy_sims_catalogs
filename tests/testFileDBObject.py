from __future__ import with_statement
from builtins import zip
import unittest
import os
import numpy as np
import lsst.utils.tests
from lsst.utils import getPackageDir
from lsst.sims.catalogs.db import fileDBObject


def setup_module(module):
    lsst.utils.tests.init()


class FileDBObjectTestCase(unittest.TestCase):
    """
    This class will test that fileDBObject can correctly ingest a database,
    preserving all of the data it was given.  This is it's own test because
    a lot of other unit tests depend on fileDBObject to create data to test
    against.
    """

    def setUp(self):
        self.scratch_dir = os.path.join(getPackageDir("sims_catalogs"),
                                        "tests", "scratchSpace")

    def test_ingest(self):
        """
        Test that fileDBObject correctly ingests a text file containing
        multiple data types.
        """
        txt_file_name = os.path.join(self.scratch_dir,
                                     "filedbojb_ingest_test.txt")

        rng = np.random.RandomState(8821)
        alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        n_rows = 34
        n_letters = 72
        f_list = rng.random_sample(n_rows)
        i_list = rng.randint(0, 2**50, n_rows)
        word_dex_list = rng.randint(0, len(alphabet)-1, (n_rows, n_letters))
        word_list = []
        with open(txt_file_name, 'w') as output_file:
            output_file.write("# a header\n")
            for ix, (ff, ii, ww) in enumerate(zip(f_list, i_list, word_dex_list)):
                word = ''
                for wwdex in ww:
                    word += alphabet[wwdex]
                word_list.append(word)
                self.assertEqual(len(word), n_letters)
                output_file.write('%d %.13f %ld %s\n' % (ix, ff, ii, word))

        dtype = np.dtype([('id', int), ('float', float), ('int', int), ('word', str, n_letters)])
        db = fileDBObject(txt_file_name, runtable='test', dtype=dtype, idColKey='id')
        results = db.execute_arbitrary('SELECT * from test')
        self.assertEqual(len(results), n_rows)
        for row in results:
            i_row = row[0]
            self.assertAlmostEqual(f_list[i_row], row[1], 13)
            self.assertEqual(i_list[i_row], row[2])
            self.assertEqual(word_list[i_row], row[3])

        if os.path.exists(txt_file_name):
            os.unlink(txt_file_name)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
