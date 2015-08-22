from __future__ import with_statement
import os
import numpy
import unittest
import lsst.utils.tests as utilsTests
from lsst.utils import getPackageDir
from lsst.sims.catalogs.generation.db import fileDBObject


class CompoundCatalogTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        baseDir = os.path.join(getPackageDir('sims_catalogs_measures'),
                               'tests', 'scratchSpace')

        cls.table1FileName = os.path.join(baseDir, 'compound_table1.txt')
        cls.table2FileName = os.path.join(baseDir, 'compound_table2.txt')

        if os.path.exists(cls.table1FileName):
            os.unlink(cls.table1FileName)
        if os.path.exists(cls.table2FileName):
            os.unlink(cls.table2FileName)

        dtype1 = numpy.dtype([
                           ('ra', numpy.float),
                           ('dec', numpy.float),
                           ('mag', numpy.float),
                           ('dmag', numpy.float),
                           ('dra', numpy.float),
                           ('ddec', numpy.float)
                           ])

        nPts = 100
        numpy.random.seed(42)
        raList = numpy.random.random_sample(nPts)*360.0
        decList = numpy.random.random_sample(nPts)*180.0-90.0
        magList = numpy.random.random_sample(nPts)*10.0+15.0
        dmagList = numpy.random.random_sample(nPts)*10.0 - 5.0
        draList = numpy.random.random_sample(nPts)*5.0 - 2.5
        ddecList = numpy.random.random_sample(nPts)*(-2.0) - 4.0

        cls.table1Control = numpy.rec.fromrecords([
                                                  (r, d, mm, dm, dr, dd) \
                                                  for r, d, mm, dm, dr, dd \
                                                  in zip(raList, decList,
                                                         magList, dmagList,
                                                         draList, ddecList)],
                                                  dtype=dtype1
                                                  )

        with open(cls.table1FileName, 'w') as output:
            output.write("# id ra dec mag dmag dra ddec\n")
            for ix, (r, d, mm, dm, dr, dd) in \
            enumerate(zip(raList, decList, magList, dmagList, draList, ddecList)):

                output.write('%d %e %e %e %e %e %e\n' \
                             % (ix, r, d, mm, dm, dr, dd))


        dtype2 = numpy.dtype([
                            ('ra2', numpy.float),
                            ('dec2', numpy.float),
                            ('mag2', numpy.float)
                            ])

        ra2List = numpy.random.random_sample(nPts)*360.0+360.0
        dec2List = numpy.random.random_sample(nPts)*180.0+180.0
        mag2List = numpy.random.random_sample(nPts)*3.0+7.0

        cls.table2Control = numpy.rec.fromrecords([
                                                  (r, d, m) \
                                                  for r, d, m in \
                                                  zip(ra2List, dec2List, mag2List)
                                                  ], dtype=dtype2)

        with open(cls.table2FileName, 'w') as output:
            output.write('# id ra dec mag\n')
            for ix, (r, d, m) in enumerate(zip(ra2List, dec2List, mag2List)):
                output.write('%d %e %e %e\n' % (ix, r, d, m))

        cls.dbName = os.path.join(baseDir, 'compound_db.db')
        if os.path.exists(cls.dbName):
            os.unlink(cls.dbName)

        fdbo = fileDBObject(cls.table1FileName, runtable='table1',
                            database=cls.dbName, dtype=dtype1,
                            idColKey='id')

        fdbo = fileDBObject(cls.table2FileName, runtable='table2',
                            database=cls.dbName, dtype=dtype2,
                            idColKey='id')


    def testCompoundCatalog(self):
        pass

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(CompoundCatalogTest)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
