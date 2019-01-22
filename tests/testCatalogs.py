from __future__ import with_statement
from builtins import zip
from builtins import object
import os
import sqlite3
import numpy as np
import unittest
import tempfile
import shutil
import lsst.utils.tests
from lsst.sims.utils.CodeUtilities import sims_clean_up
from lsst.sims.utils import ObservationMetaData
from lsst.sims.catalogs.db import fileDBObject, CatalogDBObject
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.catalogs.decorators import compound
from lsst.sims.utils import haversine, angularSeparation


ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


# a class of catalog that outputs all the significant figures in
# ra and dec so that it can be read back in to make sure that our
# Haversine-based query actually returns all of the points that
# are inside the circular bound desired
class BoundsCatalog(InstanceCatalog):
    catalog_type = 'bounds_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000']

    default_formats = {'f': '%.20f'}


def twice_fn(x):
    return 2.0*x


class TransformationCatalog(InstanceCatalog):
    catalog_type = 'transformation_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000']
    default_formats = {'f': '%.12f'}
    transformations = {'raJ2000': twice_fn}


class BasicCatalog(InstanceCatalog):
    catalog_type = 'basic_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000', 'umag', 'gmag', 'rmag', 'imag',
                      'zmag', 'ymag']

    default_formats = {'f': '%.12f'}


class TestAstMixin(object):
    @compound('ra_corr', 'dec_corr')
    def get_points_corrected(self):
        # Fake astrometric correction
        ra_corr = self.column_by_name('raJ2000')+0.001
        dec_corr = self.column_by_name('decJ2000')+0.001
        return ra_corr, dec_corr


class CustomCatalog(BasicCatalog, TestAstMixin):
    catalog_type = 'custom_catalog'
    refIdCol = 'id'
    column_outputs = ['id', 'raJ2000', 'decJ2000', 'umag', 'gmag', 'rmag', 'imag',
                      'zmag', 'ymag', 'ra_corr', 'dec_corr']


def compareFiles(file1, file2):
    with open(file1) as fh:
        str1 = "".join(fh.readlines())
    with open(file2) as fh:
        str2 = "".join(fh.readlines())
    return str1 == str2


def write_star_file_db(file_name):

    np.random.seed(88)
    nstars = 10000
    ra = np.random.random_sample(nstars)*360.0
    dec = (np.random.random_sample(nstars)-0.5)*180.0
    umag = np.random.random_sample(nstars)*10.0 + 15.0
    gmag = np.random.random_sample(nstars)*10.0 + 15.0
    rmag = np.random.random_sample(nstars)*10.0 + 15.0
    imag = np.random.random_sample(nstars)*10.0 + 15.0
    zmag = np.random.random_sample(nstars)*10.0 + 15.0
    ymag = np.random.random_sample(nstars)*10.0 + 15.0

    with open(file_name, 'w') as output_file:
        for ix, (rr, dd, um, gm, rm, im, zm, ym) in \
            enumerate(zip(ra, dec, umag, gmag, rmag, imag, zmag, ymag)):

            output_file.write('%d %.12f %.12f %.12f %.12f %.12f %.12f %.12f %.12f\n' %
                              (ix, rr, dd, um, gm, rm, im, zm, ym))

    starDtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float),
                          ('umag', np.float),
                          ('gmag', np.float),
                          ('rmag', np.float),
                          ('imag', np.float),
                          ('zmag', np.float),
                          ('ymag', np.float)])

    starDB = fileDBObject(file_name, runtable='stars', dtype=starDtype, idColKey='id')
    starDB.raColName = 'raJ2000'
    starDB.decColName = 'decJ2000'

    controlData = np.genfromtxt(file_name, dtype=starDtype)

    return starDB, controlData


class InstanceCatalogTestCase(unittest.TestCase):

    longMessage = True

    @classmethod
    def setUpClass(cls):

        cls.scratch_dir = tempfile.mkdtemp(dir=ROOT, prefix="scratchSpace-")
        cls.starTextName = os.path.join(cls.scratch_dir, 'icStarTestCatalog.txt')

        if os.path.exists(cls.starTextName):
            os.unlink(cls.starTextName)

        cls.starDB, cls.starControlData = write_star_file_db(cls.starTextName)

    @classmethod
    def tearDownClass(cls):

        if os.path.exists(cls.starTextName):
            os.unlink(cls.starTextName)
        if os.path.exists(cls.scratch_dir):
                shutil.rmtree(cls.scratch_dir)

    def setUp(self):
        self.obsMd = ObservationMetaData(boundType = 'circle', pointingRA = 210.0, pointingDec = -60.0,
                                         boundLength=20.0, mjd=52000., bandpassName='r')

    def testStarLike(self):
        """
        Write a couple of catalogs.  Verify that the objects that end up in the catalog fall
        within the pointing and that the objects that do not end up in the catalog fall
        outside of the pointing
        """

        catName = os.path.join(self.scratch_dir, '_starLikeCat.txt')

        if os.path.exists(catName):
            os.unlink(catName)

        # this dtype corresponds to the outputs of the catalog
        dtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float),
                          ('umag', np.float),
                          ('gmag', np.float),
                          ('rmag', np.float),
                          ('imag', np.float),
                          ('zmag', np.float),
                          ('ymag', np.float),
                          ('ra_corr', np.float),
                          ('dec_corr', np.float)])

        t = self.starDB.getCatalog('custom_catalog', obs_metadata=self.obsMd)
        t.write_catalog(catName)

        testData = np.genfromtxt(catName, delimiter = ', ', dtype=dtype)

        # make sure that something ended up in the catalog
        self.assertGreater(len(testData), 0)

        # iterate over the lines in the catalog
        # verify that those line exist in the control data
        # also verify that those lines fall within the requested field of view
        for line in testData:
            ic = np.where(self.starControlData['id'] == line['id'])[0][0]
            self.assertAlmostEqual(line['umag'], self.starControlData['umag'][ic], 6)
            self.assertAlmostEqual(line['gmag'], self.starControlData['gmag'][ic], 6)
            self.assertAlmostEqual(line['rmag'], self.starControlData['rmag'][ic], 6)
            self.assertAlmostEqual(line['imag'], self.starControlData['imag'][ic], 6)
            self.assertAlmostEqual(line['zmag'], self.starControlData['zmag'][ic], 6)
            self.assertAlmostEqual(line['ymag'], self.starControlData['ymag'][ic], 6)
            self.assertAlmostEqual(line['raJ2000'], self.starControlData['raJ2000'][ic], 6)
            self.assertAlmostEqual(line['decJ2000'], self.starControlData['decJ2000'][ic], 6)
            self.assertAlmostEqual(line['ra_corr'], line['raJ2000']+0.001, 6)
            self.assertAlmostEqual(line['dec_corr'], line['decJ2000']+0.001, 6)
            dl = haversine(np.radians(line['raJ2000']), np.radians(line['decJ2000']),
                           self.obsMd._pointingRA, self.obsMd._pointingDec)

            self.assertLess(np.degrees(dl), self.obsMd.boundLength)

        # examine the lines that did not fall in the catalog
        lines_not_in_catalog = []
        for id_val in self.starControlData['id']:
            if id_val not in testData['id']:
                lines_not_in_catalog.append(id_val)
        lines_not_in_catalog = np.array(lines_not_in_catalog)

        self.assertGreater(len(lines_not_in_catalog), 0)

        # make sure that those lines are, indeed, outside of the field of view
        for ic in lines_not_in_catalog:
            dl = haversine(self.obsMd._pointingRA, self.obsMd._pointingDec,
                           np.radians(self.starControlData['raJ2000'][ic]),
                           np.radians(self.starControlData['decJ2000'][ic]))

            msg = '\nRA %e Dec %e\n' % (self.starControlData['raJ2000'][ic], self.starControlData['decJ2000'][ic])
            msg += 'pointing RA %e Dec %e\n' % (self.obsMd.pointingRA, self.obsMd.pointingDec)
            self.assertGreater(np.degrees(dl), self.obsMd.boundLength, msg=msg)

        if os.path.exists(catName):
            os.unlink(catName)

        # now do the same thing for the basic catalog class

        dtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float),
                          ('umag', np.float),
                          ('gmag', np.float),
                          ('rmag', np.float),
                          ('imag', np.float),
                          ('zmag', np.float),
                          ('ymag', np.float)])

        t = self.starDB.getCatalog('basic_catalog', obs_metadata=self.obsMd)
        t.write_catalog(catName)

        testData = np.genfromtxt(catName, delimiter = ', ', dtype=dtype)

        # make sure that something ended up in the catalog
        self.assertGreater(len(testData), 0)

        # iterate over the lines in the catalog
        # verify that those line exist in the control data
        # also verify that those lines fall within the requested field of view
        for line in testData:
            ic = np.where(self.starControlData['id'] == line['id'])[0][0]
            self.assertAlmostEqual(line['umag'], self.starControlData['umag'][ic], 6)
            self.assertAlmostEqual(line['gmag'], self.starControlData['gmag'][ic], 6)
            self.assertAlmostEqual(line['rmag'], self.starControlData['rmag'][ic], 6)
            self.assertAlmostEqual(line['imag'], self.starControlData['imag'][ic], 6)
            self.assertAlmostEqual(line['zmag'], self.starControlData['zmag'][ic], 6)
            self.assertAlmostEqual(line['ymag'], self.starControlData['ymag'][ic], 6)
            self.assertAlmostEqual(line['raJ2000'], self.starControlData['raJ2000'][ic], 6)
            self.assertAlmostEqual(line['decJ2000'], self.starControlData['decJ2000'][ic], 6)
            dl = haversine(np.radians(line['raJ2000']), np.radians(line['decJ2000']),
                           self.obsMd._pointingRA, self.obsMd._pointingDec)

            self.assertLess(np.degrees(dl), self.obsMd.boundLength)

        # examine the lines that did not fall in the catalog
        lines_not_in_catalog = []
        for id_val in self.starControlData['id']:
            if id_val not in testData['id']:
                lines_not_in_catalog.append(id_val)
        lines_not_in_catalog = np.array(lines_not_in_catalog)

        self.assertGreater(len(lines_not_in_catalog), 0)

        # make sure that those lines are, indeed, outside of the field of view
        for ic in lines_not_in_catalog:
            dl = haversine(self.obsMd._pointingRA, self.obsMd._pointingDec,
                           np.radians(self.starControlData['raJ2000'][ic]),
                           np.radians(self.starControlData['decJ2000'][ic]))

            self.assertGreater(np.degrees(dl), self.obsMd.boundLength)

        if os.path.exists(catName):
            os.unlink(catName)

    def test_transformation(self):
        """
        Test that transformations are applied to columns in an InstanceCatalog
        """
        catName = os.path.join(self.scratch_dir,
                               'transformation_catalog.txt')

        if os.path.exists(catName):
            os.unlink(catName)

        t = self.starDB.getCatalog('transformation_catalog', obs_metadata=self.obsMd)
        t.write_catalog(catName)

        dtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float)])

        testData = np.genfromtxt(catName, delimiter=', ', dtype=dtype)
        self.assertGreater(len(testData), 0)
        for line in testData:
            ic = np.where(self.starControlData['id'] == line['id'])[0][0]
            self.assertAlmostEqual(line['decJ2000'], self.starControlData['decJ2000'][ic], 5)
            self.assertAlmostEqual(line['raJ2000'], 2.0*self.starControlData['raJ2000'][ic], 5)

        if os.path.exists(catName):
            os.unlink(catName)

    def test_iter_catalog(self):
        """
        Test that iter_catalog returns the same results as write_catalog
        """

        obs = ObservationMetaData(pointingRA=10.0, pointingDec=-20.0,
                                  boundLength=50.0, boundType='circle')

        cat = BasicCatalog(self.starDB, obs_metadata=obs)
        cat_name = os.path.join(self.scratch_dir, 'iter_catalog_control.txt')
        cat.write_catalog(cat_name)
        with open(cat_name, 'r') as in_file:
            in_lines = in_file.readlines()
        self.assertGreater(len(in_lines), 1)
        self.assertLess(len(in_lines), len(self.starControlData))

        cat = BasicCatalog(self.starDB, obs_metadata=obs)
        line_ct = 0
        for line in cat.iter_catalog():
            str_line = '%d, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f\n' % \
            (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8])
            self.assertIn(str_line, in_lines)
            line_ct += 1
        self.assertEqual(line_ct, len(in_lines)-1)

        if os.path.exists(cat_name):
            os.unlink(cat_name)

    def test_iter_catalog_chunks(self):
        """
        Test that iter_catalog_chunks returns the same results as write_catalog
        """

        obs = ObservationMetaData(pointingRA=10.0, pointingDec=-20.0,
                                  boundLength=50.0, boundType='circle')

        cat = BasicCatalog(self.starDB, obs_metadata=obs)
        cat_name = os.path.join(self.scratch_dir, 'iter_catalog_chunks_control.txt')
        cat.write_catalog(cat_name)
        with open(cat_name, 'r') as in_file:
            in_lines = in_file.readlines()
        self.assertGreater(len(in_lines), 1)
        self.assertLess(len(in_lines), len(self.starControlData))

        cat = BasicCatalog(self.starDB, obs_metadata=obs)
        line_ct = 0
        for chunk, chunk_map in cat.iter_catalog_chunks(chunk_size=7):
            for ix in range(len(chunk[0])):
                str_line = '%d, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f, %.12f\n' % \
                (chunk[0][ix], chunk[1][ix], chunk[2][ix], chunk[3][ix], chunk[4][ix],
                 chunk[5][ix], chunk[6][ix], chunk[7][ix], chunk[8][ix])

                self.assertIn(str_line, in_lines)
                line_ct += 1

        self.assertEqual(line_ct, len(in_lines)-1)

        if os.path.exists(cat_name):
            os.unlink(cat_name)



class boundingBoxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = tempfile.mkdtemp(dir=ROOT, prefix="scratchSpace-")
        cls.starTextName = os.path.join(cls.scratch_dir,
                                        'bbStarTestCatalog.txt')

        cls.starDB, cls.starControlData = write_star_file_db(cls.starTextName)

    @classmethod
    def tearDownClass(cls):
        sims_clean_up()
        if os.path.exists(cls.starTextName):
            os.unlink(cls.starTextName)
        for file_name in os.listdir(cls.scratch_dir):
            os.unlink(os.path.join(cls.scratch_dir, file_name))
        if os.path.exists(cls.scratch_dir):
            shutil.rmtree(cls.scratch_dir)

    def setUp(self):

        self.RAmin = 190.
        self.RAmax = 210.
        self.DECmin = -70.
        self.DECmax = -50.

        self.RAcenter = 200.
        self.DECcenter = -60.
        self.radius = 10.0

        self.obsMdCirc = ObservationMetaData(boundType='circle',
                                             pointingRA=self.RAcenter,
                                             pointingDec=self.DECcenter,
                                             boundLength=self.radius, mjd=52000., bandpassName='r')

        self.obsMdBox = ObservationMetaData(boundType='box', pointingRA=0.5*(self.RAmax+self.RAmin),
                                            pointingDec=0.5*(self.DECmin+self.DECmax),
                                            boundLength=np.array([0.5*(self.RAmax-self.RAmin),
                                                                  0.5*(self.DECmax-self.DECmin)]),
                                            mjd=52000., bandpassName='r')

    def testBoxBounds(self):
        """
        Make sure that box_bound_constraint in sims.catalogs.db.dbConnection.py
        does not admit any objects outside of the bounding box
        """

        catName = os.path.join(self.scratch_dir, 'box_test_catalog.txt')

        myCatalog = self.starDB.getCatalog('bounds_catalog', obs_metadata = self.obsMdBox)

        myIterator = myCatalog.iter_catalog(chunk_size=10)

        for line in myIterator:
            self.assertGreater(line[1], self.RAmin)
            self.assertLess(line[1], self.RAmax)
            self.assertGreater(line[2], self.DECmin)
            self.assertLess(line[2], self.DECmax)

        myCatalog.write_catalog(catName)

        # now we will test for the completeness of the box bounds

        dtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float)])

        testData = np.genfromtxt(catName, dtype=dtype, delimiter=', ')

        for line in testData:
            self.assertGreater(line['raJ2000'], self.RAmin)
            self.assertGreater(line['decJ2000'], self.DECmin)
            self.assertLess(line['raJ2000'], self.RAmax)
            self.assertLess(line['decJ2000'], self.DECmax)

        ct = 0
        for line in self.starControlData:
            if line['id'] not in testData['id']:
                ct += 1
                in_bounds = ((line['raJ2000'] < self.RAmax) and (line['raJ2000'] > self.RAmin) and
                             (line['decJ2000'] < self.DECmax) and (line['decJ2000'] > self.DECmin))

                msg = 'violates bounds\nRA: %e < %e <%e\nDec: %e < %e < %e\n' % \
                      (self.RAmin, line['raJ2000'], self.RAmax,
                       self.DECmin, line['decJ2000'], self.DECmax)

                self.assertFalse(in_bounds, msg=msg)

        self.assertGreater(ct, 0)

        if os.path.exists(catName):
            os.unlink(catName)

    def testCircBounds(self):

        """
        Make sure that circular_bound_constraint in sims.catalogs.db.dbConnection.py
        does not admit any objects outside of the bounding circle
        """

        catName = os.path.join(self.scratch_dir,
                               'circular_test_catalog.txt')

        if os.path.exists(catName):
            os.unlink(catName)

        myCatalog = self.starDB.getCatalog('bounds_catalog', obs_metadata = self.obsMdCirc)
        myIterator = myCatalog.iter_catalog(chunk_size=10)

        for line in myIterator:
            rtest = np.degrees(haversine(np.radians(self.RAcenter), np.radians(self.DECcenter),
                                         np.radians(line[1]), np.radians(line[2])))

            self.assertLess(rtest, self.radius)

        myCatalog.write_catalog(catName)

        # now we will test for the completeness of the circular bounds

        dtype = np.dtype([('id', np.int),
                          ('raJ2000', np.float),
                          ('decJ2000', np.float)])

        testData = np.genfromtxt(catName, dtype=dtype, delimiter=', ')

        self.assertGreater(len(testData), 0)

        for line in testData:
            dl = np.degrees(haversine(np.radians(line['raJ2000']), np.radians(line['decJ2000']),
                                      np.radians(self.RAcenter), np.radians(self.DECcenter)))

            self.assertLess(dl, self.radius)

        ct = 0
        for line in self.starControlData:
            if line['id'] not in testData['id']:
                ct += 1
                dl = np.degrees(haversine(np.radians(line['raJ2000']), np.radians(line['decJ2000']),
                                          np.radians(self.RAcenter), np.radians(self.DECcenter)))

                self.assertGreater(dl, self.radius)

        self.assertGreater(ct, 0)

        if os.path.exists(catName):
            os.unlink(catName)

    def test_negative_RA(self):
        """
        Test that spatial queries behave correctly around RA=0
        """
        rng = np.random.RandomState(81234122)
        db_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='negRA', suffix='.db')[1]
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE neg_ra_table
                           (cat_id int, ra real, dec real)''')

            connection.commit()
            n_samples = 1000
            id_val = np.arange(n_samples, dtype=int) + 1
            ra = 10.0*(rng.random_sample(n_samples)-0.5)
            dec = rng.random_sample(n_samples)-0.5
            values = ((int(ii), rr, dd) for ii, rr, dd in zip(id_val, ra, dec))
            cursor.executemany('''INSERT INTO neg_ra_table VALUES (?, ?, ?)''', values)
            connection.commit()

        class negativeRaCatalogDBClass(CatalogDBObject):
            tableid = 'neg_ra_table'
            idColKey = 'cat_id'
            raColName = 'ra'
            decColName = 'dec'
            objectTypeId = 126

        class negativeRaCatalogClass(InstanceCatalog):
            column_outputs = ['cat_id', 'ra', 'dec']
            delimiter = ' '

        db = negativeRaCatalogDBClass(database=db_name, driver='sqlite')

        boundLength=0.2
        pra = 359.9
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='circle', boundLength=boundLength)


        cat = negativeRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='negRa', suffix='.txt')[1]

        cat.write_catalog(cat_name)
        valid = np.where(angularSeparation(pra, pdec, ra, dec)<boundLength)
        self.assertGreater(len(valid[0]), 0)
        self.assertLess(len(valid[0]), n_samples)
        valid_pos = np.where(np.logical_and(angularSeparation(pra, pdec, ra, dec)<boundLength,
                                             ra>0.0))
        valid_neg = np.where(np.logical_and(angularSeparation(pra, pdec, ra, dec)<boundLength,
                                             ra<0.0))
        self.assertGreater(len(valid_pos[0]), 0)
        self.assertGreater(len(valid_neg[0]), 0)
        self.assertLess(len(valid_pos[0]), len(valid[0]))
        valid_id = id_val[valid]
        valid_ra = ra[valid]
        valid_dec = dec[valid]

        cat_dtype = np.dtype([('cat_id', int), ('ra', float), ('dec', float)])
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # now try it when RA is specified as negative
        pra = -0.1
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='circle', boundLength=boundLength)

        cat = negativeRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='negRa', suffix='.txt')[1]

        cat.write_catalog(cat_name)

        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # test it on a box
        pra = 359.9
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='box', boundLength=boundLength)

        cat = negativeRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='negRa', suffix='.txt')[1]

        dec_min = pdec-boundLength
        dec_max = pdec+boundLength

        valid_id = []
        valid_ra = []
        valid_dec = []
        for rr, dd, ii in zip(ra, dec, id_val):
            if dd>dec_max or dd<dec_min:
                continue
            if np.abs(rr+0.1)<boundLength:
                valid_id.append(ii)
                valid_ra.append(rr)
                valid_dec.append(dd)
        valid_id = np.array(valid_id)
        valid_ra = np.array(valid_ra)
        valid_dec = np.array(valid_dec)

        cat.write_catalog(cat_name)
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # try when defined at negative
        pra = -0.1
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='box', boundLength=boundLength)

        cat = negativeRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='negRa', suffix='.txt')[1]
        cat.write_catalog(cat_name)
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)
        del db
        if os.path.exists(db_name):
            os.unlink(db_name)

    def test_very_positive_RA(self):
        """
        Test that spatial queries behave correctly around RA=0 (when RA>350)
        """
        rng = np.random.RandomState(81234122)
        db_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='posRA', suffix='.db')[1]
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE neg_ra_table
                           (cat_id int, ra real, dec real)''')

            connection.commit()
            n_samples = 1000
            id_val = np.arange(n_samples, dtype=int) + 1
            ra = 10.0*(rng.random_sample(n_samples)-0.5)
            neg_dex = np.where(ra<0.0)
            ra[neg_dex] += 360.0
            dec = rng.random_sample(n_samples)-0.5
            values = ((int(ii), rr, dd) for ii, rr, dd in zip(id_val, ra, dec))
            cursor.executemany('''INSERT INTO neg_ra_table VALUES (?, ?, ?)''', values)
            connection.commit()

        class veryPositiveRaCatalogDBClass(CatalogDBObject):
            tableid = 'neg_ra_table'
            idColKey = 'cat_id'
            raColName = 'ra'
            decColName = 'dec'
            objectTypeId = 126

        class veryPositiveRaCatalogClass(InstanceCatalog):
            column_outputs = ['cat_id', 'ra', 'dec']
            delimiter = ' '

        db = veryPositiveRaCatalogDBClass(database=db_name, driver='sqlite')

        boundLength=0.2
        pra = 359.9
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='circle', boundLength=boundLength)


        cat = veryPositiveRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='posRa', suffix='.txt')[1]

        cat.write_catalog(cat_name)
        valid = np.where(angularSeparation(pra, pdec, ra, dec)<boundLength)
        self.assertGreater(len(valid[0]), 0)
        self.assertLess(len(valid[0]), n_samples)
        valid_pos = np.where(np.logical_and(angularSeparation(pra, pdec, ra, dec)<boundLength,
                                             ra<350.0))
        valid_neg = np.where(np.logical_and(angularSeparation(pra, pdec, ra, dec)<boundLength,
                                             ra>350.0))
        self.assertGreater(len(valid_pos[0]), 0)
        self.assertGreater(len(valid_neg[0]), 0)
        self.assertLess(len(valid_pos[0]), len(valid[0]))
        valid_id = id_val[valid]
        valid_ra = ra[valid]
        valid_dec = dec[valid]

        cat_dtype = np.dtype([('cat_id', int), ('ra', float), ('dec', float)])
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # now try it when RA is specified as negative
        pra = -0.1
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='circle', boundLength=boundLength)

        cat = veryPositiveRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='posRa', suffix='.txt')[1]

        cat.write_catalog(cat_name)

        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # test it on a box
        pra = 359.9
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='box', boundLength=boundLength)

        cat = veryPositiveRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='posRa', suffix='.txt')[1]

        dec_min = pdec-boundLength
        dec_max = pdec+boundLength

        valid_id = []
        valid_ra = []
        valid_dec = []
        for rr, dd, ii in zip(ra, dec, id_val):
            if dd>dec_max or dd<dec_min:
                continue
            if np.abs(rr-359.9)<boundLength  or (rr+0.1)<boundLength:
                valid_id.append(ii)
                valid_ra.append(rr)
                valid_dec.append(dd)
        valid_id = np.array(valid_id)
        valid_ra = np.array(valid_ra)
        valid_dec = np.array(valid_dec)

        cat.write_catalog(cat_name)
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)

        # try when defined at negative
        pra = -0.1
        pdec = 0.0
        obs = ObservationMetaData(pointingRA=pra, pointingDec=pdec,
                                  boundType='box', boundLength=boundLength)

        cat = veryPositiveRaCatalogClass(db, obs_metadata=obs)
        cat_name = tempfile.mkstemp(dir=self.scratch_dir, prefix='posRa', suffix='.txt')[1]
        cat.write_catalog(cat_name)
        cat_data = np.genfromtxt(cat_name, dtype=cat_dtype)
        np.testing.assert_array_equal(cat_data['cat_id'], valid_id)
        np.testing.assert_array_almost_equal(cat_data['ra'], valid_ra, decimal=3)
        np.testing.assert_array_almost_equal(cat_data['dec'], valid_dec, decimal=3)
        del db
        if os.path.exists(db_name):
            os.unlink(db_name)


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
