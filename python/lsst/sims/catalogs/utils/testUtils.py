from builtins import next
from builtins import str
from builtins import range
import sqlite3
import numpy as np
import json

from lsst.sims.catalogs.db import CatalogDBObject

__all__ = ["getOneChunk", "writeResult", "sampleSphere", "myTestGals",
           "makeGalTestDB", "myTestStars", "makeStarTestDB"]


def getOneChunk(results):
    try:
        chunk = next(results)
    except StopIteration:
        raise RuntimeError("No results were returned.  Cannot run tests.  Try increasing the size of the"
                           " test database")
    return chunk


def writeResult(result, fname):
    fh = open(fname, 'w')
    first = True
    for chunk in result:
        if first:
            fh.write(",".join([str(el) for el in chunk.dtype.names])+"\n")
            first = False
        for i in range(len(chunk)):
            fh.write(",".join([str(chunk[name][i]) for name in chunk.dtype.names])+"\n")
    fh.close()


def sampleSphere(size, ramin = 0., dra = 2.*np.pi, rng=None):
    # From Shao 1996: "Spherical Sampling by Archimedes' Theorem"
    if rng is None:
        rng = np.random.RandomState(42)

    ra = rng.random_sample(size)*dra
    ra += ramin
    ra %= 2*np.pi
    z = rng.random_sample(size)*2. - 1.
    dec = np.arccos(z) - np.pi/2.
    return ra, dec


def sampleFocus(size, raCenter, decCenter, radius, rng=None):
    """
    Sample points in a focused field of view
    @param [in] raCenter is the RA at the center of the field of view in radians
    @param [in] decCenter is the Dec at the center of the field of view in radians
    @param [in] radius is the radius of the field of view in radians
    @param [in] rng is a random number generator (an instance of np.random.RandomState)
    @param [out] returns numpy arrays of ra and decs in radians
    """
    if rng is None:
        rng = np.random.RandomState(1453)

    theta = rng.random_sample(size)
    rc = np.radians(raCenter)
    dc = np.radians(decCenter)
    rr = np.radians(radius)*rng.random_sample(size)
    ra = np.empty(size)
    dec = np.empty(size)
    for i, th in enumerate(theta):
        ra[i] = rc + rr*np.cos(th)
        dec[i] = dc + rr*np.sin(th)

    return ra, dec


class myTestGals(CatalogDBObject):
    objid = 'testgals'
    tableid = 'galaxies'
    idColKey = 'id'
    # Make this implausibly large?
    appendint = 1022
    objectTypeId = 45
    driver = 'sqlite'
    database = 'testDatabase.db'
    raColName = 'ra'
    decColName = 'decl'
    spatialModel = 'SERSIC2D'
    columns = [('id', None, int),
               ('raJ2000', 'ra*%f'%(np.pi/180.)),
               ('decJ2000', 'decl*%f'%(np.pi/180.)),
               ('umag', None),
               ('gmag', None),
               ('rmag', None),
               ('imag', None),
               ('zmag', None),
               ('ymag', None),
               ('magNormAgn', 'mag_norm_agn', None),
               ('magNormDisk', 'mag_norm_disk', None),
               ('magNormBulge', 'mag_norm_bulge', None),
               ('redshift', None),
               ('a_disk', None),
               ('b_disk', None),
               ('a_bulge', None),
               ('b_bulge', None)]


def makeGalTestDB(filename='testDatabase.db', size=1000, seedVal=None,
                  raCenter=None, decCenter=None, radius=None, **kwargs):
    """
    Make a test database to serve information to the myTestGals object
    @param size: Number of rows in the database
    @param seedVal: Random seed to use

    @param raCenter,decCenter: the center of the field of view in degrees (optional)
    @param radius: the radius of the field of view in degrees (optional)

    These last optional parameters exist in the event that you want to make sure
    that the objects are clustered around the bore site for a unit test
    """
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE galaxies
                  (id int, ra real, decl real, umag real, gmag real, rmag real,
                  imag real, zmag real, ymag real,
                  mag_norm_agn real, mag_norm_bulge real, mag_norm_disk real,
                  redshift real, a_disk real, b_disk real, a_bulge real, b_bulge real, varParamStr text)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")

    if seedVal is not None:
        rng = np.random.RandomState(seedVal)
    else:
        rng = np.random.RandomState(3321)

    if raCenter is None or decCenter is None or radius is None:

        ra, dec = sampleSphere(size, rng=rng, **kwargs)
    else:
        rc = np.radians(raCenter)
        dc = np.radians(decCenter)
        ra, dec = sampleFocus(size, rc, dc, radius, rng=rng)
    # Typical colors for main sequece stars
    umg = 1.5
    gmr = 0.65
    rmi = 1.0
    imz = 0.45
    zmy = 0.3
    mag_norm_disk = rng.random_sample(size)*6. + 18.
    mag_norm_bulge = rng.random_sample(size)*6. + 18.
    mag_norm_agn = rng.random_sample(size)*6. + 19.
    redshift = rng.random_sample(size)*2.5

    a_disk = rng.random_sample(size)*2.
    flatness = rng.random_sample(size)*0.8  # To prevent linear galaxies
    b_disk = a_disk*(1 - flatness)

    a_bulge = rng.random_sample(size)*1.5
    flatness = rng.random_sample(size)*0.5
    b_bulge = a_bulge*(1 - flatness)

    # assume mag norm is g-band (which is close to true)
    mag_norm = -2.5*np.log10(np.power(10, mag_norm_disk/-2.5) + np.power(10, mag_norm_bulge/-2.5) +
                             np.power(10, mag_norm_agn/-2.5))
    umag = mag_norm + umg
    gmag = mag_norm
    rmag = gmag - gmr
    imag = rmag - rmi
    zmag = imag - imz
    ymag = zmag - zmy
    for i in range(size):
        period = rng.random_sample(1)[0]*490. + 10.
        amp = rng.random_sample(1)[0]*5. + 0.2
        varParam = {'varMethodName': 'testVar', 'pars': {'period': period, 'amplitude': amp}}
        paramStr = json.dumps(varParam)
        qstr = '''INSERT INTO galaxies VALUES (%i, %f, %f, %f,
                     %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,
                     %f, %f, '%s')''' % \
               (i, np.degrees(ra[i]), np.degrees(dec[i]), umag[i], gmag[i], rmag[i], imag[i],
                zmag[i], ymag[i], mag_norm_agn[i], mag_norm_bulge[i], mag_norm_disk[i], redshift[i],
                a_disk[i], b_disk[i], a_bulge[i], b_bulge[i], paramStr)

        c.execute(qstr)

    c.execute('''CREATE INDEX gal_ra_idx ON galaxies (ra)''')
    c.execute('''CREATE INDEX gal_dec_idx ON galaxies (decl)''')
    conn.commit()
    conn.close()


class myTestStars(CatalogDBObject):
    objid = 'teststars'
    tableid = 'stars'
    idColKey = 'id'
    # Make this implausibly large?
    appendint = 1023
    objectTypeId = 46
    driver = 'sqlite'
    database = 'testDatabase.db'
    raColName = 'ra'
    decColName = 'decl'
    columns = [('id', None, int),
               ('raJ2000', 'ra*%f'%(np.pi/180.)),
               ('decJ2000', 'decl*%f'%(np.pi/180.)),
               ('parallax', 'parallax*%.15f'%(np.pi/(648000000.0))),
               ('properMotionRa', 'properMotionRa*%.15f'%(np.pi/180)),
               ('properMotionDec', 'properMotionDec*%.15f'%(np.pi/180.)),
               ('umag', None),
               ('gmag', None),
               ('rmag', None),
               ('imag', None),
               ('zmag', None),
               ('ymag', None),
               ('magNorm', 'mag_norm', float)]


def makeStarTestDB(filename='testDatabase.db', size=1000, seedVal=None,
                   raCenter=None, decCenter=None, radius=None, **kwargs):
    """
    Make a test database to serve information to the myTestStars object
    @param size: Number of rows in the database
    @param seedVal: Random seed to use

    @param raCenter,decCenter: the center of the field of view in degrees (optional)
    @param radius: the radius of the field of view in degrees (optional)

    These last optional parameters exist in the event that you want to make sure
    that the objects are clustered around the bore site for a unit test
    """
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE stars
                     (id int, ra real, decl real, umag real, gmag real, rmag real,
                     imag real, zmag real, ymag real, mag_norm real,
                     radialVelocity real, properMotionDec real, properMotionRa real, parallax real,
                     varParamStr text, ebv real)''')
        conn.commit()
    except:
        raise RuntimeError("Error creating database.")

    if seedVal is not None:
        rng = np.random.RandomState(seedVal)
    else:
        rng = np.random.RandomState(88)

    if raCenter is None or decCenter is None or radius is None:
        ra, dec = sampleSphere(size, rng=rng, **kwargs)
    else:
        rc = np.radians(raCenter)
        dc = np.radians(decCenter)
        ra, dec = sampleFocus(size, rc, dc, radius, rng=rng)

    # Typical colors
    umg = 1.5
    gmr = 0.65
    rmi = 1.0
    imz = 0.45
    zmy = 0.3
    mag_norm = rng.random_sample(size)*6. + 18.
    # assume mag norm is g-band (which is close to true)
    umag = mag_norm + umg
    gmag = mag_norm
    rmag = gmag - gmr
    imag = rmag - rmi
    zmag = imag - imz
    ymag = zmag - zmy
    radVel = rng.random_sample(size)*50. - 25.
    pmRa = rng.random_sample(size)*4./(1000*3600.)  # deg/yr
    pmDec = rng.random_sample(size)*4./(1000*3600.)  # deg/yr
    parallax = rng.random_sample(size)*1.0  # milliarcseconds per year
    ebv = rng.random_sample(size)*3.0
    for i in range(size):
        period = rng.random_sample(1)[0]*490. + 10.
        amp = rng.random_sample(1)[0]*5. + 0.2
        varParam = {'varMethodName': 'testVar', 'pars': {'period': period, 'amplitude': amp}}
        paramStr = json.dumps(varParam)
        qstr = '''INSERT INTO stars VALUES (%i, %f, %f, %f, %f, %f, %f,
               %f, %f, %f, %f, %.15f, %.15f, %.15f, '%s', %f)''' % \
               (i, np.degrees(ra[i]), np.degrees(dec[i]), umag[i], gmag[i], rmag[i],
                imag[i], zmag[i], ymag[i], mag_norm[i], radVel[i], pmRa[i], pmDec[i], parallax[i],
                paramStr, ebv[i])

        c.execute(qstr)

    c.execute('''CREATE INDEX star_ra_idx ON stars (ra)''')
    c.execute('''CREATE INDEX star_dec_idx ON stars (decl)''')
    conn.commit()
    conn.close()
