import numpy
import warnings

from lsst.sims.catalogs.measures.astrometry.Astrometry import *
from lsst.sims.catalogs.measures.photometry.Bandpass import *
from lsst.sims.catalogs.measures.photometry.Sed import *
from lsst.sims.catalogs.measures.photometry.Magnitudes import *
from lsst.sims.catalogs.measures.instance.CatalogDescription import *
from lsst.sims.catalogs.measures.instance.InstanceCatalog import *
from lsst.sims.catalogs.measures.instance.Metadata import *

slalib = ctypes.CDLL("slalsst.so")

def equToGalactic(raEqu, decEqu):
    """ Convert Equatorial to Galactic Coords """
    slalib.slaEqgal.argtypes= [ ctypes.c_double, ctypes.c_double, 
                                ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double)]

    _glon = ctypes.c_double(0.)
    _glat = ctypes.c_double(0.)

    gLong = numpy.zeros(len(raEqu))
    gLat = numpy.zeros(len(raEqu))
    for i in numpy.arange(len(raEqu)):
        slalib.slaEqgal(raEqu[i], decEqu[i], _glon, _glat)
        gLong[i] = _glon.value
        gLat[i] = _glat.value

    return gLong, gLat
