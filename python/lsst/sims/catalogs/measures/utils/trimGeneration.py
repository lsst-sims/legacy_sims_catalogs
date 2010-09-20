""" TrimGeneration

    ajc@astro June 30 2010
    Methods to generate trim file parameters - including metadata

"""

import numpy
import warnings

from lsst.sims.catalogs.measures.astrometry.Astrometry import *
from lsst.sims.catalogs.measures.photometry.Bandpass import *
from lsst.sims.catalogs.measures.photometry.Sed import *
from lsst.sims.catalogs.measures.photometry.Magnitudes import *
from lsst.sims.catalogs.measures.instance.CatalogDescription import *
from lsst.sims.catalogs.measures.instance.InstanceCatalog import *
from lsst.sims.catalogs.measures.instance.Metadata import *


#load Slalib DLLs
slalib = ctypes.CDLL("slalsst.so")

def derivedTrimMetadata(instanceCatalog):
    '''Generate metadata needed for Trim files'''

    # calculate Slalib_date from Opsim_expmjd
    year=ctypes.c_int(0)
    month=ctypes.c_int(0)
    day=ctypes.c_int(0)
    fractionDay=ctypes.c_double(0.0)
    status=ctypes.c_int(0)


    mjd = float(instanceCatalog.metadata.parameters['Opsim_expmjd'])
    
    # slalib_date
    slalib.slaDjcl.argtypes = [ctypes.c_double,ctypes.POINTER(ctypes.c_int),ctypes.POINTER(ctypes.c_int),ctypes.POINTER(ctypes.c_int),ctypes.POINTER(ctypes.c_double),ctypes.POINTER(ctypes.c_int)]
    slalib.slaDjcl(mjd, year, month, day, fractionDay, status)

    slalibDate = "%4d/%02d/%02d/%.9g" % (year.value,month.value,day.value, fractionDay.value)
    instanceCatalog.metadata.addMetadata("Slalib_date", slalibDate, "Factional date from Slalib")

#Slalib_date  1994/10/12/0.0945639999991
#Slalib_expMjd 49637.094564

if __name__ == '__main__':
    derivedTrimMetadata()
    
