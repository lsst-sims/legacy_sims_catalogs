#  Most common python modules are accepted.  This will be run using Python
#  2.7.2.  scipy and numpy are both supported.
#  This method must take a dictionary of variability parameters and
#  observation time(s) in MJD.  The return must be a list of offsets in
#  magnitude per band for each of the input observation times.
import numpy
def applyMySpecialVariability(params, expmjd):
    datadir = "lightcurves"  #relative to base directory this can be modified to any path
    expmjd = numpy.asarray(expmjd)
    filename = params['lcfile']
    toff = float(params['t0'])
    period = float(params['period'])
    lc = numpy.loadtxt(datadir+"/"+filename, unpack=True, comments='#')
    #In this example the lc is in units of phase from 0 to 1
    epoch = expmjd - toff
    phase = epoch/period - epoch//period

    magoff = {}
    for k in ('u','g','r','i','z','y'): magoff[k] = numpy.interp(phase, lc[0], lc[1])
    return magoff

