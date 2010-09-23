#

import os

# Set up timing information and simple routine. 
import time
def dtime(time_prev):
   return (time.time() - time_prev, time.time())

# Import other modules and classes. 
import numpy
import lsst.sims.catalogs.measures.photometry.Sed as Sed
import lsst.sims.catalogs.measures.photometry.Bandpass as Bandpass

# Initialize starting time.
t = time.time()

# Set the wavelength step for bandpasses. 
#wavelen_step = 0.1
wavelen_step = 0.25

# Read in LSST bandpasses.
bpdir = os.getenv("LSST_THROUGHPUTS_BASELINE")
filterlist = ('u', 'g', 'r', 'i', 'z', 'y')
lsstbp = {}
for f in filterlist:
    lsstbp[f] = Bandpass()
    lsstbp[f].readThroughput(os.path.join(bpdir, 'total_'+f+'.dat'), wavelen_step=wavelen_step)
dt, t = dtime(t)
print "Reading %d filters took %f s" %(len(filterlist), dt)

# Read in galaxy seds. (there aren't many, so just read them all).
# Replace galdir with your root galaxy sed directory.
galdir = "/Users/rhiannonjones/seds/galaxies/"
gals = {}
gallist = os.listdir(galdir)
for gal in gallist:
    gals[gal] = Sed()
    gals[gal].readSED_flambda(os.path.join(galdir,gal))

dt, t = dtime(t)
print "Reading %d galaxy seds took %f s" %(len(gallist), dt)

# Check on resampling - want all galaxy seds to have the same wavelength range.
if ((gals[gallist[0]].wavelen.min() < 30) & (gals[gallist[0]].wavelen.max() > 2000)):
    # If true, then gals[gallist[0]] is okay to use as a template -- this ought to be true.
    wavelen_match = gals[gallist[0]].wavelen
else:
    print "Had to use simple wavelength array for matching"
    wavelen_match = numpy.arange(30, 2200, 0.1, dtype='float')
for gal in gallist:
    if gals[gal].needResample(wavelen_match = wavelen_match):
        gals[gal].resampleSED(wavelen_match = wavelen_match)

dt, t = dtime(t)
print "Checking (and potentially doing) resampling took %f s" %(dt)

# Generate fake redshift and dust info for 10,000 'galaxies' that would be returned from galaxy info query.
# Although we have only 960 galaxy seds, we will likely want to calculate magnitudes for many more galaxies.
# So here, the test is how long to calculate magnitudes for 10,000 different galaxies?
num_gal = 10000
ebv_int = numpy.random.rand(num_gal)
redshifts = numpy.random.rand(num_gal) * 10.0
ebv_mw = numpy.random.rand(num_gal)  # random 0-1
gal_name = numpy.random.rand(num_gal) * len(gallist)  # 'assign' sed names
gal_name = numpy.array(gal_name, dtype='int')
fluxnorm = (numpy.random.rand(num_gal) + 2) * 1e-14

dt, t = dtime(t)
print "Picking random numbers for ebv/redshift, etc took %f s" %(dt)

# First - start 'regular' (but slightly slower) method of calculating magnitudes for galaxies, for comparison.
# If you're only calculating magnitudes for a few galaxies, this might actually be just as fast and
# would likely be easier to code/read, so is useful as a comparison.
# Actual difference in timing between this method and the next (more optimized) method can be determined
# by simply running 'python example_fastgals.py', but on my mac it was about 1.5 times faster optimized, with
# wavelen_step = 0.1 nm. (wavelen_step will have an impact on the speed difference). 

# Calculate internal a/b on the wavelength range required for calculating internal dust extinction. 
a_int, b_int = gals[gallist[0]].setupCCMab()
# Set up dictionary + arrays to hold calculated magnitude information. 
mags1 = {}
for f in filterlist:
    mags1[f] = numpy.zeros(num_gal, dtype='float')
# For each galaxy (in num_gal's), apply internal dust, redshift, apply MW dust, fluxnorm & calculate mags. 
for i in range(num_gal):
    galname = gallist[gal_name[i]]
    tmpgal = Sed(wavelen=gals[galname].wavelen, flambda=gals[galname].flambda)
    tmpgal.addCCMDust(a_int, b_int, ebv=ebv_int[i])
    tmpgal.redshiftSED(redshifts[i])
    a_mw, b_mw = tmpgal.setupCCMab()
    tmpgal.addCCMDust(a_mw, b_mw, ebv=ebv_mw[i])
    tmpgal.multiplyFluxNorm(fluxnorm[i])
    # If you comment out the synchronize sed here, then the difference between this method and the optimized
    # version increases to a 2.5 times difference.  (i.e. this 'synchronizeSED' buys you 1.5x faster, by itself.)
    tmpgal.synchronizeSED(wavelen_min=lsstbp[filterlist[0]].wavelen.min(),
                          wavelen_max=lsstbp[filterlist[0]].wavelen.max(),
                          wavelen_step = lsstbp[filterlist[0]].wavelen[1] - lsstbp[filterlist[0]].wavelen[0])
    for f in filterlist:
        mags1[f][i] = tmpgal.calcMag(lsstbp[f])
dt, t = dtime(t)
print "Calculating dust/redshift/dust/fluxnorm/%d magnitudes for %d galaxies took %f s" \
      %(len(filterlist), num_gal, dt)


# For next test: want to also do all the same steps, but in severely optimized form. This means
# doing some things that Sed does 'behind the scenes' explicitly, but also means the code may be a little
# harder to read at first.
# First: calculate internal a/b on wavelength range required for internal dust extinction.
a_int, b_int = gals[gallist[0]].setupCCMab()  # this is a/b on native galaxy sed range. 
# Next: calculate milky way a/b on wavelength range required for calculating magnitudes - i.e. 300 to 1200 nm.
tmpgal = Sed()
tmpgal.setFlatSED(wavelen_min=300, wavelen_max=1200, wavelen_step=wavelen_step)   # initializes tmpgal on range 300-1200 nm
a_mw, b_mw = tmpgal.setupCCMab()  # so this is a/b on 300-1200 range. 
# Also: set up phi for each bandpass - ahead of time. And set up a list of bandpasses, for manyMagCalc method.
bplist = []
for f in filterlist:
    lsstbp[f].sbTophi()
    bplist.append(lsstbp[f])
# Set up dictionary + arrays to hold calculated magnitude information. 
mags2 = {}
for f in filterlist:
    mags2[f] = numpy.zeros(num_gal, dtype='float')
# For each galaxy (in num_gal's), apply internal dust, redshift, resample to 300-1200 nm, apply MW dust on
#   shorter (and standardized) wavelength range, fluxnorm, and then calculate mags using manyMagCalc. 
for i in range(num_gal):
    galname = gallist[gal_name[i]]
    tmpgal = Sed(wavelen=gals[galname].wavelen, flambda=gals[galname].flambda)
    tmpgal.addCCMDust(a_int, b_int, ebv=ebv_int[i])
    tmpgal.redshiftSED(redshifts[i])
    tmpgal.resampleSED(wavelen_min=300, wavelen_max=1200, wavelen_step=wavelen_step)
    tmpgal.addCCMDust(a_mw, b_mw, ebv=ebv_mw[i])
    tmpgal.multiplyFluxNorm(fluxnorm[i])
    tmpmags = tmpgal.manyMagCalc(bplist)
    j = 0
    for f in filterlist:
        mags2[f][i] = tmpmags[j]
        j = j+1
dt, t = dtime(t)
print "Calculating dust/redshift/dust/fluxnorm/%d magnitudes for %d galaxies optimized way took %f s" \
      %(len(filterlist), num_gal, dt)


# Check for differences in magnitudes.
import pylab
pylab.figure()
colors = ['m', 'g', 'r', 'b', 'k', 'y']
i = 0
diff = {}
for f in filterlist:
   diff[f] = numpy.zeros(num_gal, dtype='float')
   diff[f] = numpy.abs(mags1[f] - mags2[f])
   condition  = (diff[f]>0.01)
   print f, diff[f][condition], redshifts[condition], fluxnorm[condition], mags1[f][condition], mags2[f][condition]
   #print f, diff[f].min(), diff[f].max(), mags1[f].max(), mags2[f].max(), len(mags1[f]), len(mags2[f])
   pylab.plot(mags1[f], mags2[f], colors[i]+'.')
   i = i + 1
x = numpy.arange(10, 35, 1)
pylab.plot(x, x, 'k-')
pylab.show()
