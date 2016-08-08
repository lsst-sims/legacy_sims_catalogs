# execfile('compareRefEImage.py')

execfile('r.py')

import os, sys, gzip, numpy, random
import lsst.afw.detection as afwDetection
import lsst.afw.image as afwImage
import lsst.afw.display.ds9 as ds9
import lsst.meas.astrom.sip as sip
import simplePlot, pyCommon, matchTrimToDet
from lsst.sims.catalogs.measures.astrometry.Astrometry import *

refMagThresh = 23
trimMagThresh = 23
# Extra pixels to allow for sources to be moved onto the chip
chipBorder = 300

#eFile = sys.argv[1]; tFile = sys.argv[2]

#eFile = 'v85501858fiR22S00.fits.gz'
eFile = 'eimage_85501858_f3_R22_S00_E000.fits.gz'
tFile = 'trim2544_85501858_0StarsAdded'

#eFile = 'v85471033fgR22S00.fits.gz'
#eFile = 'eimage_85471033_f1_R22_S00_E000.fits.gz'
#tFile = 'trim2762_85471033_0StarsAdded'

#eFile = 'v85471150fiR22S00.fits.gz'
#eFile = 'eimage_85471150_f3_R22_S00_E000.fits.gz'
#tFile = 'trim2544_85471150_0StarsAdded.gz'

#eFile = 'eimage_85661428_f1_R34_S10_E000.fits.gz'
#tFile = 'trim2543_85661428_0StarsAdded.gz'

#eFile = 'v85616039fyR22S00.fits.gz'
#eFile = 'eimage_85616039_R22_S00_E000.fits.gz'
#tFile = 'trim2544_85616039_0StarsAdded.gz'

print 'eimage file:', eFile
print 'trim file:', tFile
outDir = './out%s/' % eFile
if not os.path.exists(outDir): os.system('mkdir ' + outDir)
else:
    if os.path.exists(outDir + 'wCS.dat'):
        print '*** %s is already done; skipping.' % outDir
        sys.exit(0)

outLink = outDir + eFile
if not os.path.exists(outLink):
    os.system('ln -s ../%s %s' % (eFile, outLink))

#eFile = '/astro/net/pogo1/rgibson/test85661428r22s21/eimage_85661428_f1_R22_S21_E000.fits'
#tFile = '/astro/net/pogo1/rgibson/test85661428r22s21/trim2543_85661428_0StarsAdded'
#tFile = 'onChipTrim.txt'

#eFile = sys.argv[1]; tFile = sys.argv[2]
#eFile = 'eimage_85563138_f4_R34_S12_E000.fits.gz'; tFile = 'onChipTrim.txt'
#eFile = 'eimage_85563138_f4_R34_S02_E000.fits.gz'; tFile = '/astro/net/pogo1/patchedTrim/moreFields/trim2770_85563138_0StarsAdded.gz'
#eFile = 'eimage_85661428_f1_R22_S21_E000SuzanneNewFullFixed.fits'; tFile = '/astro/net/pogo1/patchedTrim/out/trim2543_85661428_0StarsAdded.gz'
rFile = 'star_ref_08162010.dat.gz'
if not os.path.exists(eFile):
    raise RuntimeError, '*** No such file: %s' % eFile
if not os.path.exists(tFile):
    raise RuntimeError, '*** No such file: %s' % tFile
if not os.path.exists(rFile):
    raise RuntimeError, '*** No such file: %s' % rFile

radPerDeg = numpy.pi / 180.
degPerRad = 180. / numpy.pi

if eFile.startswith('eimage'):
    t0 = eFile.split('_')
    if t0[2].startswith('f'):
        t1 = t0[5].split('.')[0]
        plotTitle = 'ObsHistID: %s, %s %s %s Filter: %s' % (
            t0[1], t0[3], t0[4], t1, t0[2])
    else:
        t1 = t0[4].split('.')[0]
        plotTitle = 'ObsHistID: %s, %s %s %s' % (
            t0[1], t0[2], t0[3], t1)
elif eFile.startswith('v'):
    plotTitle = 'ObsHistID: %s, %s %s Filter: %s' % (
        eFile[1:9], eFile[11:14], eFile[14:17], eFile[10])
else: raise RuntimeError, '*** Unknown file name format: %s' % eFile
print 'plotTitle:', plotTitle

eIm = afwImage.ImageF(eFile)
if eFile.startswith('eimage'):
    eImFakeBG = afwImage.ImageF(eFile)
    # Add a fake background to help source detection
    print 'Adding fake BG to aid source detection.'
    for i in range(0, eIm.getWidth(), 2):
        for j in range(0, eIm.getHeight(), 2):
            if random.random() > 0.5:
                t0 = eImFakeBG.get(i, j)
                eImFakeBG.set(i, j, t0 + 1)
    useEImForBG = eImFakeBG
else: useEImForBG = eIm
            
print 'Done adding fake background.'
metaData = afwImage.readMetadata(eFile)
wcs = afwImage.makeWcs(metaData)
filter0 = metaData.get('FILTER').rstrip()
mJD = metaData.get('MJD-OBS')
expoSec = metaData.get('EXPTIME')
ast = Astrometry()

minCtsBrightThresh = 2000
if eFile.startswith('v'):
    # We must un-hack the CRVALs to precess them back to coords that
    #  match the trim file (See ticket #1418.)
    print 'Un-hacking CRVALs in v* image.'
    t0 = { 'u':'0', 'g':'1', 'r':'2', 'i':'3', 'z':'4', 'y':'5' }
    if t0[eFile[10]] == 'y': minCtsBrightThresh = 500
    eImageFile = 'eimage_%s_%s_%s_E000.fits.gz' % (
        eFile[1:9], eFile[11:14], eFile[14:17])
    if not os.path.exists(eImageFile):
        # Try this one instead
        eImageFile = 'eimage_%s_f%s_%s_%s_E000.fits.gz' % (
            eFile[1:9], t0[eFile[10]], eFile[11:14], eFile[14:17])
    print '   Taking CRVALs from old eimage file: %s' % eImageFile
    if not os.path.exists(eImageFile):
        raise RuntimeError, '*** No such file: %s' % eImageFile
    t0 = afwImage.readMetadata(eImageFile)
    eCRVAL1 = t0.getDouble('CRVAL1')
    metaData.setDouble('CRVAL1', eCRVAL1)
    eCRVAL2 = t0.getDouble('CRVAL2')
    metaData.setDouble('CRVAL2', eCRVAL2)
    # Swap pixel axes (x <--> y); a joyful LSST convention
    t1 = t0.getDouble('CRPIX1')
    metaData.setDouble('CRPIX2', t1)
    t1 = t0.getDouble('CRPIX2')
    metaData.setDouble('CRPIX1', t1)
    t1 = t0.getDouble('CD1_2')
    metaData.setDouble('CD1_1', t1)
    t1 = t0.getDouble('CD1_1')
    metaData.setDouble('CD1_2', t1)
    t1 = t0.getDouble('CD2_2')
    metaData.setDouble('CD2_1', t1)
    t1 = t0.getDouble('CD2_1')
    metaData.setDouble('CD2_2', t1)
    # Update metadata
    wcs = afwImage.makeWcs(metaData)
    wcs.setMetadata(metaData)
    t1 = wcs.getMetadata()
    print 'Now using CRVALs:', t1.getDouble('CRVAL1'), t1.getDouble('CRVAL2')
    t2 = wcs.pixelToSky(100, 200).getPosition()
    print '   100, 200 ->', t2

# From 'throughputs.pdf' paper (Ivezic, Jones, Lupton)
# Using y4 here
zPMag1ctPers = { 'u':27.09, 'g':28.58, 'r':28.50, 'i':28.34, 'z':27.95, 'y':27.18 }

# Get image bounds, assuming LINEAR WCS
t0 = wcs.pixelToSky(0, 0)
t1 = wcs.pixelToSky(eIm.getWidth(), 0)
t2 = wcs.pixelToSky(0, eIm.getHeight())
t3 = wcs.pixelToSky(eIm.getWidth(), eIm.getHeight())
minRA = min(t0.getPosition()[0], t1.getPosition()[0],
            t2.getPosition()[0], t3.getPosition()[0])
maxRA = max(t0.getPosition()[0], t1.getPosition()[0],
            t2.getPosition()[0], t3.getPosition()[0])
minDec = min(t0.getPosition()[1], t1.getPosition()[1],
            t2.getPosition()[1], t3.getPosition()[1])
maxDec = max(t0.getPosition()[1], t1.getPosition()[1],
            t2.getPosition()[1], t3.getPosition()[1])

if eFile.startswith('eimage'):
    # There shouldn't be many sources in negative
    print 'Making negative footprint detections.'
    detSetN = afwDetection.makeFootprintSet(useEImForBG, afwDetection.createThreshold(5, 'stdev', False))
    footprintsN = detSetN.getFootprints()
    if len(footprintsN) > 0:
        raise RuntimeError, '*** Unexpected negative source.'
    else: print '   No negative footprints found, as expected.'

regOut = open(outDir + 'pDet.reg', 'w')
print 'Making positive footprint detections.'
detSetP = afwDetection.makeFootprintSet(useEImForBG, afwDetection.createThreshold(5, "stdev"))
footprintsP = detSetP.getFootprints()
nf = len(footprintsP)
detxs = []; detys = []; nCts = []; estSizePix = []
n = 0; boxSize = 2; circleSize = 20
print 'Considering %i positive footprints.' % len(footprintsP)
fAllFPP = open(outDir + 'fAllFPP.reg', 'w')
for fp in footprintsP:
    if n % 10000 == 0: print '%i of %i done.' % (n, nf)
    bboxes = afwDetection.footprintToBBoxList(fp)
    tCts = 0; ctsWtSumx = 0; ctsWtSumy = 0; nPix = 0
    for bbox in bboxes:
        x0, y0, x1, y1 = bbox.getX0(), bbox.getY0(), bbox.getX1(), bbox.getY1()
        dx = x1 - x0; dy = y1 - y0
        fAllFPP.write('box %5.3f %5.3f %5.3f %5.3f #color=yellow\n' % (
            min([x0,x1])+0.5*dx, min([y0,y1])+0.5*dy, dx, dy))
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                t0 = eIm.get(x, y)
                tCts += t0
                ctsWtSumx += 0.5*(x0 + x1) * t0
                ctsWtSumy += 0.5*(y0 + y1) * t0
                nPix += 1
    ctsWtSumx /= float(tCts); ctsWtSumy /= float(tCts)
    #regOut.write('box %5.5f %5.5f %i %i 0 # color=darkgoldenrod2\n' % (ctsWtSumx, ctsWtSumy, boxSize, boxSize))
    detxs.append(ctsWtSumx); detys.append(ctsWtSumy)
    nCts.append(tCts); estSizePix.append(nPix)
    if tCts > minCtsBrightThresh:
        regOut.write('circle %5.5f %5.5f %i # color=indianred\n' % (ctsWtSumx, ctsWtSumy, circleSize))
    n += 1

fAllFPP.close()

detxs = numpy.array(detxs); detys = numpy.array(detys)
nCts = numpy.array(nCts); estSizePix = numpy.array(estSizePix)

# Now get all the bright sources out of the ref cat
print 'Getting bright sources from ref cat.'
refMagCols = { 'u':8, 'g':9, 'r':10, 'i':11, 'z':12, 'y':13 }
refMagCol = refMagCols[filter0]
refRAsIn = []; refDecsIn = []; refMagsIn = []
galaxyType = 1; wDType = 2; kuruczType = 3; mLTType = 4; sSMType = 5
if rFile.endswith('.gz'): f = gzip.open(rFile, 'r')
else: f = open(rFile, 'r')
refLine = 0
for line in f:
    if line.startswith('ra,decl,gal_l,gal_b'): continue
    if refLine % 1000000 == 0: print 'Ref line: %i' % refLine
    t0 = line.split(',')
    #if float(t0[refMagCol]) > refMagThresh:
    #    refLine += 1
    #    continue
    t1 = float(t0[0])
    if t1 < 0: t1 += 360.
    refRA0 = t1
    t1 = float(t0[1])
    refDec0 = t1
    refRAsIn.append(refRA0); refDecsIn.append(refDec0)
    refMagsIn.append(float(t0[refMagCol]))
    refLine += 1

f.close()

refRAsIn = numpy.array(refRAsIn); refDecsIn = numpy.array(refDecsIn)
refMagsIn = numpy.array(refMagsIn)

print 'Precessing ref cat.'
refRAsPrec, refDecsPrec = ast.applyPrecession(
    refRAsIn * radPerDeg, refDecsIn * radPerDeg, EP0=2000.0, MJD=mJD)
print 'Finished precession.'
# Convert back go degrees
refRAsPrec *= degPerRad
refDecsPrec *= degPerRad

print 'Finding ref cat on chip.'
refxs = []; refys = []; refMags = []; refRAs = []; refDecs = []
refChipI = 0
for i in range(len(refRAsIn)):
    if refChipI % 1000000 == 0: print 'Ref line: %i' % refChipI
    t2 = wcs.skyToPixel(refRAsPrec[i], refDecsPrec[i])
    if t2.getX() < -chipBorder or t2.getY() < -chipBorder \
           or t2.getX() > eIm.getWidth()+chipBorder \
           or t2.getY() > eIm.getHeight()+chipBorder:
        refChipI += 1
        continue
    #print 'Adding a source from ref.'
    refxs.append(t2.getX()); refys.append(t2.getY())
    refRAs.append(refRAsIn[i]); refDecs.append(refDecsIn[i])
    refMags.append(refMagsIn[i])
    refChipI += 1

f.close()

refxs = numpy.array(refxs); refys = numpy.array(refys)
refMags = numpy.array(refMags)
refRAs = numpy.array(refRAs); refDecs = numpy.array(refDecs)
nRef = len(refxs)
print 'Found %i ref cat sources that fall on chip.' % nRef

# execfile('compareRefEImage.py')


# Now get all the bright sources out of the trim file
print 'Getting bright sources from trim file.'
trimxs = []; trimys = []; trimMags = []; trimTypes = []
galaxyType = 1; wDType = 2; kuruczType = 3; mLTType = 4; sSMType = 5
if tFile.endswith('.gz'): f = gzip.open(tFile, 'r')
else: f = open(tFile, 'r')
trimLine = 0
if tFile != outDir + 'onChipTrim.txt':
    fTrim = open(outDir + 'onChipTrim.txt', 'w')
for line in f:
    if trimLine % 1000000 == 0: print 'Trim line: %i' % trimLine
    if not line.startswith('object '):
        trimLine += 1
        continue
    t0 = line.split()
    #if float(t0[4]) > trimMagThresh:
    #    trimLine += 1
    #    continue
    t1 = float(t0[2]) * degPerRad
    if t1 < 0: t1 += 360.
    trimRA0 = t1
    #if trimRA0 < minRA or trimRA0 > maxRA:
    #    trimLine += 1
    #    continue
    t1 = float(t0[3]) * degPerRad
    trimDec0 = t1
    #if trimDec0 < minDec or trimDec0 > maxDec:
    #    trimLine += 1
    #    continue
    t2 = wcs.skyToPixel(trimRA0, trimDec0)
    if t2.getX() < -chipBorder or t2.getY() < -chipBorder \
           or t2.getX() > eIm.getWidth()+chipBorder \
           or t2.getY() > eIm.getHeight()+chipBorder:
        trimLine += 1
        continue
    #print 'Adding a source from trim.'
    trimxs.append(t2.getX()); trimys.append(t2.getY())
    trimMags.append(float(t0[4]))
    t1 = t0[5]
    if t1.find('galaxy') >= 0: trimTypes.append(galaxyType)
    elif t1.find('wDs') >= 0: trimTypes.append(wDType)
    elif t1.find('kurucz') >= 0: trimTypes.append(kuruczType)
    elif t1.find('mlt') >= 0: trimTypes.append(mLTType)
    elif t1.find('ssm') >= 0: trimTypes.append(sSMType)
    else: raise RuntimeError, '*** Unknown SED type: %s' % t1
    if tFile != 'onChipTrim.txt': fTrim.write(line)
    trimLine += 1

f.close()
if tFile != 'onChipTrim.txt': fTrim.close()

trimxs = numpy.array(trimxs); trimys = numpy.array(trimys)
trimMags = numpy.array(trimMags); trimTypes = numpy.array(trimTypes)
nTrim = len(trimxs)
for i in range(0, nTrim):
    if trimMags[i] > trimMagThresh: continue
    if trimTypes[i] == galaxyType: c0 = 'green'
    elif trimTypes[i] == wDType: c0 = 'cyan'
    elif trimTypes[i] == kuruczType: c0 = 'blue'
    elif trimTypes[i] == mLTType: c0 = 'red'
    elif trimTypes[i] == sSMType: c0 = 'yellow'
    else: raise RuntimeError, '*** Unknown SED Type: %s' % trimTypes[i]
    regOut.write('box %5.5f %5.5f 20 20 0 # color=%s\n' % (
        trimxs[i], trimys[i], c0))

# execfile('compareTrimEImage.py')

print 'Matching ref catalog to trim cat.'
t0 = numpy.where(refMags < refMagThresh)[0]
t1 = pyCommon.arrayAnd([trimMags < trimMagThresh, trimTypes != galaxyType,
                        trimTypes != sSMType])
t2 = numpy.where(t1)[0]
bestRefTrimXOffset, bestRefTrimYOffset, t1, t2 = \
                 matchTrimToDet.searchTrimDetMatch(
    refxs[t0], refys[t0], trimxs[t2], trimys[t2])
print 'Found match dx: %5.2f, dy: %5.2f pix' % (
    bestRefTrimXOffset, bestRefTrimYOffset)
offrefxsTrim = refxs + bestRefTrimXOffset
offrefysTrim = refys + bestRefTrimYOffset

# Now recalculate for the final pixDist and trimToRef
refPixDistsTrim, trimToRef, t0, t1 = matchTrimToDet.matchTrimToDet(
    offrefxsTrim, offrefysTrim, trimxs, trimys)

simplePlot.clear()
simplePlot.plot(trimxs, trimys, f='ko')
simplePlot.oplot(offrefxsTrim, offrefysTrim, f='r.')
simplePlot.clear()
simplePlot.plot(refMags, trimMags[trimToRef], f='ko')


## Apparently matching ref directly to image doesn't work well
#print 'Matching ref catalog to image detection.'
#t0 = numpy.where(refMags < refMagThresh)[0]
#t1 = pyCommon.arrayAnd([trimMags < trimMagThresh, trimTypes != galaxyType,
#                        trimTypes != sSMType])
#t2 = numpy.where(nCts > minCtsBrightThresh)[0]
#bestRefDetXOffset, bestRefDetYOffset, t1, t2 = \
#                 matchTrimToDet.searchTrimDetMatch(
#    refxs[t0], refys[t0], detxs[t2], detys[t2])
#print 'Found match dx: %5.2f, dy: %5.2f pix' % (
#    bestRefTrimXOffset, bestRefTrimYOffset)
#offrefxsDet = refxs + bestRefTrimXOffset
#offrefysDet = refys + bestRefTrimYOffset
#
## Now recalculate for the final pixDist and detToRef
#refPixDistsDet, detToRef, t0, t1 = matchTrimToDet.matchTrimToDet(
#    offrefxsDet, offrefysDet, detxs, detys)

#t0 = numpy.where(nCts > minCtsBrightThresh)[0]
#simplePlot.clear()
#simplePlot.plot(detxs[t0], detys[t0], f='ko')
#simplePlot.plot(offrefxsDet, offrefysDet, f='r.')

t0 = numpy.where(trimMags < 22)[0]
simplePlot.clear()
simplePlot.plot(trimxs[t0], trimys[t0], f='ko')
simplePlot.plot(detxs, detys, f='r.')


print 'Matching trim catalog to image detection.'
t0 = pyCommon.arrayAnd([trimMags < trimMagThresh, trimTypes != galaxyType,
                        trimTypes != sSMType])
t1 = numpy.where(t0)[0]
t2 = numpy.where(nCts > minCtsBrightThresh)[0]
simplePlot.clear()
simplePlot.plot(trimxs[t1], trimys[t1], f='ko')
simplePlot.plot(detxs[t2], detys[t2], f='r.')
#reload(matchTrimToDet);
bestTrimDetXOffset, bestTrimDetYOffset, t0, t1 = matchTrimToDet.searchTrimDetMatch(trimxs[t1], trimys[t1], detxs[t2], detys[t2])
print 'Found match dx: %5.2f, dy: %5.2f pix' % (
    bestTrimDetXOffset, bestTrimDetYOffset)
offtrimxs = trimxs + bestTrimDetXOffset
offtrimys = trimys + bestTrimDetYOffset
for i in range(0, nTrim):
    if trimMags[i] > trimMagThresh: continue
    if trimTypes[i] == galaxyType: c0 = 'green'
    elif trimTypes[i] == wDType: c0 = 'cyan'
    elif trimTypes[i] == kuruczType: c0 = 'blue'
    elif trimTypes[i] == mLTType: c0 = 'red'
    elif trimTypes[i] == sSMType: c0 = 'yellow'
    else: raise RuntimeError, '*** Unknown SED Type: %s' % trimTypes[i]
    regOut.write('box %5.5f %5.5f 10 10 0 # color=%s\n' % (
        offtrimxs[i], offtrimys[i], c0))

regOut.close()

# Now recalculate for the final pixDist
trimPixDists, detToTrim, t0, t1 = matchTrimToDet.matchTrimToDet(
    offtrimxs, offtrimys, detxs, detys)


t0 = numpy.where(trimMags < 22)[0]
simplePlot.clear()
simplePlot.plot(offtrimxs[t0], offtrimys[t0], f='ko')
simplePlot.plot(detxs, detys, f='r.')


trimCts = nCts[detToTrim]

t0 = numpy.where(trimTypes[trimToRef] == kuruczType)[0]
t1 = trimToRef[t0]
simplePlot.clear()
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='b.')

#refCts = nCts[detToRef]

## Apparently matching ref directly to image doesn't work well
#t0 = numpy.where(trimTypes[trimToRef] == kuruczType)[0]
#simplePlot.clear()
#simplePlot.plot(refMags[t0], numpy.log10(refCts[t0]), f='k.')



def makeDistDistrib(x, bLo, bHi):
    nb = len(x)
    retVal = numpy.zeros(len(bLo), dtype=float)
    for i in range(0, nb):
        t0 = pyCommon.arrayAnd([x[i] >= bLo, x[i] < bHi])
        t1 = numpy.where(t0)[0]
        if len(t1) != 1:
            print '*** Could not bin: %f' % x[i]
        retVal[t1] += 1
    return retVal

def plotThem(bMid, y, fStr, yMin=None):
    y0 = numpy.array(y)
    if yMin != None:
        t0 = numpy.where(y <= yMin)[0]
        y0[t0] = yMin
    simplePlot.plot(numpy.log10(bMid), numpy.log10(y0), f=fStr)

dx = 0.2
bLo = 10**numpy.arange(-2, 3, dx)
bHi = bLo * (10**dx)
bMid = (bLo + bHi) / 2.
simplePlot.clear()
yMin = 0.5; yMax = 3
t0 = numpy.where(trimTypes == galaxyType)[0]
t1 = makeDistDistrib(trimPixDists[t0], bLo, bHi)
#simplePlot.plot(bMid, numpy.log10(t1), f='g-')
plotThem(bMid, t1, 'g-', yMin)

t0 = numpy.where(trimTypes == wDType)[0]
t1 = makeDistDistrib(trimPixDists[t0], bLo, bHi)
#simplePlot.plot(bMid, numpy.log10(t1), f='k.')
plotThem(bMid, t1, 'k-', yMin)

t0 = numpy.where(trimTypes == kuruczType)[0]
t1 = makeDistDistrib(trimPixDists[t0], bLo, bHi)
#simplePlot.plot(bMid, t1, f='b.')
plotThem(bMid, t1, 'b-', yMin)

t0 = numpy.where(trimTypes == mLTType)[0]
t1 = makeDistDistrib(trimPixDists[t0], bLo, bHi)
#simplePlot.plot(bMid, t1, f='r.')
plotThem(bMid, t1, 'r-', yMin)

t0 = numpy.where(trimTypes == sSMType)[0]
t1 = makeDistDistrib(trimPixDists[t0], bLo, bHi)
#simplePlot.plot(bMid, t1, f='y.')
plotThem(bMid, t1, 'y-', yMin)

simplePlot.xlabel('$\log_{10}$(pix offset: $|$det$-$trim$|$)')
simplePlot.ylabel('$\log_{10}$(Number)')
plt = simplePlot.getplt()
t0 = plt.legend(('galaxies', 'WDs', 'Kurucz', 'MLT', 'SSM'), loc='upper left')
simplePlot.title(plotTitle)
simplePlot.savePNG(outDir + 'compareTrimEImageNumVsPixDist.png')


# These are the cts, x, y of *detected* sources match to trim cat
trimdetxs = detxs[detToTrim]; trimdetys = detys[detToTrim]
trimEstSizePix = estSizePix[detToTrim]

simplePlot.clear()
#t0 = numpy.where(trimTypes == galaxyType)[0]
t0 = numpy.where(pyCommon.arrayAnd(
    [trimTypes == galaxyType, trimPixDists < 3]))[0]
simplePlot.plot(trimMags[t0], numpy.log10(trimCts[t0]), f='g.')
t0 = numpy.where(trimTypes == wDType)[0]
simplePlot.plot(trimMags[t0], numpy.log10(trimCts[t0]), f='ko')
t0 = numpy.where(trimTypes == kuruczType)[0]
simplePlot.plot(trimMags[t0], numpy.log10(trimCts[t0]), f='bo')
t0 = numpy.where(trimTypes == mLTType)[0]
simplePlot.plot(trimMags[t0], numpy.log10(trimCts[t0]), f='ro')
t0 = numpy.where(trimTypes == sSMType)[0]
simplePlot.plot(trimMags[t0], numpy.log10(trimCts[t0]), f='yo')

simplePlot.xlabel('Ref cat mag: filter %s' % filter0)
simplePlot.ylabel('$\log_{10}$(Num counts in footprint)')
estMags = numpy.arange(10, 28, 0.1)
t1 = zPMag1ctPers[filter0]
# 1 ct/sec for zero-point mag
estCts = numpy.log10(expoSec * 1. * 10**(0.4 * (t1 - estMags)))
simplePlot.oplot(estMags, estCts, f='k-')
plt = simplePlot.getplt()
t0 = plt.legend(('galaxies in 3 pix', 'WDs', 'Kurucz', 'MLT', 'SSM'), loc='lower left')
simplePlot.xrange(0.9 * min(trimMags), 1.1 * max(trimMags))
t0 = numpy.log10(trimCts)
simplePlot.yrange(0.9 * min(t0), 1.1 * max(t0))
simplePlot.title(plotTitle)
simplePlot.savePNG(outDir + 'compareTrimEImageCtsVsFluxNormMag.png')

def makeGoodBadRegions(mags0, cts0, xs0, ys0, zPMag1ctPers, filter0,
                       expoSec0, suffix0, minMag0, outDir):
    f = open(outDir + 'goodBadRegions' + suffix0 + '.reg', 'w')
    fFlipxy = open(outDir + 'goodBadRegionsFlipxy' + suffix0 + '.reg', 'w')
    for i in range(len(mags0)):
        if mags0[i] > minMag0: continue
        estCts0 = expoSec0 * 1. * 10**(0.4 * (
            zPMag1ctPers[filter0] - mags0[i]))
        print 'cts:', cts0[i], ' estCts:', estCts0
        if cts0[i] / estCts0 < 0.1:
            f.write('circle %5.2f %5.2f 10 # color=red\n' % (
                xs0[i], ys0[i]))
            fFlipxy.write('circle %5.2f %5.2f 10 # color=red\n' % (
                ys0[i], xs0[i]))
        elif cts0[i] / estCts0 < 0.5:
            f.write('circle %5.2f %5.2f 10 # color=yellow\n' % (
                xs0[i], ys0[i]))
            fFlipxy.write('circle %5.2f %5.2f 10 # color=yellow\n' % (
                ys0[i], xs0[i]))
        else:
            f.write('circle %5.2f %5.2f 10 # color=green\n' % (
                xs0[i], ys0[i]))
            fFlipxy.write('circle %5.2f %5.2f 10 # color=green\n' % (
                ys0[i], xs0[i]))
    f.close()
    fFlipxy.close()

simplePlot.clear()
t0 = numpy.where(pyCommon.arrayAnd(
    [trimTypes[trimToRef] == galaxyType, trimPixDists[trimToRef] < 3]))[0]
t1 = trimToRef[t0]
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='g.')

t0 = numpy.where(trimTypes[trimToRef] == wDType)[0]
t1 = trimToRef[t0]
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='ko')

t0 = numpy.where(pyCommon.arrayAnd(
    [trimTypes[trimToRef] == kuruczType, refMags < refMagThresh,
     offtrimxs[trimToRef] >= 0, offtrimxs[trimToRef] < eIm.getWidth(),
     offtrimys[trimToRef] >= 0, offtrimys[trimToRef] < eIm.getHeight()]))[0]
t1 = trimToRef[t0]
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='b.')
makeGoodBadRegions(refMags[t0], trimCts[t1], offtrimxs[t1], offtrimys[t1],
                   zPMag1ctPers, filter0, expoSec, 'Kurucz', refMagThresh,
                   outDir)

t0 = numpy.where(pyCommon.arrayAnd(
    [trimTypes[trimToRef] == mLTType, refMags < refMagThresh,
     offtrimxs[trimToRef] >= 0, offtrimxs[trimToRef] < eIm.getWidth(),
     offtrimys[trimToRef] >= 0, offtrimys[trimToRef] < eIm.getHeight()]))[0]
t1 = trimToRef[t0]
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='r.')
makeGoodBadRegions(refMags[t0], trimCts[t1], offtrimxs[t1], offtrimys[t1],
                   zPMag1ctPers, filter0, expoSec, 'MLT', refMagThresh,
                   outDir)

t0 = numpy.where(trimTypes[trimToRef] == sSMType)[0]
t1 = trimToRef[t0]
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='yo')

simplePlot.xlabel('Ref cat mag: filter %s' % filter0)
simplePlot.ylabel('$\log_{10}$(Num counts in footprint)')
estMags = numpy.arange(10, 30, 0.1)
t1 = zPMag1ctPers[filter0]
# 1 ct/sec for zero-point mag
estCts = numpy.log10(expoSec * 1. * 10**(0.4 * (t1 - estMags)))
simplePlot.oplot(estMags, estCts, f='k-')
plt = simplePlot.getplt()
t0 = plt.legend(('galaxies in 3 pix', 'WDs', 'Kurucz', 'MLT', 'SSM'), loc='lower left')
simplePlot.xrange(10, 30)
t0 = numpy.log10(trimCts)
simplePlot.yrange(0.9 * min(t0), 1.1 * max(t0))
simplePlot.title(plotTitle)
simplePlot.savePNG(outDir + 'compareTrimEImageCtsVsRefMag.png')


#refToTrim = -1 * numpy.ones(len(trimTypes), dtype=int)
#for i in range(len(trimToRef)):
#    refToTrim[trimToRef[i]] = i

def calcRMS(x):
    t0 = x - numpy.mean(x)
    t1 = numpy.sum(x**2)
    nx = float(len(x))
    return numpy.sqrt(t1 / (nx-1.))

def calcMAD(x):
    t0 = abs(x - numpy.median(x))
    return numpy.median(t0) / 0.6745


trimCts = nCts[detToTrim]

t0 = numpy.where(trimTypes[trimToRef] == kuruczType)[0]
t1 = trimToRef[t0]
simplePlot.clear()
simplePlot.plot(refMags[t0], numpy.log10(trimCts[t1]), f='b.')


# Select only the good matches that are stars and have sufficient counts
# 1 ct/sec for zero-point mag... match to candidates in 10% range
#  of expected counts
t0 = zPMag1ctPers[filter0]
minCts0 = 0.1 * expoSec * 1. * 10**(0.4 * (t0 - refMags))
maxCts0 = 10. * expoSec * 1. * 10**(0.4 * (t0 - refMags))
t0 = pyCommon.arrayOr([trimTypes[trimToRef] == kuruczType,
                       trimTypes[trimToRef] == mLTType])
t1 = pyCommon.arrayAnd(
    [t0 == True, refMags <= refMagThresh,
     trimCts[trimToRef] >= minCts0, trimCts[trimToRef] <= maxCts0,
     offtrimxs[trimToRef] >= 0, offtrimxs[trimToRef] < eIm.getWidth(),
     offtrimys[trimToRef] >= 0, offtrimys[trimToRef] < eIm.getHeight() ])
wMSRef = numpy.where(t1)[0]
wMSTrimToRef = trimToRef[t1]
makeGoodBadRegions(refMags[wMSRef], trimCts[wMSTrimToRef],
                   offtrimxs[wMSTrimToRef], offtrimys[wMSTrimToRef],
                   zPMag1ctPers, filter0, expoSec, 'StarsForWCS',
                   refMagThresh, outDir)


# Check the matches with this plot
simplePlot.clear()
simplePlot.plot(refMags[wMSRef], numpy.log10(trimCts[wMSTrimToRef]), f='ko')

# Use J2000 RAs
xyToRAFit = sip.LeastSqFitter2dPoly(
    trimdetxs[wMSTrimToRef], trimdetys[wMSTrimToRef], refRAs[wMSRef],
    numpy.ones(len(wMSTrimToRef)), 2)
xyToDecFit = sip.LeastSqFitter2dPoly(
    trimdetxs[wMSTrimToRef], trimdetys[wMSTrimToRef], refDecs[wMSRef],
    numpy.ones(len(wMSTrimToRef)), 2)
#r = refRAs[wMSRef]; d = refDecs[wMSRef]
#x = trimdetxs[wMSTrimToRef]; y = trimdetys[wMSTrimToRef]

rADecToxFit = sip.LeastSqFitter2dPoly(
    refRAs[wMSRef], refDecs[wMSRef], trimdetxs[wMSTrimToRef],
    numpy.ones(len(wMSTrimToRef)), 2)
rADecToyFit = sip.LeastSqFitter2dPoly(
    refRAs[wMSRef], refDecs[wMSRef], trimdetys[wMSTrimToRef],
    numpy.ones(len(wMSTrimToRef)), 2)

# Plot some histograms of the residuals
resRAArcsec = numpy.array(xyToRAFit.residuals()) * 60. * 60.
resDecArcsec = numpy.array(xyToDecFit.residuals()) * 60. * 60.
t0 = min([min(resRAArcsec), min(resDecArcsec)])
t1 = max([max(resRAArcsec), max(resDecArcsec)])
db = t1 - t0
nBins = 100.
bMid = numpy.arange(t0 - 0.05*db, t1 + 0.05*db, db / nBins)
bLo = bMid - 0.5*(db/nBins)
bHi = bMid + 0.5*(db/nBins)
t2 = makeDistDistrib(resRAArcsec, bLo, bHi)
simplePlot.clear()
simplePlot.plot(bMid, t2, f='k-')
t3 = makeDistDistrib(resDecArcsec, bLo, bHi)
simplePlot.plot(bMid, t3, f='b-')
simplePlot.plot([0,0], simplePlot.getyrange(), f='k:')
plt = simplePlot.getplt()
plt.legend(('RA', 'Dec'), loc='upper right')
simplePlot.xlabel('WCS Residual to Matched Star (arcsec)')
simplePlot.ylabel('Number of Cases')
t0 = calcRMS(resRAArcsec); t1 = calcMAD(resRAArcsec)
t2 = calcRMS(resDecArcsec); t3 = calcMAD(resDecArcsec)
(xMin, xMax) = simplePlot.getxrange()
(yMin, yMax) = simplePlot.getyrange()
dx = xMax - xMin; dy = yMax - yMin
usex = xMin + 0.1*dx
simplePlot.text(usex, yMax - 0.10*dy, 'RA sample var: %2.3f' % t0)
simplePlot.text(usex, yMax - 0.15*dy, 'RA MAD: %2.3f' % t1)
simplePlot.text(usex, yMax - 0.20*dy, 'Dec sample var: %2.3f' % t2)
simplePlot.text(usex, yMax - 0.25*dy, 'Dec MAD: %2.3f' % t3)
simplePlot.title(plotTitle)
simplePlot.savePNG(outDir + 'WCSResidualDeg.png')

#f = open(outDir + 'testWCS.reg', 'w')
#for i in range(len(x)):
#    f.write('circle %5.3f %5.3f 10 #color=green\n' % (x[i], y[i]))
#
#f.close()


def fitSkyToPix(rA0, dec0, rADecToxFit, rADecToyFit):
    t0 = rADecToxFit.getParams()[0,0]
    t0 += rADecToxFit.getParams()[1,0]*rA0
    t0 += rADecToxFit.getParams()[0,1]*dec0
    t1 = rADecToyFit.getParams()[0,0]
    t1 += rADecToyFit.getParams()[1,0]*rA0
    t1 += rADecToyFit.getParams()[0,1]*dec0
    return (t0, t1)

xyToRAFit.printParams()
t0 = xyToRAFit.getParams()[0,0] +\
     xyToRAFit.getParams()[1,0]*trimdetxs[wMSTrimToRef] +\
     xyToRAFit.getParams()[0,1]*trimdetys[wMSTrimToRef]
t1 = t0 - refRAs[wMSRef]
simplePlot.clear()
simplePlot.plot(refRAs[wMSRef], t0, f='k.')

f = open(outDir + 'wCS.dat', 'w')
f.write('%5.7f %5.7f\n' % (
    xyToRAFit.valueAt(0, 0), xyToDecFit.valueAt(0, 0)))
f.write('%5.7f %5.7f\n' % (
    xyToRAFit.valueAt(
    eIm.getWidth()-1, 0), xyToDecFit.valueAt(eIm.getWidth()-1, 0)))
f.write('%5.7f %5.7f\n' % (
    xyToRAFit.valueAt(
    0, eIm.getHeight()-1), xyToDecFit.valueAt(0, eIm.getHeight()-1)))
f.write('%5.7f %5.7f\n' % (
    xyToRAFit.valueAt(eIm.getWidth()-1, eIm.getHeight()-1),
    xyToDecFit.valueAt(eIm.getWidth()-1, eIm.getHeight()-1)))
f.close()


# Now go back and write regions for all refCat stars using the fit WCS
print 'Writing region file using WCS fit.'
f = open(outDir + 'wCSStars.reg', 'w')
for i in range(0, len(refMags)):
    (x0, y0) = fitSkyToPix(refRAs[i], refDecs[i], rADecToxFit, rADecToyFit);
    if x0 < 0 or x0 > eIm.getWidth(): continue
    if y0 < 0 or y0 > eIm.getHeight(): continue
    if refMags[i] > refMagThresh:
        f.write('circle %5.3f %5.3f 10 10 # color=yellow\n' % (x0, y0))
    else:
        f.write('circle %5.3f %5.3f 20 # color=green\n' % (x0, y0))

f.close()

# Do forced aperture extraction using the fit WCS.  Let's try a 20-pixel,
#  4-arcsec radius
print 'Performing forced photo for %i objects (incl. non-stars)' % (
    len(refMags))
refForcedCts = numpy.zeros(len(refMags)) - 1.
forcedRad = 20; forcedRad2 = forcedRad**2
for i in range(0, len(refMags)):
    print 'Forced photo for object %i of %i' % (i, len(refMags))
    (x0, y0) = fitSkyToPix(refRAs[i], refDecs[i], rADecToxFit, rADecToyFit);
    if x0 < 0 or x0 >= eIm.getWidth(): continue
    if y0 < 0 or y0 >= eIm.getHeight(): continue
    print 'Looking at x0, y0:', x0, y0
    tCts = 0.
    for j in range(int(x0-forcedRad-1), int(x0+forcedRad+1)):
        # If you get() off image bounds, seg-fault ensues
        if j < 0 or j >= eIm.getWidth(): continue
        for k in range(int(y0-forcedRad-1), int(y0+forcedRad+1)):
            if k < 0 or k >= eIm.getHeight(): continue
            t0 = (j-x0)**2 + (k-y0)**2
            if t0 > forcedRad2: continue
            tCts += eIm.get(j, k)
    refForcedCts[i] = tCts
    print '   Found %5.1f cts.' % tCts

refForcedCts = numpy.array(refForcedCts)

# If refForcedCts < 0, it means the matched source was off-chip, so skip it
simplePlot.clear()
t0 = numpy.where(pyCommon.arrayAnd([
    trimTypes[trimToRef] == galaxyType, refForcedCts >= 0.]))[0]
simplePlot.plot(refMags[t0], numpy.log10(refForcedCts[t0]), f='g.')
t0 = numpy.where(pyCommon.arrayAnd([
    trimTypes[trimToRef] == wDType, refForcedCts >= 0.]))[0]
simplePlot.plot(refMags[t0], numpy.log10(refForcedCts[t0]), f='k.')
t0 = numpy.where(pyCommon.arrayAnd([
    trimTypes[trimToRef] == kuruczType, refForcedCts >= 0.]))[0]
simplePlot.plot(refMags[t0], numpy.log10(refForcedCts[t0]), f='b.')
t0 = numpy.where(pyCommon.arrayAnd([
    trimTypes[trimToRef] == mLTType, refForcedCts >= 0.]))[0]
simplePlot.plot(refMags[t0], numpy.log10(refForcedCts[t0]), f='r.')
t0 = numpy.where(pyCommon.arrayAnd([
    trimTypes[trimToRef] == sSMType, refForcedCts >= 0.]))[0]
simplePlot.plot(refMags[t0], numpy.log10(refForcedCts[t0]), f='y.')
simplePlot.xlabel('Ref cat mag: filter %s' % filter0)
simplePlot.ylabel('$\log_{10}$(Num cts forced photo)')
estMags = numpy.arange(int(min(refMags)), int(max(refMags)+1), 0.1)
t1 = zPMag1ctPers[filter0]
# 1 ct/sec for zero-point mag
estCts = numpy.log10(expoSec * 1. * 10**(0.4 * (t1 - estMags)))
simplePlot.oplot(estMags, estCts, f='k-')
plt = simplePlot.getplt()
t0 = plt.legend(('galaxies', 'WDs', 'Kurucz', 'MLT', 'SSM', 'Expected'), loc='lower left')
simplePlot.xrange(min(refMags) - 0.1, max(refMags) + 0.1)
t0 = numpy.where(refForcedCts > 0)[0]
t1 = numpy.log10(refForcedCts[t0])
simplePlot.yrange(0, max(t1) + 0.1)
simplePlot.title(plotTitle)
simplePlot.savePNG(outDir + 'compareTrimEImageCtsVsRefMagWCSForcedPhoto.png')



t0 = zPMag1ctPers[filter0]
estCts0 = numpy.log10(expoSec * 1. * 10**(0.4 * (t0 - refMags)))
t1 = pyCommon.arrayAnd([
    refForcedCts > 39.8, numpy.log10(refForcedCts) < estCts0-1.,
    refMags < 25], )

t2 = numpy.where(t1)[0]

simplePlot.plot(refMags[t2], numpy.log10(refForcedCts[t2]), f='mo')

f = open(outDir + 'test.reg', 'w')
for i in range(len(t2)):
    (x0, y0) = fitSkyToPix(refRAs[i], refDecs[i], rADecToxFit, rADecToyFit);
    f.write('circle %5.3f %5.3f 10 #color=cyan\n' % (x0, y0))

f.close()


