# execfile('matchTrimToDet.py')

execfile('r.py')

import numpy

def searchTrimDetMatch_old1(trimxs, trimys, detxs, detys):
    boxSize = 600
    stepSize = 10
    maxTries = 10
    successThresh = 0.8 # 80% is a successful match
    nTry = 0
    bestCloseNum = -1.e100; bestTrimXOffset = 0; bestTrimYOffset = 0
    for i in range(maxTries):
        print 'STDM:  Box %3.2f, Step: %3.2f, XOff: %3.2f, YOff: %3.2f' % (
            boxSize, stepSize, bestTrimXOffset, bestTrimYOffset)
        bestCloseNum0, bestTrimXOffset0, bestTrimYOffset0, \
                      pixDists, trimToDet = \
                      searchTrimDetMatchForBox(
            trimxs + bestTrimXOffset, trimys + bestTrimYOffset,
            detxs, detys, boxSize, stepSize)
        if bestCloseNum0 > bestCloseNum:
            raise RuntimeError, '*** Matching failed.'
        boxSize /= 10.
        while boxSize / stepSize < 10: stepSize /= 2.
        bestTrimXOffset += bestTrimXOffset0
        bestTrimYOffset += bestTrimYOffset0
        if (bestCloseNum0/float(len(trimxs))) > successThresh: break
        if boxSize < 1.: break
    return bestTrimXOffset, bestTrimYOffset, pixDists, trimToDet

def searchTrimDetMatch(trimxs, trimys, detxs, detys):
    boxSize = 600
    stepSize = 10
    maxTries = 10
    successThresh = 2. # 2 pixels is a successful match
    nTry = 0
    bestMedDist = 1.e100; bestTrimXOffset = 0; bestTrimYOffset = 0
    for i in range(maxTries):
        print 'STDM:  Box %3.2f, Step: %3.2f, XOff: %3.2f, YOff: %3.2f' % (
            boxSize, stepSize, bestTrimXOffset, bestTrimYOffset)
        bestMedDist0, bestTrimXOffset0, bestTrimYOffset0, \
                      pixDists, trimToDet = \
                      searchTrimDetMatchForBox(
            trimxs + bestTrimXOffset, trimys + bestTrimYOffset,
            detxs, detys, boxSize, stepSize)
        if bestMedDist0 > bestMedDist:
            raise RuntimeError, '*** Matching failed.'
        boxSize /= 10.
        while boxSize / stepSize < 10: stepSize /= 2.
        bestTrimXOffset += bestTrimXOffset0
        bestTrimYOffset += bestTrimYOffset0
        if bestMedDist0 < successThresh: break
        if boxSize < 1.: break
    return bestTrimXOffset, bestTrimYOffset, pixDists, trimToDet


def searchTrimDetMatchForBox_old1(trimxs, trimys, detxs, detys,
                             boxSize, stepSize):
    boxGrid = numpy.arange(-boxSize/2., boxSize/2. + 1, stepSize)
    nb = len(boxGrid)
    bestI = 1.e100; bestJ = 1.e100; bestCloseNum = -1.e100
    bestMeddx = 1.e100; bestMeddy = 1.e100; bestMedDist = 1.e100
    for i in range(len(boxGrid)):
        print 'Trying box match offset I: %3.2f %i to %i' % (
            boxGrid[i], len(trimxs), len(detxs))
        for j in range(len(boxGrid)):
            pixDists0, trimToDet0, dxs0, dys0 = matchTrimToDet(
                trimxs + boxGrid[i], trimys + boxGrid[j], detxs, detys)
            t0 = len(numpy.where(pixDists0 < 10)[0])
            if t0 > bestCloseNum:
                print 'Trying box match offset: %3.2f %3.2f' % (
                    boxGrid[i], boxGrid[j])
                print '   num close matches: %i' % (t0)
                print '   This is better than before, updating.'
                bestI = i; bestJ = j; bestMedDist = t0
                bestMeddx = numpy.median(dxs0) + boxGrid[i]
                bestMeddy = numpy.median(dys0) + boxGrid[j]
    return bestCloseNum, bestMeddx, bestMeddy, pixDists0, trimToDet0

def searchTrimDetMatchForBox(trimxs, trimys, detxs, detys,
                             boxSize, stepSize):
    boxGrid = numpy.arange(-boxSize/2., boxSize/2. + 1, stepSize)
    nb = len(boxGrid)
    bestI = 1.e100; bestJ = 1.e100; bestMedDist = 1.e100
    bestMeddx = 1.e100; bestMeddy = 1.e100
    for i in range(len(boxGrid)):
        print 'Trying box match offset I: %3.2f %i to %i' % (
            boxGrid[i], len(trimxs), len(detxs))
        for j in range(len(boxGrid)):
            pixDists0, trimToDet0, dxs0, dys0 = matchTrimToDet(
                trimxs + boxGrid[i], trimys + boxGrid[j], detxs, detys)
            pixDists1 = numpy.sort(pixDists0)
            tx = len(pixDists0)
            maxNum = 1.e100
            if tx > maxNum: tx = maxNum
            iB = range(0, tx)
            t0 = numpy.median(pixDists1[iB])
            if t0 < bestMedDist:
                print 'Trying box match offset: %3.2f %3.2f' % (
                    boxGrid[i], boxGrid[j])
                print '   median(dist) = %3.3f' % (t0)
                print '   This is better than before, updating.'
                bestI = i; bestJ = j; bestMedDist = t0
                bestMeddx = numpy.median(dxs0) + boxGrid[i]
                bestMeddy = numpy.median(dys0) + boxGrid[j]
    print '   finished iter: bestMedDist:', bestMedDist
    return bestMedDist, bestMeddx, bestMeddy, pixDists0, trimToDet0

def matchTrimToDet(trimxs0, trimys0, detxs0, detys0):
    # Match trim catalog to nearest neighbors
    nTrim = len(trimxs0)
    pixDists = numpy.zeros(nTrim, dtype=int) + 1.e100
    trimToDet = numpy.zeros(nTrim, dtype=int) - 1
    dxs = numpy.zeros(nTrim, dtype=int)
    dys = numpy.zeros(nTrim, dtype=int)
    for i in range(0, nTrim):
        if i > 0 and i % 10000 == 0: print 'Match %i of %i' % (i, nTrim)
        t0 = detxs0 - trimxs0[i]
        t1 = detys0 - trimys0[i]
        t2 = t0*t0 + t1*t1
        bestI = numpy.argmin(t2)
        pixDists[i] = numpy.sqrt(t2[bestI])
        trimToDet[i] = bestI
        dxs[i] = t0[bestI]; dys[i] = t1[bestI]

    return pixDists, trimToDet, dxs, dys
