import pyoorb
import sys
import math
from lsst.sims.catalogs.measures.photometry.Bandpass import *
from lsst.sims.catalogs.measures.photometry.Sed import *
from lsst.sims.catalogs.measures.astrometry.Astrometry import Astrometry
import os
import numpy

class makeEaster (object):
    def __init__(self):
        self.tpath = os.getenv('LSST_THROUGHPUTS_DEFAULT')
        self.spath = os.getenv('SIMS_SED_LIBRARY_DIR')
        self.specmap = self.makeSpecMap("../../data/fileMaps/spec_map.dat")
        self.eggs = []
        self.fields = ['id','filename', 'fieldid', 'ra', 'dec', 'gizisid', 'appmag', 'filtid',\
        'parallax', 'mura', 'mudec', 'vrad', 'teff',  'logg', 'feh', 'd',\
        'filtstr', 'sed','fluxnorm','magnorm','umag','gmag','rmag','imag',\
        'zmag','ymag','gal_l','gal_b','ebv']
        self.fieldind = [-1,-1, 0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18,\
                19, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1]

    def addEaster(self, filename):
        fh = open(filename)
        for l in fh:
            line = {}
            flds = l.strip().split()
            for ind, name in zip(self.fieldind, self.fields):
                if ind < 0 and name == 'filename':
                    line[name] = filename
                elif ind < 0:
                    continue
                else:
                    line[name] = flds[ind]

            magNorm, fluxNorm = self.getSpecNorms(self.specmap[line['sed']],\
                    float(line['appmag']), line['filtstr'])
            glon, glat = Astrometry().equatorialToGalactic([float(line['ra'])],\
                    [float(line['dec'])]) 
            line['gal_l'] = glon[0]*180./math.pi
            line['gal_b'] = glat[0]*180./math.pi
            line['umag'], line['gmag'], line['rmag'], line['imag'], line['zmag'], line['ymag'] =\
                    self.calcLSSTMags(self.specmap[line['sed']], fluxNorm)
            line['magnorm'] = magNorm
            line['fluxnorm'] = fluxNorm
            line['ebv'] = 0.
            try:
                line['id'] = self.eggs[-1]['id'] + 1
            except:
                line['id'] = 303 
            self.eggs.append(line)

    def getSpecNorms(self, sedfile, mag, filtstr):
        band = Bandpass()
        band.readThroughput(os.path.join(self.tpath, "total_%s.dat"%(filtstr)))
        imsimband = Bandpass()
        imsimband.imsimBandpass()
        sed = Sed()
        sed.readSED_flambda(self.spath+"/"+sedfile)
        fluxNorm = sed.calcFluxNorm(mag, band)
        sed.multiplyFluxNorm(fluxNorm)
        magNorm = sed.calcMag(imsimband)
        return magNorm, fluxNorm

    def calcLSSTMags(self, sedfile, fluxnorm):
        sed = Sed()
        sed.readSED_flambda(self.spath+"/"+sedfile)
        sed.multiplyFluxNorm(fluxnorm)
        mags = []
        for filtstr in ('u', 'g', 'r', 'i', 'z', 'y'):
            band = Bandpass()
            band.readThroughput(os.path.join(self.tpath, "total_%s.dat"%(filtstr)))
            imsimband = Bandpass()
            imsimband.imsimBandpass()
            mags.append(sed.calcMag(band))
        return mags

    def writeEasterFile(self):
        fh = open("EasterEggs2.out", 'w')
        fh.write(",".join([el for el in self.fields])+"\n")
        for egg in self.eggs:
            fh.write(",".join([str(egg[el]) for el in self.fields])+"\n")
        fh.close()

    def makeSpecMap(self, filename):
        fh = open(filename)
        mapdict = {}
        for l in fh:
            if l.startswith("["):
                continue
            else:
                flds = l.strip().split("=")
                mapdict[flds[0].strip()] = eval(flds[1].strip())
        fh.close()
        return mapdict


if __name__ == "__main__":
    fileList = open(sys.argv[1])
    me = makeEaster()
    for file in fileList:
        me.addEaster(file.rstrip())
    me.writeEasterFile()

