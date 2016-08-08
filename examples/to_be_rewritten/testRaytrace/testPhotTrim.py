#!/usr/bin/env python
import pyoorb
import math
import random
from lsst.sims.catalogs.measures.photometry.Bandpass import *
from lsst.sims.catalogs.measures.photometry.Sed import *
from lsst.sims.catalogs.measures.photometry.EBV import *
from lsst.sims.catalogs.measures.astrometry.Astrometry import *
from lsst.sims.catalogs.measures.utils.catalogGeneration import *
from lsst.sims.catalogs.measures.utils.trimGeneration import *
import os
import re
import copy
import numpy

class testPhotTrim (object):
    def __init__(self, metafile, trimfilename, centfilename, outfile, donum = None):
        self.datadir = os.environ.get("SIMS_DATA_DIR")

        self.tpath = os.getenv('LSST_THROUGHPUTS_DEFAULT')
        self.spath = os.getenv('SIMS_SED_LIBRARY_DIR')
        self.bands = {"u":None, "g":None, "r":None, "i":None, "z":None, "y":None}
        keys = self.bands.keys()
        for k in keys:
            self.bands[k] = Bandpass()
            self.bands[k].readThroughput(os.path.join(self.tpath, "total_%s.dat"%k))
        self.imsimband = Bandpass()
        self.imsimband.imsimBandpass()
        self.mfile = metafile
        self.tfile = trimfilename
        self.ofile = outfile
        self.outdata = {'id':[],'u':[], 'g':[], 'r':[], 'i':[], 'z':[],
                'y':[]}
        self.centdata = self.readCentroid(centfilename)
        self.filtmap = {0:'u', 1:'g', 2:'r', 3:'i', 4:'z', 5:'y'}
        self.filter = None
        self.donum = donum

    def readCentroid(self,file):
        ifh = open(file)
        data = {}
        for l in ifh:
            flds = l.rstrip().split()
            if flds[0].startswith("Source") or int(flds[1]) == 0:
                continue
            data[float(flds[0])] = {'photons':int(flds[1]), 'x':float(flds[2]), 'y':float(flds[3])}
        return data

    def mkGalPhot(self):
        ifh = open(self.mfile)
        lnum = 0
        k = None
        for l in ifh:
            flds = l.rstrip().split()
            if l.startswith("Opsim_filter"):
                self.filter = self.filtmap[int(flds[1])]
                k = self.filter
        ifh.close()
        ifh = open(self.tfile)
        for l in ifh:
            flds = l.rstrip().split()
            if not flds[0] == "object":
                continue
            otype = flds[12]
            if otype != "sersic2D":
                continue
            id = float(flds[1])
            if not self.centdata.has_key(id):
                continue
            magNorm = float(flds[4])
            spec = flds[5]
            redshift = float(flds[6])
            ind = float(flds[16])
            mwav = float(flds[21])
            av = float(flds[18])
            sed = Sed()
            sed.readSED_flambda(self.spath+"/"+spec)
            a_int, b_int = sed.setupCCMab()
            self.outdata['id'].append(id)
            if lnum > self.donum and self.donum is not None:
                break
            if lnum%10000 == 0:
                print id
            fluxNorm = sed.calcFluxNorm(magNorm, self.imsimband)
            sed.multiplyFluxNorm(fluxNorm/(1+redshift))
            sed.addCCMDust(a_int, b_int, A_v=av)
            sed.redshiftSED(redshift, dimming=False)
            a_mw, b_mw = sed.setupCCMab()
            sed.addCCMDust(a_mw, b_mw, A_v=mwav)
            line = {'flux':None, 'mag':None}
            mag = sed.calcMag(self.bands[k])
            flux = sed.calcADU(self.bands[k], gain=1.0)
            line['mag'] = mag
            line['flux'] = flux
            self.outdata[k].append(line)
            lnum += 1
        ifh.close()

    def mkStarPhot(self):
        ifh = open(self.mfile)
        lnum = 0
        k = None
        for l in ifh:
            flds = l.rstrip().split()
            if l.startswith("Opsim_filter"):
                self.filter = self.filtmap[int(flds[1])]
                k = self.filter
        ifh.close()
        ifh = open(self.tfile)
        for l in ifh:
            flds = l.rstrip().split()
            if not flds[0] == "object":
                continue
            otype = flds[12]
            if otype != "point":
                continue
            id = float(flds[1])
            if not self.centdata.has_key(id):
                continue
            magNorm = float(flds[4])
            spec = flds[5]
            av = float(flds[14])
            sed = Sed()
            self.outdata['id'].append(id)
            if re.search("kurucz", spec):
              sed.readSED_flambda(self.spath+"/"+spec)
            else:
              sed.readSED_flambda(self.spath+"/"+spec)
            fluxNorm = sed.calcFluxNorm(magNorm, self.imsimband)
            sed.multiplyFluxNorm(fluxNorm)
            a, b = sed.setupCCMab()
            sed.addCCMDust(a, b, A_v=av)
            line = {'flux':None, 'mag':None}
            mag = sed.calcMag(self.bands[k])
            flux = sed.calcADU(self.bands[k], gain=1.)
            line['mag'] = mag
            line['flux'] = flux
            self.outdata[k].append(line)
            lnum += 1
        ifh.close()

    def printComp(self):
        ofh = open(self.ofile, "w")
        for id, mag in zip(self.outdata['id'], self.outdata[self.filter]):
            if self.centdata.has_key(id):
                line = (id,mag['mag'],mag['flux'],self.centdata[id]['photons'])
                ofh.write(",".join([str(el) for el in line])+"\n")
        ofh.close()

if __name__ == "__main__":
    if not len(sys.argv) == 5:
        print "usage: testPhotTrim.py metafile trimfile centroidfile outputfile"        
        quit()
    metafile = sys.argv[1]
    infile = sys.argv[2]
    centfile = sys.argv[3]
    outfile = sys.argv[4]
    tpt = testPhotTrim(metafile,infile,centfile,outfile)
    tpt.mkStarPhot()
    tpt.mkGalPhot()
    tpt.printComp()
