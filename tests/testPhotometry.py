import numpy

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.photometry.photUtils import Photometry

class photometryTestClass(Photometry):
    def __init__(self):
        pass

class photometryUnitTest(unittest.TestCase):

    testObject=photometryTestClass()
    directory="data/"
    sedNames=["test_sed_0.dat"]
    for i in range(9):
        name="test_sed_%d.dat" % (i+1)
        sedNames.append(name)
        
    sedDict=testObject.loadSeds(sedNames,directory)   
   
    filterRoot="test_bandpass_"
    filterlist=('u','g','r','i','z')
    bandpassDict=testObject.loadBandpasses(filterlist=filterlist,dataDir=directory,filterroot=filterRoot)
    
    def testPhiArray(self):
        
        phiArray, wavelenstep = self.testObject.setupPhiArray_dict(self.bandpassDict,self.filterlist)
        
        for i in range(len(self.filterlist)):
            phiName=self.directory+'test_phi_'+self.filterlist[i]+'.dat'
            phiFile=open(phiName,"r")
            lines=phiFile.readlines()
            for j in range(len(phiArray[i])):
                values = lines[j].split()
                wavelen = float(values[0])
                phi = float(values[1])
                self.assertAlmostEqual(phi,phiArray[i][j],7)    
            phiFile.close()
        
def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
