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
    
    def testMagnitudes(self):
        pass

        


def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
