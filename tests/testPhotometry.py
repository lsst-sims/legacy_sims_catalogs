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

    def testMagnitudes(self):
        
        testObject=photometryTestClass()
        
        
        directory="data/"
        seds=["test_sed_0.dat"]
        for i in range(9):
            name="test_sed_%d.dat" % (i+1)
            seds.append(name)
        
        sedDict=testObject.loadSeds(seds,directory)
        


def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(photometryUnitTest)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(),shouldExit)

if __name__ == "__main__":
    run(True)
