import unittest, os
import jobAllocator

class JobAllocatorTest(unittest.TestCase):
    #def testinit(self):
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/sdata1/rgibson/'):
    #        workDir = '/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir)
    #    self.assertEqual(j.nJobs, None)
    #    self.assertEqual(j.catalogTypes, None)
    #    self.assertNotEqual(j.metaDataManager, None)
    #    self.assertNotEqual(j.catalogTypeManager, None)
    #    self.assertNotEqual(j.uIToDBManager, None)

    #def testreset(self):
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/sdata1/rgibson/'):
    #        workDir = '/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir)
    #    j.metaDataManager.isCleared = False
    #    j.catalogTypeManager.isCleared = False
    #    j.uIToDBManager.isCleared = False
    #    j.mergedMetaData = -1
    #    j.nJobs = -1
    #    j.catalogTypes = -1
    #    j.chunkSize = -1
    #    j.reset()
    #    self.assertEqual(j.nJobs, None)
    #    self.assertEqual(j.catalogTypes, None)
    #    self.assertEqual(j.chunkSize, None)
    #    self.assertNotEqual(j.metaDataManager, None)
    #    self.assertNotEqual(j.catalogTypeManager, None)
    #    self.assertNotEqual(j.uIToDBManager, None)
    #    self.assert_(j.metaDataManager.isCleared)
    #    self.assert_(j.catalogTypeManager.isCleared)
    #    self.assert_(j.uIToDBManager.isCleared)

    #def testclearMetaData(self):
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/sdata1/rgibson/'):
    #        workDir = '/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir)
    #    j.metaDataManager.isCleared = False
    #    j.clearMetaData()
    #    self.assert_(j.metaDataManager)
    #    self.assert_(j.metaDataManager.isCleared == True)

    #def testAddMetaData0(self):
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/sdata1/rgibson/'):
    #        workDir = '/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir)
    #    testDict = { 'a' : 1, 'b' : 2 }
    #    j.addMetaData(testDict)
    #    d = j.metaDataManager.dict['main']
    #    self.assertEqual(len(d.keys()), len(testDict.keys()))
    #    for k in d.keys():
    #        self.assertEqual(d[k], testDict[k])
    #    # Testing beyond this level is up to the MetaData class
        
    #def testAddMetaDataN(self):
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/sdata1/rgibson/'):
    #        workDir = '/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir)
    #    n = 100
    #    for i in range(n):
    #        j.addMetaData({ str(i) : i })
    #    d = j.metaDataManager.dict['main']
    #    self.assertEqual(len(d.keys()), n)
    #    for i in range(n):
    #        self.assert_(d.has_key(str(i)))
    #    # Testing beyond this level is up to the MetaData class

    #def teststartStubCatalog(self):
    #    j = jobAllocator.JobAllocator(chunkSize=10)
    #    # For some reason, need to use square brackets
    #    j.startCatalogs(['STUB'], 'TEST QUERY', '85748128')

    #def testSmallTrimCatalogMultiType(self):
    #    print 'In testSmallTrimCatalog()'
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/athena/share/sdata1/rgibson/'):
    #        workDir = '/share/athena/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir, chunkSize=2000, maxCats=3)
    #    # For some reason, need to use square brackets
    #    # Note that "STARS" contains both MS and WD stars!
    #    j.startCatalogs(['TRIM'], ['WDSTARS', 'MSSTARS', 'GALAXY', 'SSM'], '85748128')

    def testSmallTrimCatalog(self):
        print 'In testSmallTrimCatalog()'
        if os.path.exists('/local/tmp/'):
            workDir = '/local/tmp/jobAllocator/'
        elif os.path.exists('/share/athena/share/sdata1/rgibson/'):
            workDir = '/share/athena/share/sdata1/rgibson/jobAllocator/'
        else:
            raise RuntimeError, '*** No place for workDir.'
        j = jobAllocator.JobAllocator(workDir=workDir, chunkSize=2000, maxCats=3)
        # For some reason, need to use square brackets
        j.startCatalogs(['TRIM'], ['ALLSTARS', 'GLENS', 'IMAGE', 'EASTEREGGS', 'SSM', 'GALAXY_BULGE', 'GALAXY_DISK', 'AGN'], '85748128')

    #def testFullTrimCatalog(self):
    #    print 'In testFullTrimCatalog()'
    #    if os.path.exists('/local/tmp/'):
    #        workDir = '/local/tmp/jobAllocator/'
    #    elif os.path.exists('/share/athena/share/sdata1/rgibson/'):
    #        workDir = '/share/athena/share/sdata1/rgibson/jobAllocator/'
    #    else:
    #        raise RuntimeError, '*** No place for workDir.'
    #    j = jobAllocator.JobAllocator(workDir=workDir, chunkSize=500000, maxCats=-1)
    #    # For some reason, need to use square brackets
    #    j.startCatalogs(['TRIM'], ['ALLSTARS', 'GLENS', 'IMAGE', 'EASTEREGGS', 'SSM', 'GALAXY_BULGE', 'GALAXY_DISK', 'AGN'], '85748128')

if __name__ == '__main__':
    unittest.main()
