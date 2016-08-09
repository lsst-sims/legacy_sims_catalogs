import os, sys, time, cPickle, time, copy
from jobAllocatorStubs import *
import getFileNameWC
from lsst.sims.catalogs.generation.db import queryDB
from lsst.sims.catalogs.generation.db import jobDB
import lsst.sims.catalogs.measures.utils as mUtils

class JobAllocator:

    nJobs = None
    catalogTypes = None
    chunkSize = None

    metaDataManager = None
    catalogTypeManager = None
    uIToDBManager = None
    DBManager = None
    executionDBManager = None

    # I don't remember what these are for...
    chunkOutputs = None #???
    fileName = None #???
    directory = None #???
    
    def __init__(self, workDir='/local/tmp/jobAllocator/', chunkSize=1000, maxCats=-1, queryOnly=False):
        print 'In JA __init__()'
        print 'Creating JA with chunkSize: %i, maxCats: %i' % (chunkSize, maxCats)
        self.nJobs = None
        self.catalogTypes = None
        self.chunkSize = chunkSize
        self.metaDataManager = MetaDataManager()
        self.catalogTypeManager = CatalogTypeManager()
        self.uIToDBManager = UIToDBManager()
        #self.executionDBManager = jobDB.JobState()
        if not os.path.exists(workDir):
            os.system('mkdir %s' % workDir)
        self.WorkDir = workDir.rstrip('/') + '/'
        self.nextFileNum = 0
        self.maxCats = maxCats
        if self.maxCats < 1 and self.chunkSize < 100000:
            raise RuntimeError, '*** You are not allowed to swamp the network.'
        self.QueryOnly = queryOnly
        print 'Leaving JA __init()__'
        
    def reset(self):
        print 'In JA reset()'
        self.nJobs = None
        self.catalogTypes = None
        self.chunkSize = None
        if self.metaDataManager:
            self.metaDataManager.reset()
        if self.catalogTypeManager:
            self.catalogTypeManager.reset()
        if self.uIToDBManager:
            self.uIToDBManager.reset()
        #if self.executionDBManager:
        #    self.executionDBManager.reset()
        self.WorkDir = None
        self.nextFileNum = 0
        print 'Leaving JA reset()'

    def clearMetaData(self):
        """Flush the current metaData."""
        self.metaDataManager.reset()

    def addMetaDataFromFiles(metaDataFileList):
        """Merge metaData from files in the list into the current set
        of metaData.  This is a bit vague right now; how it works depends
        on the metaData classes."""
        for m in metaDataFileList:
            self.metaDataManager.mergeFromFile(m)
    
    def addMetaData(self, metaData):
        self.metaDataManager.merge(metaData)

    def verifyMetaData():
        """For each registered catalog type, check that appropriate
        data is present.  Then call lower-level metaData verify()."""
        for c in catalogTypes:
            self.catalogTypeManager.verifyMetaData(c, self.metaDataManager)

    def getNextGoodFileNum(self):
        nTry = 0
        while len(getFileNameWC.do(self.WorkDir, '*%i*.ja' % self.nextFileNum)) > 0:
            self.nextFileNum += 1
            nTry += 1
            if nTry > 10000:
                raise RuntimeError, '*** Could not find a unique file number.'
        return self.nextFileNum

    def startCatalogs(self, catalogTypes, queryTypes, obsHistID):
        for ct in catalogTypes:
            self.doOneCatalogType(ct, queryTypes, obsHistID)

    def doOneCatalogType(self, catalogType, queryTypes, obsHistID):
        #nFN = self.getNextGoodFileNum()
        fullTimeStart = time.time()
        self.executionDBManager = jobDB.JobState()
        t0 = self.executionDBManager.getJobId()

        nFN = '%s_%s' % (t0.getOwner(), t0.getId())
        print 'Using job ID: %s' % nFN
        print 'queryTypes:', queryTypes
        jobNum = 0
        jobTypes = []; jobNums = []; jobPickleFiles = []; useTypes = []
        allOutputFiles = []; curMD = None
        self.metaDataManager.reset()
        os.system('free -m')
        for objectType in queryTypes:
            if objectType not in useTypes: useTypes.append(objectType)
            print 'Getting first %s instance catalog of size %i...' % (
                objectType, self.chunkSize)
            t0 = time.time()
            myQDB = queryDB.queryDB(
                chunksize=self.chunkSize, objtype=objectType)
            print '   ...setting up QDB took %i sec.' % (time.time() - t0)
            t0 = time.time()
            instanceCat = myQDB.getInstanceCatalogById(obsHistID)
            print '   ...and getting catalog took %i sec.' % (time.time() - t0)

            numCats = 0
            if instanceCat != None:
                # This code adds some needed fields to the metadata
                mUtils.trimGeneration.derivedTrimMetadata(instanceCat)
                os.system('free -m')
                # Deep copy so we can store this after instanceCat disappears
                if curMD == None:
                    curMD = copy.deepcopy(instanceCat.metadata)
                else:
                    curMD.mergeMetadata(instanceCat.metadata)

            while instanceCat != None:
                t0 = self.WorkDir + 'catData%s_%i.ja' % (nFN, jobNum)
                t1 = self.WorkDir + 'catData%s_%i.p' % (nFN, jobNum)
                print 'Now pickling query type: %s' % objectType
                # Store job data files in instance
                time0 = time.time()
                instanceCat.jobAllocatorDataFile = t0
                allOutputFiles.append(t0) # Order is important
                instanceCat.jobAllocatorCatalogType = catalogType
                instanceCat.jobAllocatorObjectType = objectType
                cPickle.dump(instanceCat, open(t1, 'w'))
                print '   ...pickling took %i sec.' % (time.time() - time0)
                jobTypes.append(catalogType)
                jobNums.append(jobNum)
                jobPickleFiles.append(t1)
                jobNum += 1
                if numCats > 0:
                    curMD.mergeMetadata(instanceCat.metadata)

                # *** RRG:  Free up memory somehow here for instanceCat...
                del(instanceCat); instanceCat = None
                os.system('free -m')
                if self.maxCats >= 0 and (numCats + 1) >= self.maxCats:
                    instanceCat = None
                else:
                    print 'Querying DB for next chunk.'
                    t0 = time.time()
                    instanceCat = myQDB.getNextChunk()
                    print '   ...took %i sec.' % (time.time() - t0)
                    if instanceCat != None:
                        # This code adds some needed fields to the metadata
                        mUtils.trimGeneration.derivedTrimMetadata(instanceCat)
                    os.system('free -m')
                    numCats += 1

        # RRG:  For now this must be disabled
        #curMD.validateMetadata(catalogType, myQDB.opsim)
        mFName = self.WorkDir + 'metaData%s_%s.ja' % (nFN, catalogType)
        curMD.writeMetadata(mFName, catalogType, myQDB.opsim, newfile=True)

        # Finished with queryDB; clean up nicely.
        myQDB.closeSession()
        
        # For debug mode, don't start the clients
        if self.QueryOnly == True:
            print 'Full time for this file: %i sec' % (time.time()-fullTimeStart)
            print 'DEBUG:  Finished, no client processes started.'

        # Now fire off the jobs
        for i in range(len(jobNums)):
            jobId = '%s_%i' % (nFN, jobNums[i])
            self.executionDBManager.updateState(jobId, 'JAAdded')
            print 'Added job to execution DB: %s' % jobId
            #t0 = '/astro/apps/pkg/python64/bin/python jobAllocatorRun.py %i %s %s&' % (nFN, jobId, jobPickleFiles[i])
            #t0 = 'qsub ./runOneAthena.csh %i %s %s&' % (nFN, jobId, jobPickleFiles[i])
            #t0 = 'ssh minerva0 "(cd $PBS_O_WORKDIR; qsub ./runOneAthena.csh %i %s %s)"' % (nFN, jobId, jobPickleFiles[i])
            cwd0 = os.getcwd()
            f0 = open('tmpJA%s.csh' % jobId, 'w')
	    f0.write('#!/bin/csh\n#PBS -N jA%s\n#PBS -l walltime=1:00:00\n#PBS -e jA%s.err\n#PBS -o jA%s.out\ncd %s\nsource setupAthena.csh\npython jobAllocatorRun.py %s %s %s\necho Finished.' % (jobId, jobId, jobId, cwd0, nFN, jobId, jobPickleFiles[i]))
            f0.close()
            t0 = 'ssh minerva0 "(cd %s; /opt/torque/bin/qsub tmpJA%s.csh)"' % (cwd0, jobId)
            print t0
            os.system(t0)

        # Check that everything started within a certain time limit
        # On minerva, jobs may be queued indefinitely, so this won't work
        for i in range(len(jobNums)):
            jobId = '%s_%i' % (nFN, jobNums[i])
            tryNum = 0
            t0 = self.executionDBManager.queryState(jobId)
            while t0 != 'JAFinished':
                print 'Try %i: JA sees state for %s: %s' % (tryNum, jobId, t0)
                time.sleep(10)
                # Give it up to a day
                if tryNum > 60 * 60 * 24:
                    raise RuntimeError, '*** Job not started: %s' % jobId
                tryNum += 1
                t0 = self.executionDBManager.queryState(jobId)
            print 'Finished (Try %i):  JA sees state for %s: %s' % (tryNum, jobId, t0)

        # Finally, merge the output trim file
        trimFile = self.WorkDir + 'trim%s_%s.ja' % (nFN, catalogType)
        t0 = 'cat %s > %s' % (mFName, trimFile)
        print t0
        os.system(t0)
        for f in allOutputFiles:
            t0 = 'cat %s >> %s' % (f, trimFile)
            print t0
            os.system(t0)
        print 'Full time for this file: %i sec' % (time.time()-fullTimeStart)
        print 'Finished catting trim file: ', trimFile

        

    # Private
    def _setnJobs(self, nJobs):
        """Verify value and then assign it."""
        verifynJobs(nJobs)
        self.nJobs = nJobs
        
    def _setcatalogTypes(self, catalogTypes):
        """Verify value and then assign it."""
        verifycatalogTypes(catalogTypes)
        self.catalogTypes = catalogTypes
        
    def _setchunkSize(self, chunkSize):
        """Verify value and then assign it."""
        verifychunkSize(chunkSize)
        self.chunkSize = chunkSize

    def _verifynJobs(nJobs):
        """If value is not appropriate, raise a ValueError exception."""

    def _verifycatalogTypes(catalogTypes):
        """If value is not appropriate, raise a ValueError exception."""

    def _verifychunkSize(chunkSize):
        """If value is not appropriate, raise a ValueError exception."""

        




#Controller -> JobAllocator: clear()
#Controller -> JobAllocator: addMetaDataList(metaDataFileList)
#JobAllocator -> MetaDataManager: load(metaDataFile0)
#MetaDataManager -> JobAllocator: data
#JobAllocator -> MetaDataManager: load(metaDataFileN)
#MetaDataManager -> JobAllocator: data
#JobAllocator -> MetaDataManager: mergeMetaDataList(metaDataList)
#MetaDataManager -> JobAllocator: Done
#JobAllocator -> MetaDataManager: verifyMetaData(metaDataList)
#JobAllocator -> Controller: Done
#Controller -> JobAllocator: makeCatalogs(catalogTypes, uIInfo, chunkSize=None, nJobs=None)
#JobAllocator -> CatalogTypeManager: verifyMetaData(catalogTypes, metaData)
#CatalogTypeManager -> JobAllocator: Done
#JobAllocator -> JobAllocator: writeMetaDataFile(outFile)
#JobAllocator -> UIToDBManager: constructDBQuery(uIInfo)
#UIToDBManager -> JobAllocator: dBQuery
#JobAllocator -> DBQuery: initialize(dBQuery, chunkSize)
#JobAllocator -> DBQuery: queryNextChunk()
#DBQuery -> JobAllocator: dBChunk
#JobAllocator -> InstanceCatalog: instantiate(catalogTypes, metaDataFile, dBChunk)
#InstanceCatalog -> CatalogTypeManager: verifyMetaData(catalogTypes, metaData)
#CatalogTypeManager -> InstanceCatalog: Done
#InstanceCatalog -> JobAllocator: jobParameters
#JobAllocator -> ExecutionDB: newJob(jobParameters)
#ExecutionDB -> JobAllocator: Done
#JobAllocator -> Controller: Done(executionDBTable)
