import random, os, numpy
import getFileNameWC

class MetaDataManager:
    def __init__(self):
        self.isCleared = True
        self.dict = {}
        self.dict['main'] = {}

    def reset(self):
        self.isCleared = True
        
    def mergeFromFile(self, inFile):
        """Load data from a file into a metaData object.  For this stub,
        we'll store the data as a dictionary of dictionaries.  The first
        dictionary is the file name (although the file doesn't really
        exist), and the referenced dictionaries are (key, value) pairs.
        Prepend the file name to each key to make sure we avoid
        collisions."""
        if self.dict.has_key(inFile):
            useDict = self.dict[inFile]
        else:
            useDict = self.dict['main']

        (keys, vals) = _fakeLoad(inFile)
        for i in range(len(keys)):
            if useDict.has_key(keys[i]):
                raise ValueError, '*** Key conflict: %s' % keys[i]
            useDict[keys[i]] = vals[i]

    def merge(self, metaData, file=None):
        if file: useDict = self.dict[file]
        else: useDict = self.dict['main']
        keys = metaData.keys()
        for i in range(len(keys)):
            if useDict.has_key(keys[i]):
                if useDict[keys[i]] != metaData[keys[i]]:
                    raise ValueError, '*** Key already present with different value: %s' % keys[i]
            useDict[keys[i]] = metaData[keys[i]]

    def verify(self, metaData):
        """Perform a low-level verification on the metaData that does
        not care about what data is present, only that syntax is correct
        and values are reasonable (?)."""
        pass

    def writeToFile(self, catalogType, fileName):
        f = open(fileName, 'w')
        for k in self.dict.keys():
            t0 = self.dict[k].keys()
            for t in t0:
                f.write('%s %s' % (k + '_' + t, str(self.dict[k][t])))
        f.close()

    def _fakeLoad(self, inFile):
        """Create a fake list of metadata properties and add it to the
        store."""
        if self.dict.has_key(inFile):
            raise ValueError, '*** File %s already loaded.' % inFile
        nPer = 10
        keys = []; vals = []
        for i in range(nPer):
            keys.append(inFile + '_' + string(i))
            vals.append('val' + string(i))
        return keys, vals

        

class CatalogType:
    def __init__(self):
        self.isCleared = True

    def reset(self):
        self.isCleared = True
        
    """Like an enum of catalog types."""
    pass

class CatalogTypeManager:
    def __init__(self):
        self.isCleared = True

    def reset(self):
        self.isCleared = True
        
    def verifyMetaData(self, catalogType, metaData):
        """Check that all the needed values are present."""

class UIToDBManager:
    def __init__(self):
        self.isCleared = True

    def reset(self):
        self.isCleared = True
        
    """Convert UI input to DB query."""
    def convert(uIInput):
        dBQuery = None
        return dBQuery

class InstanceCatalog:
    def __init__(self, obsHistID, list, catalogType):
        self.ObsHistID = obsHistID
        self.List = list
        self.MetaData = { 'ObsHistID' : obsHistID }
        self.CatalogType = catalogType

    def writeToFile(self, fileName):
        f = open(fileName, 'w')
        for i in range(len(self.List)):
            f.write('%1.3f/n' % self.List[i])
        f.close()

    def getMetaData(self):
        return self.MetaData

class DBRsp:
    def __init__(self, list, chunkSize=1000):
        self.List = list
        self.ChunkSize = chunkSize

    def getInstanceCatalogByID(self, ObsHistID, catalogType):
        self.CurChunk = self.ChunkSize
        if len(self.List) < self.ChunkSize:
            return InstanceCatalog(ObsHistID, self.List, catalogType)
        else:
            return InstanceCatalog(ObsHistID, self.List[0:self.ChunkSize], catalogType)

    def getNextChunk(self):
        if self.CurChunk >= self.ChunkSize: return None
        else:
            t0 = numpy.arange(self.CurChunk, min(len(self.List), self.CurChunk + self.ChunkSize))
            self.CurChunk += self.ChunkSize
            return InstanceCatalog(ObsHistID, self.List[t0], catalogType)


class DBQuery:
    def __init__(self):
        self.isCleared = True

    def reset(self):
        self.isCleared = True

    def query(self, query, chunksize=1000):
        t0 = numpy.empty(chunksize)
        for i in range(chunksize):
            t0[i] = random.random()
        return DBRsp(t0)

class ExecutionDBInterface:
    def __init__(self, stubDir='/local/tmp/jobAllocator/'):
        self.stubDir = stubDir.rstrip('/') + '/'
        self.isCleared = True

    def reset(self):
        self.stubDir = None
        self.isCleared = True

    def clearJob(self, jobId):
        t0 = getFileNameWC.do(self.stubDir, '*_%s.edb' % jobId)
        for t in t0:
            print 'Execution DB clearing job: %s' % t
            os.system('rm -f %s%s' % self.stubDir, t)
        
    def addNewJob(self, jobId):
        self.clearJob(jobId)
        t0 = 'added_%s.edb' % jobId
        os.system('echo "New" > %s' % (self.stubDir + t0))
        print 'Execution DB added job: %s' % t0

    def checkJobAdded(self, jobId):
        t0 = self.stubDir + 'added_%s.edb' % jobId
        print 'Execution DB checking if job added: %s' % t0
        if os.path.exists(t0): print '... job exists'
        else: print '... job does not exist (yet?)'
        return os.path.exists(t0)

    def startNewJob(self, jobId):
        if not self.checkJobAdded(jobId):
            raise RuntimeError, '*** Job not added yet: %s' % jobId
        t0 = 'started_%s.edb' % jobId
        os.system('echo "Start" > %s' % (self.stubDir + t0))
        print 'Execution DB: job started: %s' % t0

    def checkJobStarted(self, jobId):
        t0 = self.stubDir + 'started_%s.edb' % jobId
        print 'Execution DB checking if job started... %s' % t0
        if os.path.exists(t0): print '... job exists.'
        else: print '... job DNE.'
        return os.path.exists(t0)



