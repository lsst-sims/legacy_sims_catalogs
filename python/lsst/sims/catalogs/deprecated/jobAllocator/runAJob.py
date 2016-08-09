import os, sys
import jobAllocator

def makeTrimCatalog(workDir, chunkSize0, obsHistId, maxCats):
    if not os.path.exists(workDir):
        raise RuntimeError, '*** workDir does not exist: %s' % workDir
    j = jobAllocator.JobAllocator(
        workDir=workDir, chunkSize=chunkSize0, maxCats=maxCats)
    # For some reason, need to use square brackets
    j.startCatalogs(['TRIM'], 'TEST QUERY', obsHistId)

if __name__ == '__main__':
    obsHistId = sys.argv[1]
    chunkSize0 = int(sys.argv[2])
    workDir = sys.argv[3]
    maxCats = int(sys.argv[4])
    print 'Starting...'
    print '   obsHistId: %s' % obsHistId
    print '   chunkSize: %i' % chunkSize0
    print '   workDir: %s' % workDir
    print '   maxCats: %i' % maxCats
    makeTrimCatalog(workDir, chunkSize0, obsHistId, maxCats)
