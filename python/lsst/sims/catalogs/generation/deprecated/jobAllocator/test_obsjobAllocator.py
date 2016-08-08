import os, sys
import jobAllocator

print 'testFullTrimCatalog()'
if os.path.exists('/local/tmp/'):
    workDir = '/local/tmp/jobAllocator/'
elif os.path.exists('/share/athena/share/sdata1/rgibson/'):
    workDir = '/share/athena/share/sdata1/rgibson/jobAllocator/'
else:
    raise RuntimeError, '*** No place for workDir.'
print 'workDir:', workDir

if sys.argv[2] = 'queryOnly':
    q0 = True
    print '*** DEBUG:  QueryOnly mode.'
else: q0 = False

j = jobAllocator.JobAllocator(workDir=workDir, chunkSize=500000, maxCats=-1, queryOnly=qO)
# For some reason, need to use square brackets
j.startCatalogs(['TRIM'], ['ALLSTARS', 'GLENS', 'IMAGE', 'EASTEREGGS', 'SSM', 'GALAXY_DISK', 'GALAXY_BULGE', 'AGN'], sys.argv[1])
