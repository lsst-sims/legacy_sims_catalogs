# execfile('testThrottleObsList.py')

import os, sys, time
from lsst.sims.catalogs.generation.db import jobDB
import throttleUtils

maxNumJobs = int(sys.argv[1])
radDeg = float(sys.argv[2])
waitTime = 5

possibleTestModeStr = ''
if len(sys.argv) > 3:
    if sys.argv[-1].lower() == 'testmode':
        possibleTestModeStr = ' testMode'
        print '>>> Using test mode.'
        
obsList = []
f = open('inList.txt', 'r')
for line in f:
    t0 = line.split()
    if len(t0) != 1: continue
    obsList.append(t0[0])

f.close()
print 'obsList:', obsList

numJobs = len(obsList)

executionDBManager = jobDB.JobState()
t0 = executionDBManager.getJobId()

nFN = '%s_%s' % (t0.getOwner(), t0.getId())
print 'Using job ID: %s' % nFN


for i in range(len(obsList)):
    jobId = '%s_%i' % (nFN, i)
    throttleUtils.throttle(executionDBManager, maxNumJobs, waitTime)
    # For now, call addJob() before actually starting the job,
    #  because there could be a race condition if addJob()
    #  and removeJob() are called simultaneously.
    t0 = int(time.time())
    t1 = '%s_%i' % (obsList[i], t0)
    throttleUtils.addJob(executionDBManager, jobId, t1)

    #jobIdPlusI = jobId + '_' + str(i)
    cwd0 = os.getcwd()
    f0 = open('/share/home/krughoff/catalogGenFramework/sTScripts/tempST%s.csh' % jobId, 'w')
    f0.write('#!/bin/csh\n#PBS -N %i_sT%s\n#PBS -l qos=astro,walltime=47:59:59,nodes=1:ppn=1\n#PBS -e /share/home/krughoff/catalogGenFramework/out/sT%s.err\n#PBS -o /share/home/krughoff/catalogGenFramework/out/sT%s.out\n\ncd %s\nsource setupOldAthena.csh\npython ./testThrottleObsListRun.py %s %s %s %s%s\necho Finished.' % (i, jobId, jobId, jobId, cwd0, nFN, jobId, obsList[i], str(radDeg), possibleTestModeStr))
    f0.close()
    # Use this from a compute node
    t0 = 'ssh minerva0 "(cd %s; /opt/torque/bin/qsub ./sTScripts/tempST%s.csh)"' % (cwd0, jobId)
    #t0 = '/opt/torque/bin/qsub ./sTScripts/tempST%s.csh' % (jobId)
    print t0
    os.system(t0)

print 'Quitting, as all jobs have at least been submitted.  State:'
throttleUtils.showStates(executionDBManager)
