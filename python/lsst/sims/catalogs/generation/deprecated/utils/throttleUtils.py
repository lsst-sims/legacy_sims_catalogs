# execfile('throttleUtils.py')

import os, sys, time
from lsst.sims.catalogs.generation.db import jobDB

def getCopyDBM(iD):
    eDBM = jobDB.JobState(iD)
    return eDBM

def showStates(eDBM):
    d = eDBM.showStates()
    for k in d.keys():
        print k + ': ' + d[k]

def addJob(eDBM, jobId, extraText):
    # This is a concurrency issue, as the job states could change
    #  between the time we check the number and the time we increment,
    #  etc.  We really need to lock the DB.
    t0 = eDBM.queryState('NumJobsRunning')
    if t0 == None: t0 = 0
    print 'addJob: Current num: ', t0
    t1 = int(t0) + 1
    eDBM.updateState('NumJobsRunning', str(t1))
    print 'addJob: New num: ', t1
    eDBM.updateState(jobId, 'JobAdded_%s' % (extraText))
    print 'Added job to execution DB: %s' % jobId
    showStates(eDBM)

def removeFinishedJob(eDBM, jobId, extraText):
    # This is a concurrency issue, as the job states could change
    #  between the time we check the number and the time we increment,
    #  etc.  We really need to lock the DB.
    t0 = eDBM.queryState('NumJobsRunning')
    if t0 == None: t0 = 0
    print 'removeFinishedJob: Current num: ', t0
    t1 = int(t0) - 1
    eDBM.updateState('NumJobsRunning', str(t1))
    print 'removeFinishedJob: New num: ', t1
    eDBM.updateState(jobId, 'JobFinished_%s' % (extraText))
    print 'Removed finished job from execution DB: %s' % jobId
    showStates(eDBM)

def removeFailedJob(eDBM, jobId, extraText):
    # This is a concurrency issue, as the job states could change
    #  between the time we check the number and the time we increment,
    #  etc.  We really need to lock the DB.
    t0 = eDBM.queryState('NumJobsRunning')
    if t0 == None: t0 = 0
    print 'removeFailedJob: Current num: ', t0
    t1 = int(t0) - 1
    eDBM.updateState('NumJobsRunning', str(t1))
    print 'removeFailedJob: New num: ', t1
    eDBM.updateState(jobId, 'JobFailedAndRemoved_%s' % (extraText))
    print 'Removed failed job from execution DB: %s' % jobId
    showStates(eDBM)

def throttle(eDBM, maxNumJobs, throttleTime):
    print 'throttle: maxNumJobs is ', maxNumJobs
    done = False
    while done == False:
        numJobs = howManyJobs(eDBM)
        print 'throttle: current numJobs is ', numJobs
        if numJobs >= maxNumJobs:
            print 'Max reached; sleeping...'
            time.sleep(throttleTime)
            print 'Waking to check again.'
        else:
            done = True

def howManyJobs(eDBM):
    t0 = eDBM.queryState('NumJobsRunning')
    if t0 == None: t0 = 0
    else: t0 = int(t0)
    print 'howManyJobs: Current num: ', t0
    return t0
