import sys, random, time
from lsst.sims.catalogs.generation.db import jobDB

def howManyJobs(eM, tableId, jobInt):
    tableStr = str(tableId)
    tableInt = int(tableId)
    eM = jobDB.JobState(tableInt)
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    if t0 == None: t0 = 0
    else: t0 = int(t0)
    print 'howManyJobs: Current num: ', t0
    return t0

def qsubJob(eM, tableId, jobId):
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    if t0 == None: t0 = 0
    print 'AddedJob: Current num: ', t0
    t1 = int(t0) + 1
    eM.updateState(tableStr + 'NumJobs', str(t1))
    print 'AddedJob: New num: ', t1
    jobKey = tableStr + '_%s' %(jobId) + 'JS'
    eM.updateState(jobKey, 'QSUBBED')
    eM.updateState(jobKey + 'QT', time.ctime())
    print 'Updated the state of jobId %s as QSUBBED.' %(jobId)
    #howManyJobs(eM, tableId, jobId)
    #eM.showStates()

def jobRunning(eM, tableId, jobId):
    tableStr = str(tableId) 
    jobKey = tableStr + '_%s' %(jobId) + 'JS'
    eM.updateState(jobKey, 'RUNNING')
    eM.updateState(jobKey + 'RT', time.ctime())
    print 'Updated the state of jobId %s as RUNNING.' %(jobId)
    #eM.showStates()

def jobFinished(eM, tableId, jobId):
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    print 'addJob: Current num: ', t0
    t1 = int(t0) - 1
    eM.updateState(tableStr + 'NumJobs', str(t1))
    print 'addJob: New num: ', t1
    jobKey = tableStr + '_%s' %(jobId) + 'JS'
    eM.updateState(jobKey, 'FINISHED')
    eM.updateState(jobKey + 'FT', time.ctime())
    print 'Updated the state of %s as FINISHED.' %(jobId)
    #eM.showStates()

def throttle(eM, tableId, maxNumJobs, throttleTime):
    print 'throttle: maxNumJobs is ', maxNumJobs
    done = False
    while done == False:
        numJobs = howManyJobs(eM, tableId)
        print 'throttle: numJobs is ', numJobs
        if numJobs >= maxNumJobs:
            print 'Max reached; sleeping...'
            time.sleep(throttleTime)
            print 'Waking to check again.'
        else:
            done = True

if not len(sys.argv) == 4:
    print "usage: %python myJobMonitor.py tableId state jobId"
    quit()
    
tableId = sys.argv[1]
state = sys.argv[2]
jobId = sys.argv[3]

if state == 'qsubbed':
    tableId = int(tableId)
    eM = jobDB.JobState(tableId)
    qsubJob(eM, tableId, jobId)

if state == 'running':
    tableId = int(tableId)
    eM = jobDB.JobState(tableId)
    jobRunning(eM, tableId, jobId)

if state == 'finished':
    #jid = jobDB.JobId(id, owner)
    tableId = int(tableId)
    eM = jobDB.JobState(tableId)
    jobFinished(eM, tableId, jobId) 

if state == 'howmany':
    tableId = int(tableId)
    eM = jobDB.JobState(tableId)
    howManyJobs(eM, tableId, jobId)
