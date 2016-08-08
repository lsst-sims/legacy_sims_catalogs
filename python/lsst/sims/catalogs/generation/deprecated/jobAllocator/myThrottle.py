import sys, random, time
from lsst.sims.catalogs.generation.db import jobDB

def howManyJobs(eM, tableId):
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    if t0 == None: t0 = 0
    else: t0 = int(t0)
    print 'howManyJobs: Current num: ', t0
    return t0

def addJob(eM, tableId):
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    if t0 == None: t0 = 0
    print 'addJob: Current num: ', t0
    t1 = int(t0) + 1
    eM.updateState(tableStr + 'NumJobs', str(t1))
    print 'addJob: New num: ', t1
    eM.showStates()

def removeJob(eM, tableId):
    tableStr = str(tableId)
    t0 = eM.queryState(tableStr + 'NumJobs')
    print 'addJob: Current num: ', t0
    t1 = int(t0) - 1
    eM.updateState(tableStr + 'NumJobs', str(t1))
    print 'addJob: New num: ', t1
    
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


tableId = sys.argv[1]
print 'Using tableId: ', tableId
eM = jobDB.JobState(tableId)

for i in range(100):
    howManyJobs(eM, tableId)
    print '--------'
    throttle(eM, tableId, 100, 60)
    addJob(eM, tableId)
    print '--------'

