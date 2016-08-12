import sys, random, time
from lsst.sims.catalogs.generation.db import jobDB

def howManyJobs(eM, jobid):
    t0 = eM.queryState(jobid + '_NumJobs')
    if t0 == None: t0 = 0
    else: t0 = int(t0)
    return t0

def qsubJob(eM, stateKey, jobid):
    t0 = howManyJobs(eM, jobid)
    print 'AddedJob: Current number of active jobs: ', t0
    t1 = int(t0) + 1
    eM.updateState(jobid + '_NumJobs', str(t1))
    print 'AddedJob: New number of active jobs: ', t1
    eM.updateState(stateKey, 'QSUBBED')
    eM.updateState(stateKey + '_QT', time.ctime())
    print 'Updated the state of job %s as QSUBBED.' %(stateKey)

def jobRunning(eM, stateKey, jobid):
    eM.updateState(stateKey, 'RUNNING')
    eM.updateState(stateKey + '_RT', time.ctime())
    print 'Updated the state of job %s as RUNNING.' %(stateKey)

def jobFinished(eM, stateKey, jobid):
    t0 = howManyJobs(eM, jobid)
    print 'AddedJob: Current number of active jobs: ', t0
    t1 = int(t0) - 1
    eM.updateState(jobid + '_NumJobs', str(t1))
    print 'AddedJob: New number of active jobs: ', t1
    eM.updateState(stateKey, 'FINISHED')
    eM.updateState(stateKey + '_FT', time.ctime())
    print 'Updated the state of job %s as FINISHED.' %(stateKey)

def jobError(eM, stateKey, jobid):
    t0 = howManyJobs(eM, jobid)
    print 'AddedJob: Current number of active jobs: ', t0
    t1 = int(t0) - 1
    eM.updateState(jobid + '_NumJobs', str(t1))
    print 'AddedJob: New number of active jobs: ', t1
    eM.updateState(stateKey, 'FAILED')
    eM.updateState(stateKey + '_ET', time.ctime())
    print 'Updated the state of job %s as FAILED.' %(stateKey)

if not len(sys.argv) == 5:
    print "usage: %python myJobTracker.py obshistid state sensorId username"
    quit()
    
obshistid = sys.argv[1]
state = sys.argv[2]
# Must be in the form: sensorId = rx+ry+'_'+sx+sy+'_'+ex
sensorId = sys.argv[3]
username = sys.argv[4]

rxry, sxsy, ex = sensorId.split('_')

raftmap = {"01":"0,1", "02":"0,2", "03":"0,3", \
           "10":"1,0", "11":"1,1", "12":"1,2", "13":"1,3", "14":"1,4", \
           "20":"2,0", "21":"2,1", "22":"2,2", "23":"2,3", "24":"2,4", \
           "30":"3,0", "31":"3,1", "32":"3,2", "33":"3,3", "34":"3,4", \
           "41":"4,1", "42":"4,2", "43":"4,3"}

sensormap = {"00":"0,0", "01":"0,1", "02":"0,2", \
             "10":"1,0", "11":"1,1", "12":"1,2", \
             "20":"2,0", "21":"2,1", "22":"2,2"}

# constructed to have the form "R:rx,ry S:sx,sy:snap"
# which is how the fpaFig.map keys are constructed
sensorid = "R:"+raftmap[rxry]+" "+"S:"+sensormap[sxsy]+":"+ex

jobid = jobDB.JobId(id=obshistid, owner=username)
jobStr = str(jobid)
eM = jobDB.JobState(jobid=jobid)
stateKey = jobStr + '_%s' %(sensorid) + '_JS'

if state == 'qsubbed':
    qsubJob(eM, sensorid, jobStr)

if state == 'running':
    jobRunning(eM, sensorid, jobStr)

if state == 'finished':
    jobFinished(eM, sensorid, jobStr) 

if state == 'error':
    jobError(eM, sensorid, jobStr) 
