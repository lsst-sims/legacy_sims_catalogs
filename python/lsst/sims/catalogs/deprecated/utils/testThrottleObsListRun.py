# execfile('testThrottleObsListRun.py')

import os, sys, time, random, signal
from lsst.sims.catalogs.generation.db import jobDB
import throttleUtils

def signalHandler(sig, func=None):
    print '*** CAUGHT SIGNAL; EXITING.'
    sys.exit(1)

if __name__ == '__main__':
    print 'Registering signal handler.'
    signal.signal(signal.SIGTERM, signalHandler)
    
    print 'Started with args:'
    for i in range(1, len(sys.argv)):
        print sys.argv[i]

    testMode = False
    if sys.argv[-1].lower() == 'testmode':
        testMode = True

    t0 = sys.argv[1].split('_')
    jobId = jobDB.JobId(int(t0[1]), owner=t0[0])
    d = jobDB.JobState(jobId)
    procId = sys.argv[2]
    obsId = sys.argv[3]
    rad = sys.argv[4]
    startTime = time.time()
    t0 = int(startTime)
    d.updateState(procId, 'JobRunning_%s_%i' % (obsId, t0))
    print 'Update state: %s to JobRunning_%s_%i' % (procId, obsId, t0)
    #HACK this should be changed to just call the catalog generation classes
    #Rob says this may be an issue because he checks the error code on exit.
    if testMode == False:
        t0 = 'python $CATALOGS_GENERATION_DIR/bin/runFiles.py %s %s'
        t1 = t0 % (obsId, rad)
    else:
        t0 = None
        t1 = 'python $CATALOGS_GENERATION_DIR/bin/fakeRunFiles.py'
        
    succeeded = False
    nAttempsRemaining = 10
    numRetries = 0
    rc = -1
    while succeeded == False and nAttempsRemaining > 0:
        print t1
        rc = os.system(t1)
        if (rc >> 8) == 0:
            t0 = int(time.time()-startTime)
            d.updateState(procId, 'JobFinished_%s_%i' % (obsId, t0))
            print 'Updated state: %s to JobFinished_%s_%i' % (
                procId, obsId, t0)
            succeeded = True
        else:
            numRetries += 1
            nAttempsRemaining -= 1
            t0 = int(time.time()-startTime)
            d.updateState(procId, 'JobRetry_%s_%s_%i' % (
                obsId, str(numRetries), t0))
            print 'Updated state: %s to JobRetry_%s_%s_%i' % (
                procId, obsId, str(numRetries), t0)
            # Sleep a while before restarting in case DB is swamped
            if testMode == False:
                t2 = random.random() * 20 + 10
                time.sleep(t2*60)
            else:
                time.sleep(1)

    t0 = int(time.time()-startTime)
    extraStr = '%s_%i_%i' % (obsId, numRetries, t0)
    if succeeded == True:
        print "Writing success to database"
        throttleUtils.removeFinishedJob(d, procId, extraStr)
    else:
        throttleUtils.removeFailedJob(d, procId, extraStr)
        
    throttleUtils.showStates(d)
