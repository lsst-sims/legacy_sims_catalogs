#!/usr/bin/env python
import time
from lsst.sims.catalogs.generation.db import JobId, JobState
import sys

if __name__ == "__main__":
    if len(sys.argv) == 1:
        jid = 77
        owner = 'anon'
    elif len(sys.argv) == 2:
        jid = int(sys.argv[1])
        owner = 'anon'
    elif len(sys.argv) == 3:
        jid = int(sys.argv[1])
        owner = sys.argv[2]
    else:
        print "usage: python getStateList.py [jobid] [owner]"
    jobid = JobId(id=jid, owner=owner)
    js = JobState(jobid=jobid)
    states = js.showStates()
    print "***Printing all keys***"
    for k in states.keys():
        print "%s %s"%(k, states[k])
