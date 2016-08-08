#!/usr/bin/env python
from lsst.sims.generation.db import jobDB
import sys

if len(sys.argv) < 2:
  print "Usage jobstate_update_test.py jobid key"
  sys.exit(1)

jid = int(sys.argv[1])
js = jobDB.JobState(jobid=jid)
jobid = js.getJobId()
print 'Querying jobID: %s, key: %s' % (jobid, sys.argv[2])
print js.queryState(sys.argv[2])
#print js.queryState("newkey")
#js.updateState("mykey","value from second process")
#print js.queryState("mykey")
#js.updateState("newprocesskey", "brand new key/value from second process")
#print js.queryState("newprocesskey")
