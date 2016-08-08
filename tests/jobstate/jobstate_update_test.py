#!/usr/bin/env python
from lsst.sims.catalogs.generation.db import JobId, JobState
import sys

if len(sys.argv) < 3:
  print "Usage jobstate_update_test.py jobid owner"
  sys.exit(1)

id = int(sys.argv[1])
owner = sys.argv[2]
jid = JobId(id, owner)
js = JobState(jobid=jid)
jobid = js.getJobId()
print jobid
print js.queryState("mykey")
print js.queryState("newkey")
js.updateState("mykey","value from second process")
print js.queryState("mykey")
js.updateState("newprocesskey", "brand new key/value from second process")
print js.queryState("newprocesskey")
print js.showStates()
