#!/usr/bin/env python
from lsst.sims.catalogs.generation.db import JobState
import sys

js = JobState()
jobid = js.getJobId()
print jobid.getOwner(), jobid.getId()
js.updateState("mykey","my value")
print js.queryState("mykey")
js.updateState("mykey","now my value")
print js.queryState("mykey")
js.updateState("newkey", "my new key")
print js.queryState("newkey")
print "***Printing all keys***"
states = js.showStates()
for k in states.keys():
  print "%s %s"%(k, states[k])
jids = js.getJobIdsByOwner('anon')
for jid in jids:
  print jid.getId()
