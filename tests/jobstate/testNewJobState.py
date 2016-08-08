from lsst.sims.catalogs.generation.db import JobId, JobState 

#Make a couple of ids...
myid1 = JobId(10)
myid2 = JobId(10,"simon")

#make a couple of job states
js = {}
js["1"] = JobState(myid1)
js["2"] = JobState(myid2)
js["3"] = JobState()
js["4"] = JobState(11)

for k in js.keys():
  js[k].updateState("Updating with key", "Key is %s"%k)
  js[k].updateState("Updating with another key", "Key is %s"%k)

for k in js.keys():
  js[k].showStates()

js["1"].deleteStates()
js["2"].deleteStates()
js["3"].deleteStates()
js["4"].deleteStates()
