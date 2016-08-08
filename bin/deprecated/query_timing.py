import psycopg2 as pg
from datetime import datetime
import numpy
import sys

def total_hours(td):
    return (td.microseconds+(td.seconds+td.days*4*3600)*10**6)/10**6/3600.

def count_running(r, results):
    n = 0
    for res in results:
        if res[2]:
            if r[1] >= res[1] and r[1] < res[2]:

                n += 1
        else:
            if r[1] >= res[1]:
                n += 1
    return n

conn = pg.connect(host="z.astro.washington.edu", database="joblog",
        user="jobreporter", password="jobreporter")
cur = conn.cursor()
qstr = """
select stimes.jobid, stimes.start_time, ftimes.stop_time,
ftimes.stop_time-stimes.start_time runtime from
  (select jobid, max(time) start_time
     from eventlog
     where jobid in
     (select jobid
       from eventlog
       group by jobid
       order by max(time) desc)
       and pkey = 'TASK_START'
    group by jobid) stimes left outer join
    (select jobid, max(time) stop_time
      from eventlog
      where jobid in
      (select jobid
        from eventlog
        group by jobid
        order by max(time) desc)
      and pkey = 'TASK_STOP'
      group by jobid) ftimes
  on (stimes.jobid = ftimes.jobid) order by stimes.start_time;
"""
cur.execute(qstr)
results = cur.fetchall()
res_o = results[0]
data = {'id':[], 'starttime':[], 'stoptime':[], 'runtime':[]}
done = open("done.dat", "w")
running = open("running.dat", "w")
for r in results:
    num = count_running(r, results)
    dt1 = r[1].replace(tzinfo=None)
    if r[2]:
        dt2 = r[2].replace(tzinfo=None)
        done.write("%i %s %s %f %i\n"%(r[0], dt1.isoformat(), dt2.isoformat(), total_hours(r[3]), num))
    else:
        now = datetime.now()
        running.write("%i %s %s %f %i\n"%(r[0], dt1.isoformat(), None, total_hours(now-dt1), num))

done.close()
running.close()
