"""

ljones

$Id$


This provides a way to generate ephemeride positions for a moving object, over a given set of ephemerides.
Extends to multiple objects (with differing ephemeris dates). 

"""

import warnings as warning
import numpy as n
import pyoorb as oo
import movingObject as mo
import useful_input as ui

def process_objlist(infilename, outfilename):
    """ Given filename of file containing ssmid / time of observation for series of objects, """
    """  generate ephemerides for all of those objects at all of the given times """
    
    # initialize JPL ephemerides for oorb
    ephem_datfile = ""
    oo.pyoorb.oorb_init(ephemeris_fname=ephem_datfile)

    # read input file and process objects / times for each object into dictionary of time lists
    filein = open(infilename, 'r')
    objidprev = 0
    objlist = []
    obsdatedict = {}
    for line in filein:
        objid, obsdate = line.split()
        objid = int(objid)
        obsdate = float(obsdate)
        if objid != objidprev: 
            objlist.append(objid)
            objidprev = objid
            obsdatedict[objid] = [obsdate]
        else:
            obsdatedict[objid].append(obsdate)
    filein.close()
        
    # open connection to orbit database
    db, cursor = ui.sqlConnect(dbname='ssm', verbose=False)
    objects = []    
    for id in objlist:
        keys = ('q', 'e', 'i', 'node', 'argperi', 't_peri', 'h_v', 'g_value', 'ssmid', 'objtype')
        query = "select q, e, i, node, argperi, t_peri, h_v, g_value, ssmid, objtype from orbits where ssmid=%d" %(id) 
        results = ui.sqlQuery(cursor, query, verbose=False)
        data = {}
        i = 0
        for key in keys:
            data[key] = float(results[0][i])
            i = i+1
        data['epoch'] = 49353.16
        mymo = mo.MovingObject(data['q'], data['e'], data['i'], data['node'],
                               data['argperi'], data['t_peri'], data['epoch'],
                               magHv=data['h_v'], phaseGv=data['g_value'],
                               objid=data['ssmid'], objtype=data['objtype'])
        objects.append(mymo)

    # generate ephemerides for each moving object
    
    fileout = open(outfilename, 'w')
    i = 0
    for mobj in objects:
        objname = objlist[i]
        mjdTaiList = obsdatedict[objname]
        mobj.calcEphemeris(mjdTaiList, obscode=807, timescale=4.0)
        i = i+1
        for mjdTai in mjdTaiList:
            thismjd = mobj.mjdTaiStr(mjdTai)
            obstime, ra, dec, dradt, ddecdt, magV = mobj.Ephemerides[thismjd].getPosition()
            print >>fileout, objname, obstime, ra, dec, dradt, ddecdt, magV
    fileout.close()


if __name__ == '__main__' :
    import sys
    filein= sys.argv[1]
    fileout = sys.argv[2]
    process_objlist(filein, fileout)
