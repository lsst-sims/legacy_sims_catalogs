#!/usr/bin/env python
import sys
sys.path.append("./movingObjects")
import math
import pg
#import pyPgSQL as pg
import movingObject as mo
import movingObjectList as mol

class cosmoDB:
    def __init__(self, host = "deathray.astro.washington.edu", user = "cosmouser", passwd = "cosmouser", dbname = "cosmoDB.11.19.2009"):
        self._host = host
        self._user = user
        self._dbname = dbname
        self._passwd = passwd
        self.connection = self.connect()

    def connect(self):
        return pg.connect(host=self._host,
                user=self._user, dbname=self._dbname, passwd=self._passwd)
    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def isOpen(self):
        return self.connection != None

    def deg2rad(self,ang):
        return ang*math.pi/180.

    def rad2deg(self,ang):
        return ang*180./math.pi

    def getMovingObjectsById(self, ids):
        objects = []
        for id in ids:
            query = "select q,e,inclination,node,argperi,t_peri,epoch,h_g,phasegv,index,"
            query += "n_par,moid,objid,otype,isvar,t0,timescale,varfluxmax,sed_filename,"
            query += "u_opp, g_opp, r_opp, i_opp, z_opp, y_opp from " 
            query += "orbits where objid = %i"%(id)
            res = self.connection.query(query)
            results = res.dictresult()
            r = results[0]
            mymo = mo.MovingObject(r['q'], r['e'], r['inclination'], r['node'],
                                   r['argperi'], r['t_peri'], r['epoch'],
                                   magHv=r['h_g'], phaseGv=r['phasegv'], index=r['index'],
                                   n_par=r['n_par'], moid=r['moid'], 
                                   objid=r['objid'], objtype=r['otype'],
                                   isVar=r['isvar'], var_t0=r['t0'],
                                   var_timescale=r['timescale'], var_fluxmax=r['varfluxmax'],
                                   sedname=r['sed_filename'],
                                   u_opp=r['u_opp'],g_opp=r['g_opp'], r_opp=r['r_opp'],
                                   i_opp=r['i_opp'], z_opp=r['z_opp'], y_opp=r['y_opp'])
            objects.append(mymo)
        # done getting all the objects
        return mol.MovingObjectList(objects)

    def getMaxVelByMjd(self, mjd, poptable = 'neonightlyephem'):
        """
        This method tries to return the maximum velocity on the sky for the
        day surrounding the input mjd.
        input:
          mjd -- Modified Julian Date for the query
        output:
          velocity -- maximum vector sum of velocities in ra and dec
        """
        mjdmin = mjd-0.5
        mjdmax = mjd+0.5
        query = "select max(sqrt(pow(dra_dt,2) + pow(ddec_dt,2))) as vel from %s"%(poptable)
        query = query+" where obsdate between %f and %f"%(mjdmin, mjdmax)
        print query
        res = self.connection.query(query)
        results = res.dictresult()
        return results[0]['vel']


    def getPotentialMovingObjectsByCone(self, ra, dec, radius, obsdate,
            poptable = 'neonightlyephem'):
        ramax = ra+radius/math.cos(self.deg2rad(dec))
        ramin = ra-radius/math.cos(self.deg2rad(dec))
        if ramax > 360:
            ramax = ramax - 360
        if ramin < 0:
            ramin = ramin + 360
        decmax = dec+radius
        decmin = dec-radius
        if decmin < -90:
            decmin = -90
        if decmax > 90:
            decmax = 90
                
        mjdmax = obsdate + 0.5
        mjdmin = obsdate - 0.5


        # simple query, didn't wrap around 0
        if ramin < ramax: 
            query = "select o.* from %s n, orbits o where (decl between %f and %f) and (ra between %f and %f) and (obsdate between %f and %f) and o.objid=n.objid" %(poptable, decmin, decmax, ramin, ramax, mjdmin, mjdmax)
        # else wrapped around zero
        elif ramin >= ramax:
            query  = "select o.* from %s n, orbits o where (decl between %f and %f) and (ra>=%f or ra<=%f) and (obsdate between %f and %f) and o.objid=n.objid" %(poptable, decmin, decmax, ramin, ramax, mjdmin, mjdmax)
        res = self.connection.query(query)
        results = res.dictresult()
        #print "Got results from cosmoDB query %s" %(query)
        #print "number results: %d" %(len(results))
        #for r in results:
        #    print r
        objects = []
        for r in results:
            mymo = mo.MovingObject(r['q'], r['e'], r['inclination'], r['node'],
                                   r['argperi'], r['t_peri'], r['epoch'],
                                   magHv=r['h_g'], phaseGv=r['phasegv'], index=r['index'],
                                   n_par=r['n_par'], moid=r['moid'], 
                                   objid=r['objid'], objtype=r['otype'],
                                   isVar=r['isvar'], var_t0=r['t0'],
                                   var_timescale=r['timescale'], var_fluxmax=r['varfluxmax'],
                                   sedname=r['sed_filename'],
                                   u_opp=r['u_opp'],g_opp=r['g_opp'], r_opp=r['r_opp'],
                                   i_opp=r['i_opp'], z_opp=r['z_opp'], y_opp=r['y_opp'])
            objects.append(mymo)
           
        return mol.MovingObjectList(objects)
