"""
$Id$

ljones, jmyers


This contains the main functions to be used for generating catalogs of moving
object sources. 
Builds on movingObject (a class which contains information on a single moving object, 
   including its orbit at an epoch and potentially multiple ephemerides) 
  and movingObjectList (a class which operates on groups of movingObjects, primarily
   intended to make ephemeris generation faster for many objects at the same ephem_date [shortcut calls to fortran],
   as well as make calculating magnitudes faster for many objects). 

Call buildMovingObjectCatalog with an RA(deg)/Dec(deg) of pointing, radius of the field of view(deg),
 and MJD-TAI time of the exposure. Will produce two ephemerides, separated by 
 dtExp(in seconds) .. the time between exposure 1 and exposure 2
Returns list of lists of objects, plus description of what is in each object

Call fixMovingObjectCatalog with an RA(deg)/Dec(deg) of pointing, radius of the field of view(deg),
 and MJD-TAI time of the exposure, AND the filename containing a list of object id's that are expected in the fov.
 Will produce two ephemerides, separated by dtExp(seconds). 
Returns list of lists of objects, plus description of what is in each object
"""

import warnings as warning
import cosmoDB as cosmo
import movingObject as mo
import movingObjectList as mol
import pyoorb as oo
import math

import time
def dtime(time_prev):
    return (time.time() - time_prev, time.time())


####################
##### buildMovingObjectCatalog --- build list of moving objects from field of view and time information

def buildMovingObjectCatalog(ra_fov, dec_fov, radius_fov, mjdTai, filt,
                             fiveSigmaLimitingMag, dtExp=17.0, obscode=807,
                             SNRcutoff=5, verbose=False, startup_pyoorb=True,
                             rootSEDdir='/Users/ljones/work/code/lsstpython/movingObjects/dat'):
    """
    input: a set of parameters for a telescope pointing, and a
    database containing ephemerides of possible objects which may be
    observed on that night.
    
    output: a list of movingObjects, each holding a single ephemeris
    in its ephemeris dictionary.  those ephemerides are expected to
    be visible in the image, with magnitude information set up
    correctly.
    """              
    # ## currently set up for diasources! 

    # sanity check input ra/dec/radius
    if ra_fov > 360.:
        ra = ra % 360.
        warning.warn("Input RA was greater than 360; wrapped to %f" %(ra))
    if ra_fov< 0.:
        while ra<0.:
            ra = ra + 360.
            warning.warn("Input RA was less than 0; wrapped to %f" %(ra))
    if dec_fov < -90.:
        raise Exception, "Input Dec was less than -90; what did you mean by %f??" %(dec)
    if dec_fov > 90.:
        raise Exception, "Input Dec was greater than 90; what did you mean by %f??" %(dec)
    if radius_fov > 180.:
        raise Exception, "Input radius of field of view was greater than 180 - was %f" %(radius)

    if verbose:
        t = time.time()

    
    # startup = True -- need to initialize pyoorb with .dat file
    if startup_pyoorb:
        # set up pyoorb - have to do this once per 'runtime' - DO NOT REMOVE
        # de405.dat found from your environment variables, unless changed here
        # this should return 0 if no errors
        ephem_datfile = ""
        oo.pyoorb.oorb_init(ephemeris_fname=ephem_datfile)
                            

    ## debugging output
    if verbose:
        dt, t = dtime(t)
        print "Using ra=%f(deg), dec=%f(deg), field of view radius = %f(deg), exposure MJD=%f, \n filter=%s, limitingMag=%f, SNR cutoff=%f, observatory code=%f, \n and the root directory for filters and SED data files is %s)" %(ra_fov, dec_fov, radius_fov, mjdTai, filt, fiveSigmaLimitingMag, SNRcutoff, obscode, rootSEDdir)
        print "Initialized pyoorb, in %f s" %(dt)
    ## debugging output      


    # initial cosmoDB, connect to database (uses postgres sql module)
    cosmodb = cosmo.cosmoDB()
    if verbose:
        print "Connected to cosmo DB"

    # list of cosmoDB ephemeride tables (split ephemerides because very large tables!)
    ephemtables = ('neonightlyephem','othernightlyephem','mba1nightlyephem','mba2nightlyephem','mba3nightlyephem','mba4nightlyephem','mba5nightlyephem','mba6nightlyephem','mba7nightlyephem','mba8nightlyephem','mba9nightlyephem') #,'mba10nightlyephem')    
    #ephemtables = ('mba1nightlyephem', )
    newlist = True
    for ephempop in ephemtables:
        # potentialSources = MovingObjectList, of all things that may be in field of view
        # find the fastest velocity of this population on this date
        maxvel = cosmodb.getMaxVelByMjd(mjdTai, poptable=ephempop)
        # calculate the time between this exposure and midnight
        deltatime = mjdTai - math.floor(mjdTai)
        if deltatime>0.5:  #dealing with time just before midnight instead of just after
            deltatime = mjdTai - math.ceil(mjdTai)
            # calculate buffer for potentialmovingobjects query of nightly ephemerides database
        queryrad = radius_fov + maxvel * abs(deltatime)
        print queryrad
        # do the query
        if newlist:
            potentialMovingObjects = cosmodb.getPotentialMovingObjectsByCone(ra_fov, dec_fov, queryrad,
                                                                             obsdate=mjdTai, poptable=ephempop)
            newlist = False
        else:
            # do the query, add objects to moving object list
            potentialMovingObjects.addMovingObjectList(cosmodb.getPotentialMovingObjectsByCone(ra_fov, dec_fov,
                                                                                               queryrad,
                                                                                               obsdate=mjdTai,
                                                                                               poptable=ephempop))
        ## debugging output
        if verbose:
            print "Retrieved moving objects from cosmo DB, ephemeride table %s" %(ephempop)
            print "Now have %d objects in movingobjectlist" %(potentialMovingObjects.countList())
            dt, t = dtime(t)
            print "Time to query %s ephemeride table : %f s" %(ephempop, dt)
        ## debugging output

    # although only passed in one mjdTai (of visit) actually want to generate catalogs for both exposures in visit
    mjdTai_2 = mjdTai + (dtExp/60.0/60.0/24.0)
    mjdTaiList = [mjdTai, mjdTai_2]
    
    # generate updated ephemerides for all objects at this time, for both exposures
    #  (each individual moving object can store >1 ephemeride)
    potentialMovingObjects.generateEphemeridesForAllObjects(mjdTaiList, obscode)
    # now all objects in potentialSources contains two ephemerides in its ephemerides dict.
    ## debugging output
    if verbose:
        print "Generated updated ephemerides for all moving objects, for both exposures in visit"
        dt, t = dtime(t)
        print "Time to generate ephemerides: %f s" % (dt)
    ## debugging output

    # process potential moving objects list to see which objects are really in the field of view
    # note that since this could be different for each exposure, we now have to split the movingobjectLists
    fovMovingObjects= {}
    for mjdTai in mjdTaiList:
        fovMovingObjects[mjdTai] = potentialMovingObjects.getMovingObjectsInFieldofView(ra_fov, dec_fov,
                                                                                        radius_fov, mjdTai)
        ## debugging output       
        if verbose:
            print "Narrowed list to objects within field of view at time %f" %(mjdTai)
            dt, t = dtime(t)
            print "Time to remove objects from outside field of view: %f s" %(dt)
            print "Now have %d objects in movingobjectlist for this time" %(fovMovingObjects[mjdTai].countList())
        ## debugging output       
        
    # free up that potentialMovingObject memory:
    potentialMovingObjects = None


    # continue processing each exposure's worth of moving object list
    # calculate magnitudes 
    for mjdTai in mjdTaiList:
        fovMovingObjects[mjdTai].calcAllMags(filt, mjdTai, rootSEDdir, withErrors=True,
                                             fiveSigmaLimitingMag = fiveSigmaLimitingMag) 
    ## debugging output       
    if verbose:
        dt, t = dtime(t)
        print "Time to calculate magnitudes for all objects: %f s" %(dt)
        print "Calculated magnitudes (in filter) and fluxnorms for all objects in field of view"
    ## debugging output       

    # calculate SNR of objects within field of view; cut on some SNR if appropriate
    finalMovingObjects = {}
    if SNRcutoff>0:
        for mjdTai in mjdTaiList:
            finalMovingObjects[mjdTai] = fovMovingObjects[mjdTai].cutAllSNR(fiveSigmaLimitingMag, SNRcutoff, mjdTai)
            # free up that fovMovingObjects memory
            fovMovingObjects[mjdTai] = None
            ## debugging output       
            if verbose:
                print "Culled list using SNRcutoff %f" %(SNRcutoff)
                dt, t = dtime(t)
                print "Time to cull list for this time, all objects: %f s" %(dt)
                print "Now have %d objects in movingobjectlist at %f" %(finalMovingObjects[mjdTai].countList(), mjdTai)
            ## debugging output       
    else:
        for mjdTai in mjdTaiList:
            finalMovingObjects[mjdTai] = fovMovingObjects[mjdTai]
            ## debugging output       
            if verbose:
                print "SNRcutoff not used"
                print "Still have %d objects in movingobjectlist at %f" %(finalMovingObjects[mjdTai].countList(),
                                                                          mjdTai)
            ## debugging output       

    # and we're done - send back the moving object list for this exposure
    if verbose:
        print "Returning %d moving object lists, in dictionary." %(len(finalMovingObjects.keys()))
    return finalMovingObjects, mjdTaiList
