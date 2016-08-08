""" 

ljones, jmyers
$Id$

This class holds and uses lists of MovingObjects, letting you do functions which are better called on
an aggregated bunch of MovingObjects than on the independent MovingObjects themselves. 
 * generateEphemeridesForAllObjects
 * getMovingObjectsInFieldofView
 * calcAllMags
 * cut on SNR 
 * output as a list of lists

This uses pyoorb, which needs to be EXPLICITLY INITIALIZED with a call to 
pyoorb.pyoorb.oorb_init()
this must be done EXACTLY ONCE through the life of the process;
whether this is to be done eventually in an __init__ file or by the
end user explicitly in a __main__ is not yet decided.

"""

import warnings as warning
import numpy as n
import pyoorb as oo
import movingObject as mo


class MovingObjectList(object):

    """
    A utility class for holding a list of MovingObjects.

    This is useful because we would like to often get ephemerides for
    the same time, for a set of orbits; this class is computationally
    efficient because this way we only need to make one call to
    Fortran-land, handing OOrb a large array of orbits.

    """
    def __init__(self, objectList=None):
        if objectList == None:
            self._mObjects = None
        else:
            self.setList(objectList)
        return

    def setList(self, mObjects):
        """
        mObjects should be a list, where the objects in the list are MovingObjects.
        """
        self._verifyIsMovingObjectList(mObjects)
        # mObjects is a 'normal' square bracket list
        self._mObjects = mObjects
        return

    def addMovingObjectList(self, new_mObjList):
        """
        new_mObjects should be a MovingObjectList, with a _mObjects property
        """
        self._verifyIsMovingObjectList(new_mObjList._mObjects)
        for movingobj in new_mObjList._mObjects:
            self._mObjects.append(movingobj)
        return

    def countList(self):
        """
        Debugging help
        """
        count = len(self._mObjects)
        return count

    def getList(self):
        return copy(self._mObjects)


    def _verifyIsMovingObjectList(self, objectList):
        """ Just a checker to see if you've handed in a proper list of movingObjects"""
        errStr = "movingObjectList constructor and movingObjectList.setList " + \
        "each take a list of movingObjects as their only argument."
        if not isinstance(objectList, list):
            raise TypeException(errStr)
        for e in objectList:
            if not (isinstance(e, mo.MovingObject)):
                raise TypeException(errStr)
    

    def generateEphemeridesForAllObjects(self, mjdTaiList, obscode=807):
        """ generates ephemerides for all sources in the source list
        at the specified MJD(s) in mjdTaiList and obscode.  
        ephemerides are stored in ephemeride dictionary associated with each object.
        currently, all dates are ASSUMED to be MJD_TAI  (need to change 'timescale' if not)
        """
        # convert float mjdTai's into strings for dictionary lookup
        movingobj = self._mObjects[0]
        mjdTaiListStr = []
        if isinstance(mjdTaiList, list) == False:
            mjdTaiList = [mjdTaiList]
        for mjdTai in mjdTaiList:
            mjdTaiListStr.append(movingobj.mjdTaiStr(mjdTai))
            # mjdTaiListStr will be the same for all objects

        # set up array to hold orbital information from all moving objects
        orbitsArray = n.empty([len(self._mObjects), 12], dtype=n.double, order='F')
        # loop through objects to populate orbitsArray for ephemeris generation
        for i in range(len(self._mObjects)):
            movingobj = self._mObjects[i]
            objid = movingobj.getobjid()
            elements = movingobj.Orbit
            #set up an array for pyoorb. it takes:
            # 0: orbitId
            # 1 - 6: orbital elements, using radians for angles
            # 7: element type code, where 2 = cometary - means timescale is TT, too
            # 8: epoch
            # 9: timescale for the epoch; 1= MJD_UTC, 2=UT1, 3=TT, 4=TAI
            # 10: H
            # 11: G
            orbitsArray[i][:] = [movingobj.getobjid(),
                                 elements.getq(), 
                                 elements.gete(),
                                 n.radians(elements.geti()),
                                 n.radians(elements.getnode()),
                                 n.radians(elements.getargPeri()),
                                 elements.gettimePeri(),
                                 n.double(2),
                                 elements.getepoch(),
                                 elements.getorb_timescale(),
                                 movingobj.getmagHv(),
                                 movingobj.getphaseGv()]
        # we don't need a covariance matrix for ssm objects (=0) 
        # and the oorb_ephemeris call changes if a covariance exists (to oorb_ephemeris_covariance)
        # set up array to hold ephemeris date information for all moving objects
        ephem_dates = n.zeros([len(mjdTaiList),2], dtype=n.double, order='F')
        for i in range(len(mjdTaiList)):
            ephem_dates[i][:] = [mjdTaiList[i], 4.0]
        # timescale; 1 = UTC. 2 = UT1.  3= TT. 4 = TAI 

        # now do ephemeris generation for all objects on all dates
        ephem_datfile = ""
        oo.pyoorb.oorb_init(ephemeris_fname=ephem_datfile)

        ephems, err = oo.pyoorb.oorb_ephemeris(in_orbits = orbitsArray,
                                               in_obscode = obscode,
                                               in_date_ephems = ephem_dates)
        
        if (err != 0):
            raise Exception("pyoorb.oorb_ephemeris encountered an error")
        
        # ephems contains a 3-D Fortran array of ephemerides, the ephemerides are:
        # distance, ra, dec, mag, ephem mjd, ephem mjd timescale, dradt(sky), ddecdt(sky)
        # per object, per date, 8 elements (shape is OBJ(s)/DATE(s)/VALUES)
        # now go back and assign data to individual moving objects (using the string to put back in dictionary)
        for i in range(len(self._mObjects)):
            for j in range(len(mjdTaiList)):
                eph = ephems[i][j]
                dradt = eph[6]  #/n.cos(n.radians(eph[2]))  -- sky motion (including cos(dec) is coord motion)
                ddecdt = eph[7]                 
                self._mObjects[i].Ephemerides[mjdTaiListStr[j]] = mo.Ephemeris(mjdTai=mjdTaiList[j],
                                                                               ra=eph[1], dec=eph[2],
                                                                               magV=eph[3],
                                                                               distance=eph[0],
                                                                               dradt=dradt, 
                                                                               ddecdt=ddecdt)
        # done calculating ephemerides. ephemerides stored in dictionary with movingObjects
        return

    def getMovingObjectsInFieldofView(self, ra_fov, dec_fov, radius_fov, mjdTai):
        """ Return MovingObjectList of objects within field of view """        
        """  given field of view info in degrees """
        # this method is only applicable to ONE time and one field of view at a time
        outputList = []        
        for movingobj in self._mObjects:
            # check if an ephemeris exists for this night - calculate if necessary 
            # avoid this warning if possible - method to calc all ephemerides at once much faster
            try: 
                movingobj.Ephemerides[movingobj.mjdTaiStr(mjdTai)]
            except AttributeError:
                warning.warn('moving object does not have ephemeris on this date')            
                movingobj.calcEphemeris(mjdTai) 
            # check if moving object was in the field of view of exposure
            if movingobj.Ephemerides[movingobj.mjdTaiStr(mjdTai)].isInFieldofView(ra_fov, dec_fov, radius_fov): 
                # then object was in the field of view
                outputList.append(movingobj)        
        return MovingObjectList(outputList)

    def calcAllMags(self, filt, mjdTaiList, rootSEDdir, rootFILTERdir=None, withErrors=True, fiveSigmaLimitingMag=None):
        """ Calculate the magnitude of all objects in the movingObjectList. 

        Give the filter, the mjd (to find the ephemeris info) and the root directory of SEDS. Calculates magnitudes.
        Individual objects already know their SED and their expected V magnitude (part of ephemeris info). """
        # Set up data needed to calculate magnitudes for each moving object.
        # First find location of lsst filter throughput curves, if not specified.
        import os
        if rootFILTERdir == None:
            # (assumes throughputs is setup)
            # Just use the default directory of the throughputs as this is probably correct.
            rootFILTERdir = os.getenv("LSST_THROUGHPUTS_DEFAULT")
        # Now rootFILTERdir should be defined. 
        if rootFILTERdir == None:
            raise Exception("Ack: rootFILTERdir is undefined and it seems that 'throughputs' is not setup.")
        # Import Sed and Bandpass for calculating magnitudes. (assumes catalogs_measures is setup)
        import lsst.sims.catalogs.measures.photometry.Sed as Sed
        import lsst.sims.catalogs.measures.photometry.Bandpass as Bandpass
        # read in and set up filter files
        bandpass = {}
        filterlist = ('V', 'imsim', filt)
        for f in filterlist:
            if f == 'V':
                filename = os.path.join(rootSEDdir,'harris_V.dat')
                # Instantiate and read the throughput file. 
                bandpass[f] = Bandpass()
                bandpass[f].readThroughput(filename)
            elif f=='imsim':
                filename = 'imsim'
                bandpass[f] = Bandpass()
                bandpass[f].imsimBandpass()
            else:
                filename = os.path.join(rootFILTERdir,'total_' + f + '.dat')
                # Instantiate and read the throughput file. 
                bandpass[f] = Bandpass()
                bandpass[f].readThroughput(filename)
        # Read in and set up sed files. Assumes asteroid SEDS end in .dat
        possiblesedtypes = os.listdir(rootSEDdir)
        sedtypes = []
        for p in possiblesedtypes:
            if p.endswith('.dat'):
                sedtypes.append(p)
        sed={}
        sedmag = {}
        sedcol = {}
        for sedfile in sedtypes:
            # read sed files
            filename = os.path.join(rootSEDdir, sedfile)
            sed[sedfile] = Sed()
            sed[sedfile].readSED_flambda(filename)
            # set up magnitudes for color calculation.
            sedmag[sedfile] = {}
            sedcol[sedfile] = {}
            for f in filterlist:
               sedmag[sedfile][f] = sed[sedfile].calcMag(bandpass[f])
               sedcol[sedfile][f] = sedmag[sedfile][f] - sedmag[sedfile]['V']
        # set up mjdTaiListStr for access to ephemeris dictionaries
        movingobj = self._mObjects[0]
        mjdTaiListStr = []
        if isinstance(mjdTaiList, list) == False:
            mjdTaiList = [mjdTaiList]
        for mjdTai in mjdTaiList:
            mjdTaiListStr.append(movingobj.mjdTaiStr(mjdTai))
            # mjdTaiListStr will be the same for all objects.

        # now loop through each object and assign appropriate magnitudes values for this observation
        for movingobj in self._mObjects:
            # loop through mjdTaiList
            for mjdTaiStr in mjdTaiListStr:
                try:  # check ephemerides exist
                    movingobj.Ephemerides[mjdTaiStr]
                except AttributeError:
                    raise Exception("Do not have an ephemeride on date %s" %(mjdTaiStr))
                vmag = movingobj.Ephemerides[mjdTaiStr].getmagV()
                sedname = movingobj.getsedname()
                # Check if sedname in list of known seds.
                if sed.has_key(sedname)==False:
                    # HACK  - FIX when catalogs updated
                    sedname = 'S.dat'
                    #raise Exception("SED (%s) of moving object (#%d) not in movingObjectList's directory."
                    #                %(sedname, movingobj.getobjid()))
                # calculate magnitudes
                filtmag = vmag + sedcol[sedname][filt]
                imsimmag = vmag + sedcol[sedname]['imsim']
                # set filter magnitude in ephemeris 
                movingobj.Ephemerides[mjdTaiStr].setmagFilter(filtmag)  
                movingobj.Ephemerides[mjdTaiStr].setmagImsim(imsimmag)
                # Add SNR measurement if given 5-sigma limiting mag for exposure.
                if fiveSigmaLimitingMag != None:
                    flux_ratio = n.power(10, 0.4*(fiveSigmaLimitingMag - filtmag))
                    snr = 5 * (flux_ratio)
                    movingobj.Ephemerides[mjdTaiStr].setsnr(snr)                    
                # Calculate approx errors in ra/dec/mag from magnitude/m5.
                if withErrors:
                    if fiveSigmaLimitingMag == None:
                        raise Exception("To calculate errors, fiveSigmaLimitingMag is needed.")
                    # calculate error in ra/dec
                    rgamma = 0.039
                    # average seeing is 0.7" (or 700 mas)
                    error_rand = n.sqrt((0.04-rgamma)*flux_ratio + rgamma*flux_ratio*flux_ratio)
                    ast_error_rand = 700.0 * error_rand
                    ast_error_sys = 10.0
                    astrom_error = n.sqrt(ast_error_sys**2 + ast_error_rand**2)
                    # convert from mas to deg
                    astrom_error = astrom_error / 100.0 / 60.0/ 60.0
                    movingobj.Ephemerides[mjdTaiStr].setastErr(astrom_error)
                    mag_error_sys = 0.005
                    mag_error = n.sqrt(error_rand**2 + mag_error_sys**2)
                    movingobj.Ephemerides[mjdTaiStr].setmagErr(mag_error)
            # end of mjdTaiList loop
        return

    def cutAllSNR(self, fiveSigmaLimitingMag, SNRcutoff, mjdTai):
        """calculate SNR for each object and create new moving object list of objects above the SNR cutoff """
        """ Given five sigma limiting mag for image and SNR cutoff """
        # This method is ONLY applicable to ONE time and ONE five sigma limiting magnitude at once
        outputList = []
        # Uses SNR generated in calcAllMags when used with error generation. 
        # Check that ephemerides exist and magnitudes/snr calculated on this date
        movingobj = self._mObjects[0]
        mjdTaiStr = movingobj.mjdTaiStr(mjdTai)
        try:
            movingobj.Ephemerides[mjdTaiStr]
        except AttributeError:
            raise Exception("Need to set up ephemerides and magnitudes for this date (%f) first" %(mjdTai))
        try: 
            movingobj.Ephemerides[mjdTaiStr].getsnr()
        except AttributeError:
            raise Exception("Have an ephemeride for this date (%f), but no SNR yet - calcAllMags first with a 5-sigma limiting magnitude value" %(mjdTai))
        for movingobj in self._mObjects:
            if (movingobj.Ephemerides[mjdTaiStr].getsnr() > SNRcutoff):
                # then object was above the SNR cutoff
                outputList.append(movingobj)
        return MovingObjectList(outputList)

    def printList(self, mjdTaiList):
        """ Simple print of all the parameters associated with a particular object """
        output = ['objid', 'mjdTai', 'ra', 'dec', 'dradt', 'ddecdt', 'distance', 'magImsim', 'magFilter', 'filter']

        # set up mjdTaiListStr for access to ephemeris dictionaries
        movingobj = self._mObjects[0]
        mjdTaiListStr = []
        if isinstance(mjdTaiList, list) == False:
            mjdTaiList = [mjdTaiList]
        for mjdTai in mjdTaiList:
            mjdTaiListStr.append(movingobj.mjdTaiStr(mjdTai))
            # mjdTaiListStr will be the same for all objects

        for mjdTaiStr in mjdTaiListStr:
            for movingobj in self._mObjects:
                ephem = movingobj.Ephemerides[mjdTaiStr]
                print "%d %f %f %f %f %f %f %f %f %s" %(movingobj.getobjid(), 
                                                        ephem.getra(), ephem.getdec(),
                                                        ephem.getdradt(), ephem.getddecdt(),
                                                        ephem.getdistance(), 
                                                        ephem.getmagImsim(), ephem.getmagFilter(),
                                                        ephem.getfilter())
        return

"""
    def makeListOutput_imsim(self, mjdTai):
        # want to send out : 
        #  objid  RA(deg)  Dec(deg)  
        #  imsimMag  SED_filename   
        #  dRA/dt(deg/day)  dDec/dt (deg/day)  
        descriptionList = [ 'objid', 'ra', 'decl', 'distance',
                            'flux_scale', 'sedname',
                            'dradt', 'ddecdt']

        # set up mjdTaiListStr for access to ephemeris dictionaries
        movingobj = self._mObjects[0]
        mjdTaiStr = movingobj.mjdTaiStr(mjdTai)

        outList = []
        for movingobj in self._mObjects:
            ephem = movingobj.Ephemerides[mjdTaiStr]
            outLine = [movingobj.getobjid(),
                       ephem.getra(), 
                       ephem.getdec(),
                       ephem.getdistance(),
                       ephem.getmagImsim(),
                       movingobj.getsedname(),
                       ephem.getdradt(),
                       ephem.getddecdt()]
            outList.append(outLine)
        return outList, descriptionList


    def makeListOutput(self, mjdTai):
        # want to send out : 
        #  objid  RA(deg)  Dec(deg)  
        #  imsimMag  SED_filename   
        #  dRA/dt(deg/day)  dDec/dt (deg/day)  
        descriptionList = [ 'objid',
                            'ra', 'ra_err', 'dradt',
                            'dec', 'dec_err', 'ddecdt',
                            'distance', 
                            'magFilter', 'magErr',
                            'magImsim', 'flux_scale', 'sedname']
        
        outList = []
        if self.countList() == 0:
            warning.warn('No moving objects in this MovingObjectList.')
            return outList, descriptionList
        # set up mjdTaiListStr for access to ephemeris dictionaries
        movingobj = self._mObjects[0]
        mjdTaiStr = movingobj.mjdTaiStr(mjdTai)
        for movingobj in self._mObjects:
            ephem = movingobj.Ephemerides[mjdTaiStr]
            outLine = [movingobj.getobjid(),
                       ephem.getra(),
                       ephem.getastErr(),
                       ephem.getdradt(),
                       ephem.getdec(),
                       ephem.getastErr(),
                       ephem.getddecdt(),
                       ephem.getdistance(),
                       ephem.getmagFilter(),
                       ephem.getmagErr(),
                       ephem.getmagImsim(),
                       ephem.getfluxnorm(),
                       movingobj.getsedname()]
            outList.append(outLine)
        return outList, descriptionList
"""
