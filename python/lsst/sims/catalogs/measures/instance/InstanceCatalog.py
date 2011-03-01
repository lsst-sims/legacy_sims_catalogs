""" InstanceCatalog Class

    ajc@astro Feb 10 2010
    Class and methods that operate on an InstanceClass

"""

import numpy
import warnings
import sys
from copy import deepcopy

from lsst.sims.catalogs.measures.astrometry.Astrometry import *
from lsst.sims.catalogs.measures.photometry.Bandpass import *
from lsst.sims.catalogs.measures.photometry.Sed import *
#from lsst.sims.catalogs.measures.photometry.Magnitudes import *
#from lsst.sims.catalogs.measures.instance.CatalogDescription import *
from lsst.sims.catalogs.measures.instance.SiteDescription import *
from lsst.sims.catalogs.measures.instance.Metadata import *
from CatalogDescription import *

class InstanceCatalog (Astrometry):
    """ Class that describes the instance catalog for the simulations. 

    Instance catalogs include a dictionary of numpy arrays which contains 
    core data. Additional arrays can be appended as ancillary data are 
    derived

    Catalog types and Object types are defined in the CatalogDescription class
    catalogType =  TRIM, SCIENCE, PHOTCAL, DIASOURCE, MISC, INVALID
    objectType = Point, Moving, Sersic, Image, Artefact, MISC
    catalogTable is name of the database table queried
    dataArray dictionary of numpy arrays of data

    """

    def __init__(self, configFile):
        """Create an InstanceClass

        Instantiate an InstanceClass with the catalog type set to invalid
        """
        self.catalogDescription = CatalogDescription(configFile)
        self.site = SiteDescription()
        self.metadata = Metadata(configFile)

        self.catalogType = None
        self.neighborhoodType = None
        self.objectType = None
        self.catalogTable = ""
        self.dataArray = {}

    # dataArray operations    
    def addColumn(self, array, name):
        """Add a numpy array to dataArray and warn if it already exists """
        if name in self.dataArray:
            warnings.warn("Entry %s exists in dataArray" % name)
        self.dataArray[name] = array
    def deleteColumn(self, name):
        """Delete a  numpy array from dataArray """
        if self.dataArray.has_key(name):
            del self.dataArray[name] 
        else:
            warnings.warn("Entry %s does not exists in dataArray" % name)
            
    # validate that the catalog contains the correct data
    def validateData(self, catalogType):
        """Validate that the class contains the correct attributes
        
        This does not test validity of the data
            This combines searches through the required and derived attributes"""

        # validate required and derived data 
        requiredAttributeList = self.catalogDescription.getRequiredFields(catalogType,
                                                                          self.neighborhoodType,
                                                                          self.objectType)

        for name in requiredAttributeList:
            if ((self.dataArray.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in required data"%name)

        derivedAttributeList = self.catalogDescription.getDerivedFields(catalogType,
                                                                          self.neighborhoodType,
                                                                          self.objectType)
        for name in derivedAttributeList:
            if ((self.dataArray.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in derived data" % name)

        return True
                
    # Output of formatted data catalogs
    def writeCatalogData(self, filename, catalogType, newfile = False):
        """Write an instanceCatalog dataArray based on the catalog type given

           If the catalogType is TRIM use the objectType to determine the output format
           Provide the option to clobber the file
        """
        # open file
        if (newfile == False):
            outputFile = open(filename,"a")
        else:
            outputFile = open(filename,"w")

        # Determine the catalogType and objectType for printing
        format, attributeList = self.catalogDescription.getFormat(catalogType, self.objectType)

        #manipulate sedFilename to add path for file on disk
        sedPaths = self.catalogDescription.getPathMap()
        self.addColumn(map(lambda x: sedPaths[x], self.dataArray['sedFilename']),'sedFilename')

        # generate degrees column for ra and dec
        self.addColumn(self.dataArray['raTrim']*180./math.pi,'raTrim_deg')
        self.addColumn(self.dataArray['decTrim']*180./math.pi,'decTrim_deg')



        #write trim file based on objectType
        # add newline to format string - configobj does not interpret these correctly
        format = format +"\n"
        for i in range(len(self.dataArray["id"])):
            # use map to output all attributes in the given format string
            # 2.6 outputFile.write(formatString,(map(lambda x: self.dataArray[x][i],attributeList)))
            outputFile.write(format % tuple(map(lambda x: self.dataArray[x][i],attributeList)))
        outputFile.close()

    # Composite astrometry operations
    def makeHelio(self):
        """ Generate Heliocentric coordinates """

        # apply precession
        raOut, decOut = self.applyPrecession(self.dataArray['ra'], self.dataArray['dec'],
                                                   MJD = self.metadata.parameters['Opsim_expmjd'])

        # apply proper motion
        raOut, decOut = self.applyProperMotion(raOut, decOut, self.dataArray['properMotionRa'],
                                               self.dataArray['properMotionDec'], self.dataArray['parallax'],
                                               self.dataArray['radialVelocity'], MJD = self.metadata.parameters['Opsim_expmjd'])

        # TODO 3/29/2010 convert FK5 to ICRS?
        self.addColumn(raOut, 'raHelio')
        self.addColumn(decOut, 'decHelio')

    def makeApparent(self):
        """ Generate apparent coordinates
        

        This converts from the J2000 coordinates to the position as
        viewed from the center of the Earth and includes the effects
        of light defection (ignored), annual aberration, precession
        and nutation
        """
        raOut, decOut = self.applyMeanApparentPlace(self.dataArray['ra'], self.dataArray['dec'],
                                                    self.dataArray['properMotionRa'],
                                                    self.dataArray['properMotionDec'], self.dataArray['parallax'],
                                                    self.dataArray['radialVelocity'],
                                                    MJD=self.metadata.parameters['Opsim_expmjd'])

        self.addColumn(raOut, 'raApp')
        self.addColumn(decOut, 'decApp')


    def makeObserved(self):
        """ Generate Observed coordinates

        From the observed coordinates generate the position of the
        source as observed from the telescope site. This includes the
        hour angle, diurnal aberration, alt-az, and refraction. 
        """
        if ((("raApp" in self.dataArray) and 
             ("decApp" in self.dataArray)) != True):
            self.makeApparent()
        raOut, decOut = self.applyMeanObservedPlace(self.dataArray['raApp'], self.dataArray['decApp'],
                                                    MJD=self.metadata.parameters['Opsim_expmjd'])

        self.addColumn(raOut, 'raObs')
        self.addColumn(decOut, 'decObs')

    def makeTrimCoords(self):
        """ Generate TRIM coordinates

        From the apparent coordinates generate the position of the
        source as observed from the telescope site (required for the
        trim files). This includes the hour angle, diurnal aberration,
        alt-az. This does NOT include refraction.
        """
        #Calculate pointing of telescope in observed frame and the rotation matrix to transform to this position
        raCenter, decCenter = self.transformPointingToObserved(
            self.metadata.parameters['Unrefracted_RA'],
            self.metadata.parameters['Unrefracted_Dec'])

        xyzJ2000 = self.sphericalToCartesian(self.metadata.parameters['Unrefracted_RA'],
                                           self.metadata.parameters['Unrefracted_Dec'])
        xyzJ2000 /= math.sqrt(numpy.dot(xyzJ2000, xyzJ2000))

        xyzObs = self.sphericalToCartesian(raCenter, decCenter)
        xyzObs /= math.sqrt(numpy.dot(xyzObs, xyzObs))

        rotationMatrix = self.rotationMatrixFromVectors(xyzObs, xyzJ2000)

        #convert positions of sources from reference to observed
        if ((("raApp" in self.dataArray) and 
             ("decApp" in self.dataArray)) != True):
            self.makeApparent()
            raOut, decOut = self.applyApparentToTrim(self.dataArray['raApp'],
                                                     self.dataArray['decApp'],
                                                     MJD=self.metadata.parameters['Opsim_expmjd'])


        # correct for pointing of the telescope (so only have differential offsets)
        # and set output as ICRS
        xyz = self.sphericalToCartesian(raOut, decOut).transpose()
        for i,_xyz in enumerate(xyz):
            xyzNew = numpy.dot(rotationMatrix,_xyz)
            raOut[i], decOut[i] = self.cartesianToSpherical(xyzNew)

        self.addColumn(raOut, 'raTrim')
        self.addColumn(decOut, 'decTrim')

    def transformPointingToObserved(self, ra, dec):
        """Take an LSST central pointing and determine is observed position on the sky
        (excluding refraction)"""
        raOut, decOut = self.applyMeanApparentPlace([ra], [dec], [0.], [0.],[0.], [0.],
                                                    MJD=self.metadata.parameters['Opsim_expmjd'])
        print ra,dec,raOut, decOut,self.metadata.parameters['Opsim_expmjd']
        raObs, decObs = self.applyApparentToTrim(raOut, decOut,
                                                    MJD=self.metadata.parameters['Opsim_expmjd'])
        return raObs[0], decObs[0]


    # Photometry composite methods
    def calculateStellarMagnitudes(self, bandpassList, dataDir = "./"):
        """For stellar sources and a list of bandpass names generate magnitudes"""
    
        # load bandpasses
        bandpassDict = loadBandpasses(bandpassList, dataDir = dataDir)
        
        #load required SEDs
        sedDict = loadSeds(self.dataArray["sedFilename"], dataDir=dataDir)

        # Calculate dust parameters for all stars 
        a_x, b_x = sedDict[self.dataArray["sedFilename"][0]].setupCCMab(wavelen=sedDict[
            self.dataArray["sedFilename"][0]].wavelen)

        # generate magnitude arrays and set to zero
        for name,bandpass in bandpassDict.items():
            self.addColumn(numpy.zeros(len(self.dataArray["id"])), name)

        # loop through magnitudes and calculate   
        for i,sedName in enumerate(self.dataArray["sedFilename"]):
            sed = deepcopy(sedDict[sedName])
            sed.multiplyFluxNorm(self.dataArray["fluxNorm"][i]*1.e-22)
            sed.addCCMDust(a_x, b_x, A_v=self.dataArray["galacticAv"][i], R_v=self.dataArray["galacticRv"][i])
            for name,bandpass in bandpassDict.items():
                self.dataArray[name][i] = sed.calcMag(bandpass)
            del sed

    def calculateGalaxyMagnitudes(self, bandpassList, dataDir = "./"):
        """For stellar sources and a list of bandpass names generate magnitudes"""
    
        # load bandpasses
        bandpassDict = loadBandpasses(bandpassList, dataDir = dataDir)
        
        #load required SEDs
        sedDict = loadSeds(self.dataArray["sedFilename"], dataDir=dataDir)

        # generate magnitude arrays and set to zero
        for name,bandpass in bandpassDict.items():
            self.addColumn(numpy.zeros(len(self.dataArray["id"])), name)

        # loop through magnitudes and calculate   
        for i,sedName in enumerate(self.dataArray["sedFilename"]):
            sed = deepcopy(sedDict[sedName])
            # Calculate dust parameters for all galaxies
            a_x, b_x = sedDict[self.dataArray["sedFilename"][0]].setupCCMab(wavelen=sedDict[
                self.dataArray["sedFilename"][0]].wavelen)
            sed.addCCMDust(a_x, b_x, A_v=self.dataArray["internalAv"][i], R_v=self.dataArray["internalRv"][i])

            sed.multiplyFluxNorm(10**((self.dataArray["magNorm"][i] + 8.9)/-2.5))

            sed.redshiftSED(redshift=self.dataArray["redshift"][i], dimming=False)

            # add dust from our galaxy            
            a_x, b_x = sed.setupCCMab()
            # apply dust
            sed.addCCMDust(a_x, b_x, A_v=self.dataArray["galacticAv"][i], R_v=self.dataArray["galacticRv"][i])
            for name,bandpass in bandpassDict.items():
                self.dataArray[name][i] = sed.calcMag(bandpass)
            del sed

""" TODO (2/18/2010) incorporate the precession routines
    def makeMeasured(self):
        raOut, decOut = self.applyPropermotion(self.dataArray['ra'], 
                                    self.dataArray['dec'])
        raOut, decOut = self.applyParallax(raOut, decOut)
        self.addColumn(raOut, 'raMeasured')
        self.addColumn(decOut, 'decMeasured')
    def makeGeo(self):
        if ((("raHelio" in self.dataArray) and 
             ("decHelio" in self.dataArray)) != True):
            self.makeHelio()
        raOut, decOut = self.applyParallax()
        raOut, decOut = self.applyAberration()
        self.addColumn(raOut, 'raGeo')
        self.addColumn(decOut, 'decGeo')
    def makeTopo(self):
        if ((("raGeo" in self.dataArray) and 
             ("decGeo" in self.dataArray)) != True):
            self.makeGeo()
        raOut, decOut = self.applyAbsoluteRefraction(raPar, decPar)
        self.addColumn(raOut, 'raHTopo')
        self.addColumn(decOut, 'decTopo')
     """



