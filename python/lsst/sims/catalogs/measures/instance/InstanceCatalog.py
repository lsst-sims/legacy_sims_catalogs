""" InstanceCatalog Class

    ajc@astro Feb 10 2010
    Class and methods that operate on an InstanceClass

"""

from lsst.sims.catalogs.measures.astrometry.Astrometry import *
import SiteDescription
import numpy
import warnings
import CatalogDescription
import Metadata

class CatalogType:
    """Enum types for the catalog type format"""
    INVALID = 0
    TRIM = 1
    DIASOURCE=2
    PHOTCAL=3
    SCIENCE=4
    MISC=5
    
class SourceType:
    """Enum types for the source type used to define trim catalog format"""
    INVALID = 0
    POINT = 1
    SERSEC2D = 2
    MOVING = 3
    IMAGE = 4
    ARTEFACT = 5


class InstanceCatalog (Astrometry):
    """ Class that describes the instance catalog for the simulations. 

    Instance catalogs include a dictionary of numpy arrays which contains 
    core data. Additional arrays can be appended as ancillary data are 
    derived

    Catalog types and Object types are defined in the CatalogDescription class
    catalogType = # TRIM, SCIENCE, PHOTCAL, DIASOURCE, MISC, INVALID
    objectType # Point, Moving, Sersic, Image, Artefact, MISC
    catalogTable is name of the database table queried
    dataArray dictionary of numpy arrays of data

    """

    def __init__(self):
        """Create an InstanceClass

        Instantiate an InstanceClass with the catalog type set to invalid
        """
        self.catalogDescription = None
        
        self.site = SiteDescription.SiteDescription()
        self.metadata = Metadata.Metadata()
        self.catalogType = CatalogType.INVALID
        self.objectType = ""
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
    def validateData(self, dataType):
        """Validate that the class contains the correct attributes
        
            This does not test validity of the data
            This combines searches through the schema and derived attributes"""
        if (dataType == None):
            return True
        else:
            # copy the attribute list so we dont append to the original list
            attributeList = self.catalogDescription.databaseAttributeList(dataType)[:]
            attributeList.extend(self.catalogDescription.derivedAttributeList(dataType))
            for name in attributeList:
                if ((self.dataArray.has_key(name[0]) == False)):
                    raise ValueError("Entry %s does not exist in data"%name)
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
        if (catalogType == None):
            #write all columns
            pass
        elif (catalogType == "TRIM"):
            #write trim file based on objectType
            attributeList = self.catalogDescription.formatString(self.objectType)[0][0].split(',')
            # Added a newline as shlex reads in without this information parsed
            formatString = self.catalogDescription.formatString(self.objectType)[0][1]+"\n"
            for i in range(len(self.dataArray["id"])):
                # use map to output all attributes in the given format string
                outputFile.write(formatString.format(map(lambda x: self.dataArray[x][i],attributeList)))
        else:
            #write catalog based on catalogType - format string needs to be parsed
            attributeList = self.catalogDescription.formatString(catalogType)[0][0].split(',')
            # Added a newline as shlex reads in without this information parsed
            formatString = self.catalogDescription.formatString(catalogType)[0][1]+"\n"
            # RRG:  changed self.dataArray["ra"].size to len(...)
            for i in range(len(self.dataArray["id"])):
                # use map to output all attributes in the given format string
                outputFile.write(formatString.format(map(lambda x: self.dataArray[x][i],attributeList)))
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
        if ((("raApp" in self.dataArray) and 
             ("decApp" in self.dataArray)) != True):
            self.makeApparent()
        raOut, decOut = self.applyApparentToTrim(self.dataArray['raApp'], self.dataArray['decApp'],
                                                    MJD=self.metadata.parameters['Opsim_expmjd'])

        self.addColumn(raOut, 'raTrim')
        self.addColumn(decOut, 'decTrim')



    # Photometry composite methods
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

