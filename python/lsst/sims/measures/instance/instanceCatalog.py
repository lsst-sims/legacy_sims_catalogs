""" InstanceCatalog Class

    ajc@astro Feb 10 2010
    Class and methods that operate on an InstanceClass

"""

#import astrometry
#import magnitudes
#import instrument
#import site
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



class InstanceCatalog (object):
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
        self.catalogDescription = CatalogDescription.CatalogDescription()
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
        """ Validate that the class contains the correct attributes
        
            This does not test validity of the data"""
        if (dataType == None):
            return True
        else:
            attributeList = self.catalogDescription.dataAttributeList(dataType)
            for name in attributeList:
#                if ((self.dataArray.has_key(name) == False)):
                if ((self.dataArray.has_key(name) == False)):
                    raise ValueError("Entry %s does not exist in data"%name)
        return True
                
                
    # write formatted data catalogs
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
            formatString = self.catalogDescription.objectFormatString(self.objectType)
            attributeList = self.catalogDescription.dataAttributeList(self.objectType)
            print attributeList
            for i in range(self.dataArray["ra"].size):
#                print map(lambda x: self.dataArray[x][i],attributeList)
                outputFile.write(formatString.format(map(lambda x: self.dataArray[x][i],attributeList)))
        else:
            #write catalog based on catalogType
            formatString = self.catalogDescription.dataFormatString(catalogType)
            attributeList = self.catalogDescription.dataAttributeList(catalogType)
            print formatString
            print attributeList
            # RRG:  changed self.dataArray["ra"].size to len(...)
            for i in range(len(self.dataArray["ra"])):
#                print map(lambda x: self.dataArray[x][i],attributeList)
                outputFile.write(formatString.format(map(lambda x: self.dataArray[x][i],attributeList)))
        outputFile.close()

    # composite astrometry operations
""" TODO (2/18/2010) incorporate the precession routines
    def makeMeasured(self):
        raOut, decOut = self.applyPropermotion(self.dataArray['ra'], 
                                    self.dataArray['dec'])
        raOut, decOut = self.applyParallax(raOut, decOut)
        self.addColumn(raOut, 'raMeasured')
        self.addColumn(decOut, 'decMeasured')
    def makeHelio(self):
        raOut, decOut = self.applyPrecession()
        raOut, decOut = self.applyPropermotion()
        self.addColumn(raOut, 'raHelio')
        self.addColumn(decOut, 'decHelio')
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

