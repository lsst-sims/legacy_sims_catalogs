""" Metadata Class

    Contains the metadata associated with a file type and query

    ajc@astro 2/23/2010

    methods
    addMetadata(name,value,comment)
    deleteMetadata(name)
    mergeMetadata(metadata, clobber=True)

    writeMetadata(filename, catalogType)

"""
import warnings
from CatalogDescription import *

class Metadata (object):
    """ Class that describes the metadata for an instanceCatalog"""
    
    def __init__(self, configFile):
        self.catalogDescription = CatalogDescription(configFile)
        self.parameters = {}
        self.comments = {}
        self.ismerged = 0

    # metadata operations
    def addMetadata(self, name, value, comment, clobber=True):
        """ Add entry to metadata"""
        if (name in self.parameters):
            warnings.warn("Entry %s exists in metadata" % name)
            if (clobber == False):
                return
        self.parameters[name] = value
        self.comments[name] = comment

    def deleteMetadata(self, name):
        """ Delete item from metadata"""
        if ((name in self.parameters) == True):
            del self.parameters[name]
            del self.comments[name]
        else:
            raise ValueError("Entry %s does not exist in metadata"%name)
        
    def mergeMetadata(self, metadata, clobber=True):
        """ Loop through a metadata class and add parameters to existing metadata"""
        for name in metadata.parameters:
            self.addMetadata(name,metadata.parameters[name],metadata.comments[name],clobber)
    
    def validateMetadata(self, catalogType):
        """ Validate that the metadata contains the correct attributes

        This does not test validity of the data"""

        # get metadata list for required and derived values
        attributeList = self.catalogDescription.getRequiredMetadata(catalogType)
        for name in attributeList:
            if ((self.parameters.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in required metadata"%name[0])

        attributeList = self.catalogDescription.getDerivedMetadata(catalogType)
        for name in attributeList:
            if ((self.parameters.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in derived metadata"%name[0])


        return True


    def writeMetadata(self, filename, catalogType, newfile = False):
        """Write metadata to file"""
        # open file
        if (newfile == False):
            outputFile = open(filename,"a")
        else:
            outputFile = open(filename,"w")

        format = "%s %s \n"
        # get required and derived metadata given catalogType
        attributeList = self.catalogDescription.getRequiredMetadata(catalogType)[:]
        attributeList.extend(self.catalogDescription.getDerivedMetadata(catalogType))
        #2.6 formatString = "{0} {1}\n"
        for name in attributeList:
            # 2.6 outputFile.write(formatString.format(name[0],self.parameters[name[0]]))
            outputFile.write(format % (name,self.parameters[name]))

        outputFile.close()

  
