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
import math
import gzip
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
            self.addMetadata(name, metadata.parameters[name], metadata.comments[name],clobber)
    
    def validateMetadata(self, catalogType, opsimId):
        """ Validate that the metadata contains the correct attributes

        This does not test validity of the data"""

        return self.validateRequiredMetadata(catalogType,  opsimId) & self.validateDerivedMetadata(catalogType)

    def validateRequiredMetadata(self, catalogType, opsimId):
        """ Validate that the required (not derived) metadata contains the correct attributes

        This does not test validity of the data"""

        # get metadata list for required values
        attributeList = self.catalogDescription.getRequiredMetadata(catalogType, opsimId)
        for name in attributeList:
            if ((self.parameters.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in required metadata"%name)
        return True

    def validateDerivedMetadata(self, catalogType):
        """ Validate that the derived (not required) metadata contains the correct attributes

        This does not test validity of the data"""

        # get metadata list for derived values
        attributeList = self.catalogDescription.getDerivedMetadata(catalogType)
        for name in attributeList:
            if ((self.parameters.has_key(name) == False)):
                raise ValueError("Entry %s does not exist in derived metadata"%name)
        return True

    def writeMetadata(self, filename, catalogType, opsimId, newfile = False, filelist = None, compress = False):
        """Write metadata to file"""
        # open file
        if compress:
            if (newfile == False):
                outputFile = gzip.open(filename+".gz","a")
            else:
                outputFile = gzip.open(filename+".gz","w")
        else:
            if (newfile == False):
                outputFile = open(filename,"a")
            else:
                outputFile = open(filename,"w")

        # get required and derived metadata given catalogType
        attributeList = self.catalogDescription.getRequiredMetadata(catalogType, opsimId)[:]
        conversion = self.catalogDescription.getRequiredMetadataDataFormat(catalogType, opsimId)[:]

        attributeList.extend(self.catalogDescription.getDerivedMetadata(catalogType))
        conversion.extend(self.catalogDescription.getDerivedMetadataDataFormat(catalogType))
        
        if catalogType == 'TRIM':
            format = "%s %s \n"
        else:
            format = "# %s %s \n"
        #2.6 formatString = "{0} {1}\n"
        for name,conv in zip(attributeList,conversion):
            # 2.6 outputFile.write(formatString.format(name[0],self.parameters[name[0]]))
            x = self.parameters[name]
            outputFile.write(format%(name,eval(conv)))
        if filelist is not None:
            for file in filelist:
                outputFile.write("includeobj %s\n"%(file))
        outputFile.close()

  
