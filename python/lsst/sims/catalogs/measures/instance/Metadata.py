""" Metadata Class

    Contains the metadata associated with a file type and query

    ajc@astro 2/23/2010

    methods
    addMetadata(name,value,comment)
    deleteMetadata(name)
    mergeMetadata(metadata, clobber=True)

    writeMetadata(filename, catalogType)

"""
import CatalogDescription
import warnings

class Metadata (object):
    """ Class that describes the metadata for an instanceCatalog"""
    
    def __init__(self):
        self.catalogDescription = None
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
    
    def validateMetadata(self, dataType):
        """ Validate that the metadata contains the correct attributes

        This does not test validity of the data"""
        if (dataType == None):
            return True
        else:
            attributeList = self.catalogDescription.metadataAttributeList(dataType)
            for name in attributeList:
                if ((self.parameters.has_key(name) == False)):
                    raise ValueError("Entry %s does not exist in data"%name)
        return True


    def writeMetadata(self, filename, catalogType, newfile = False):
        """Write metadata to file"""
        # open file
        if (newfile == False):
            outputFile = open(filename,"a")
        else:
            outputFile = open(filename,"w")

        if (catalogType != None):
            attributeList = self.catalogDescription.metadataAttributeList(catalogType)
            formatString = "{0} {1}\n"
            for name in attributeList:
                #print '   Name: ', name[0]
                outputFile.write(formatString.format(name[0],self.parameters[name[0]]))
        else:
            formatString = "#{0} {1}\n"
            for name in self.parameters:
                outputFile.write(formatString.format(name[0],self.parameters[name[0]]))
        outputFile.close()

  
