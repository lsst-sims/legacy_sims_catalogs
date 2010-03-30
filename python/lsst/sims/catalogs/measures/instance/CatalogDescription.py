""" CatalogDescription Class

    Class is a singleton that defines the attributes of a given catalog
    On creation it generates an object on further creation it generates 
    a poitner to the first instance so all versions have the same state

    Class is used in the definition of formats and validation

    ajc@astro 2/23/2010

    methods
    dataAttributeList
        return list of attributes in dataArray for a given InstanceClass object type
    dataFormatString
        returns formatString for output of data from an InstanceCatalog
    objectFormatString
        returns formatString for output of object in a trim file
    metadataAttributeList
        returns list of metadata attributes for a given InstanceClass type
    catalogTypeList
        returns a list of catalog types in the an instanceCatalog
    objectTypeList
        returns a list of object types in the an instanceCatalog

"""

import re
import shlex

class CatalogDescription (object):
    """ Singleton class  describes the defintions of types of InstanceClasses"""

    def __init__(cls, requireMetadataFile, requiredSchemaFile, requiredDerivedFile, requiredFormatFile):
        #        requireMetadataFile, requiredSchemaFile, requiredDerivedFile, requiredFormatFile):
        # List the attributes, header information and format string
        # for a given set of catalogTypes
        # Make all attributes private
        cls.__metadataAttributeList = cls.readRequiredData(requireMetadataFile)
        
        cls.__databaseAttributeList =  cls.readRequiredData(requiredSchemaFile)

        cls.__derivedAttributeList =  cls.readRequiredData(requiredDerivedFile)

        cls.__formatString =  cls.readRequiredData(requiredFormatFile)

    def formatString(self, catalogType):
        """Return the format for the output of the data for catalogType"""
        if ((catalogType in self.__formatString) == False):
            raise ValueError("Entry %s does not exist in format list"%catalogType)
        return self.__formatString[catalogType]

    def databaseAttributeList(self, catalogType):
        """Return the list of attributes in dataArray for catalogType"""
        return self.__databaseAttributeList[catalogType]

    def derivedAttributeList(self, catalogType):
        """Return the list of attributes in dataArray for catalogType"""
        return self.__derivedAttributeList[catalogType]

    def metadataAttributeList(self, catalogType):
        """Return the list of header data for catalogType"""
        return self.__metadataAttributeList[catalogType]

#    def listTypes(self):
#        """ Return a list of catalogTypes available"""
#        return self.__dataAttributeList.keys()

#    def listTypeList(self):
#        """ Return a list of objectTypes available"""
#        return self.__objectFormatString.keys()


    def readRequiredData(self, fileName):
        """ Read in a data file that specifies required attributes/metadata for a given catalog type
        
        Given a filename  returns a dictionary of tuples with attribute name and data type
        """
        fp = open(fileName,"r")
        metadata = {}
        attributeList = None
        for line in fp:
            line = line.strip()
            if line.startswith("["):
                if (attributeList != None):
                    metadata[objectType] = attributeList
                objectType = re.findall(r'\w+',line)[0]
                attributeList = []
            elif ((len(line) > 0) and (line.startswith("#") == False)):
                metadataKey,metadataType = shlex.split(line)
                attributeList.append((metadataKey,metadataType))
        
        metadata[objectType] = attributeList

        return metadata
