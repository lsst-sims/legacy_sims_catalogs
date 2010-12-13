""" CatalogDescription Class

    Class defines the attributes of a given catalog.

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
import os
from lsst.sims.catalogs.generation.config import ConfigObj

class CatalogDescription (object):
    """Class describes the definitions of types of InstanceClasses"""

    def __init__(cls, configFile, configType='CATALOG'):
        #        requireMetadataFile, requiredFieldsFile, derivedFieldsFile, requiredFormatFile):
        # List the attributes, header information and format string
        # for a given set of catalogTypes
        # Make all attributes private
        config = ConfigObj(configFile)
        fileName = os.path.join(os.getenv("CATALOG_DESCRIPTION_PATH"),
                                config[configType]['requiredMetadataFile'])
        cls.__requiredMetadata = ConfigObj(fileName)

        fileName = os.path.join(os.getenv("CATALOG_DESCRIPTION_PATH"),
                                config[configType]['derivedMetadataFile'])
        cls.__derivedMetadata =  ConfigObj(fileName)

        fileName = os.path.join(os.getenv("CATALOG_DESCRIPTION_PATH"),
                                config[configType]['requiredFieldsFile'])
        cls.__requiredFields =  ConfigObj(fileName)

        fileName = os.path.join(os.getenv("CATALOG_DESCRIPTION_PATH"),
                                config[configType]['derivedFieldsFile'])
        cls.__derivedFields =  ConfigObj(fileName)

        fileName = os.path.join(os.getenv("CATALOG_DESCRIPTION_PATH"),
                                config[configType]['formatFile'])
        cls.__format =  ConfigObj(fileName)

        fileName = os.path.join(os.getenv("FILE_MAP_PATH"),
                                config[configType]['derivedSEDPaths'])
        cls.__sedPaths =  ConfigObj(fileName)


    def getFormat(self, catalogType, objectType):
        """Return the formatter, attributeList tuple given catalogType and object type"""
        if ((catalogType in self.__format) == False):
            raise ValueError("Type %s does not exist in format list"%catalogType)

        if ((objectType in self.__format[catalogType]) == False):
            raise ValueError("Type %s does not exist in format list"% objectType)

        return self.__format[catalogType][objectType]['fmt'],self.__format[catalogType][objectType]['attributes']
        

    def getFields(self, fieldConfig, catalogType, neighborhoodType, objectType):
        """Return the list of attribute fields"""
        if ((catalogType in fieldConfig) == False):
            raise ValueError("Type %s does not exist in format list"%catalogType)

        if ((neighborhoodType in fieldConfig[catalogType]) == False):
            raise ValueError("Type %s does not exist in format list"% neighborhoodType)

        if ((objectType in fieldConfig[catalogType][neighborhoodType]) == False):
            raise ValueError("Type %s does not exist in format list"% objectType)

        return fieldConfig[catalogType][neighborhoodType][objectType].keys()

    def getRequiredFields(self, catalogType, neighborhoodType, objectType):
        """Return the list of attributes in dataArray for catalogType"""
        return self.getFields(self.__requiredFields, catalogType, neighborhoodType, objectType)

    def getDerivedFields(self, catalogType, neighborhoodType, objectType):
        """Return the list of attributes in dataArray for catalogType"""
        return self.getFields(self.__derivedFields, catalogType, neighborhoodType, objectType)


    def getMetadata(self, fieldConfig, catalogType, opsimId=False):
        """Return the list of header data for catalogType"""
        if ((catalogType in fieldConfig) == False):
            raise ValueError("Type %s does not exist in format list"%catalogType)
        if opsimId:
            if ((opsimId in fieldConfig[catalogType]) == False):
                raise ValueError("opsimId %s does not exist in format list"%opsimId)
            return fieldConfig[catalogType][opsimId].keys()
        else:
            return fieldConfig[catalogType].keys()

    def getRequiredMetadata(self, catalogType, opsimId=False):
        """Return the list of required header data for catalogType"""
        return self.getMetadata(self.__requiredMetadata, catalogType, opsimId)

    def getDerivedMetadata(self, catalogType):
        """Return the list of required header data for catalogType"""
        return self.getMetadata(self.__derivedMetadata, catalogType)



    def getPathMap(self, mapName = 'SPECMAP'):
        """Return the dictionary of SEDs and their relative paths on disk"""
        return self.__sedPaths[mapName]
