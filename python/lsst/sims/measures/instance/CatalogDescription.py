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
class CatalogDescription (object):
    """ Singleton class  describes the defintions of types of InstanceClasses"""

    def __new__(cls, *p, **k):
        if not '_the_instance' in cls.__dict__:
            cls._the_instance = object.__new__(cls)

            # List the attributes, header information and format string
            # for a given set of catalogTypes
            # Make all attributes private
            cls.__dataAttributeList = {"STAR":["ra","dec","time"], 
                                     "STUB":["id","ra","dec"]}

            cls.__metadataAttributeList = {"TRIM":["OPSIM_RA"], 
                                       "STUB":["OPSIM_RA"]}

            cls.__dataFormatString = {"TRIM":"", 
                                      "STUB":"{0[0]:d} {0[1]:g} {0[2]:g} \n"}

            cls.__objectFormatString = {"STAR":"object {0[0]:g} {0[1]:g} {0[2]:g}\n", 
                                        "SERSIC2D":"object {0[0]:g} {0[1]:g} {0[2]:.9g}\n"}

        return cls._the_instance

    def dataFormatString(self, catalogType):
        """Return the format for the output of the data for catalogType"""
        return self.__dataFormatString[catalogType]

    def objectFormatString(self, objectType):
        """Return the format for the trim file output of the data for objectType"""
        return self.__objectFormatString[objectType]

    def dataAttributeList(self, catalogType):
        """Return the list of attributes in dataArray for catalogType"""
        return self.__dataAttributeList[catalogType]

    def metadataAttributeList(self, catalogType):
        """Return the list of header data for catalogType"""
        return self.__metadataAttributeList[catalogType]

    def catalogTypeList(self):
        """ Return a list of catalogTypes available"""
        return self.__dataAttributeList.keys()

    def objectTypeList(self):
        """ Return a list of objectTypes available"""
        return self.__objectFormatString.keys()
