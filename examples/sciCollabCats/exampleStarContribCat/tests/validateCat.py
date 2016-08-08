#!/usr/bin/env python
import sys
import os
import numpy

"""
This program reads the metadata and catalog files and does sanity checks on
all the constituent files.
"""
class CatalogValidator(object):
    def __init__(self, configfile, catfile="catalog.txt"):
        self.parameterDictionary = eval(open(os.path.join(configfile), 'r').read())
        try:
            self.base_dir = self.parameterDictionary['BASE_DIR']
        except:
            print "BASE_DIR not set.  Assuming it is the current working directory"
            self.base_dir = os.getcwd()
        sys.path.append(os.path.join(self.base_dir,"python"))
        print "***Validating parameters from configuration."
        self.validateParameters()
        print "***Reading catalog."
        self.readCatalogFile(catfile)
        print "***Validating schema and variability parameters."
        self.validateSchema()

    def validateParameters(self):
        metadataDefaults = {'ABSOLUTE_POSITIONS': True, 'FIT_SPECTRUM': False,\
                'FIT_LIBRARY': 'star', 'SCALE_FILTER': 'r', 'LC_METHOD': None,\
                'LC_DICTIONARY_KEYS': None, 'CATAOLOG_DELIMITER': '|'}
        requiredMetadata = ['AUTHOR', 'INSTITUTION', 'DESCRIPTION', 'CATALOG_COLUMNS',\
                'CATALOG_DTYPES']
        for k in requiredMetadata:
            if not self.parameterDictionary.has_key(k):
                raise("ERROR:  The required parameter -- %s -- is not set in settings.py"%(k))
        for k in metadataDefaults.keys():
            if self.parameterDictionary.has_key(k):
                continue
            else:
                self.parameterDictionary[k] = metadataDefaults[k]

    def validateSchema(self):
        schema = {}
        schema['point'] = ['id', 'ra', 'decl', 'umag', 'gmag', 'rmag',\
                'imag', 'zmag', 'ymag', 'specname', 'muRa', 'muDec', 'distance',\
                'doRed', 'varDictionary']
        #We will allow users to either apply zero reddenning or to have us embed the
        #object in our dust model.
        #Make sure there are positions supplied
        if not ('ra' in self.parameterDictionary['CATALOG_COLUMNS'] and 'decl'\
                in self.parameterDictionary['CATALOG_COLUMNS']):
            raise("Positional columns must exist in the catalog") 

        #If the spectrum is to be fit, we need all mags and a fit library
        if self.parameterDictionary['FIT_SPECTRUM']:
            for mag in ['umag', 'gmag', 'rmag', 'imag', 'zmag', 'ymag']:
                if not mag in self.parameterDictionary['CATALOG_COLUMNS']:
                    raise("All magnitudes must be populated for a fit to be done:\
                            %s is missing"%(mag))
                else:
                    magmedian = numpy.median(self.catalog[self.columnMap[mag]]) 
                    if magmedian < 16:
                        print "WARNING -- The values of column %s seem to be"
                        print "too bright to make sense for the image simulations."
                        print "Simulations saturate at ~16 the median of %s is %f"%(mag, magmedian)

        #If we are not asked to fit the spectrum we will scale the spectrum to
        #the specified magnitude which must exist
        else:
            mag = "%smag"%(self.parameterDictionary['SCALE_FILTER'])
            if mag not in self.parameterDictionary['CATALOG_COLUMNS']:
                raise("SCALE_FILTER was set to %s, but does not exist in the catalog"%(self.parameterDictionary['SCALE_FILTER']))
            else:
                magmedian = numpy.median(self.catalog[self.columnMap[mag]]) 
                if magmedian < 16:
                    print "WARNING -- The values of column %s seem to be"
                    print "too bright to make sense for the image simulations."
                    print "Simulations saturate at ~16 the median of %s is %f"%(mag, magmedian)

        #We will test to make sure the variability dictionaries are populated
        #if a variability method is supplied.  We also test to see if there is
        #any offset in the first year of the survey (from OpSim).
        if self.parameterDictionary['LC_METHOD'] is not None:
           self.testVariability()

    def testVariability(self):
        import variability
        for var in self.catalog[self.columnMap['varDictionary']]:
            varDictionary = eval(var)
            for k in self.parameterDictionary['LC_DICTIONARY_KEYS']:
                if not k in varDictionary.keys():
                    raise("ERROR: The key %s defined in settings.py is not in the\
                        variability dictionary: %s"%(k, myd))
            startDay = 49353.
            epochs = numpy.linspace(startDay, startDay+365.25, 1000.)
            magoffsets = eval("variability.%s(varDictionary, epochs)"%(self.parameterDictionary['LC_METHOD']))
            if numpy.all(magoffsets == 0.):
                print "Variability WARNING -- No deviation in any band seen for variability in first year of the survey."

    def readCatalogFile(self, filename):
        self.catalog = numpy.array([None for el in self.parameterDictionary['CATALOG_COLUMNS']])
        tmpcat = \
        numpy.loadtxt(os.path.join(self.base_dir,filename), delimiter=self.parameterDictionary['CATALOG_DELIMITER'], comments="#", \
                dtype={'names': self.parameterDictionary['CATALOG_COLUMNS'], 'formats': self.parameterDictionary['CATALOG_DTYPES']})
        self.columnMap = {}
        #Apparently loadtxt does not unpack if dtype is specified.  The
        #following is to transpose the array
        for pair in zip(self.parameterDictionary['CATALOG_COLUMNS'], \
                range(len(self.parameterDictionary['CATALOG_COLUMNS'])), \
                self.parameterDictionary['CATALOG_DTYPES']):
            self.columnMap[pair[0]] = pair[1]
            self.catalog[pair[1]] = numpy.array([el[pair[1]] for el in tmpcat], dtype=pair[2])

'''
schema['extended'] = ['id', 'ra', 'dec', 'umag', 'gmag', 'rmag',\
        'imag', 'zmag', 'ymag', 'specname', 'redshift', 'ebv']
This is much harder.  Do we make the users give all information for every
component on one line, or do they need to be one per line.  Do we make them
give us the normalization for each component or do we derive that from
bulge/total or something else?
'''

if __name__ == "__main__":
    cv = CatalogValidator("settings.py")
