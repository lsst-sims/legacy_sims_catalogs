{
'BASE_DIR' : "/astro/net/pogo3/krughoff/programs/python/catalogs_svn/generation_mssql/examples/sciCollabCats/exampleStarContribCat"
,
### Author Description ###
'AUTHOR' : "John B. Doe"
,
'INSTITUTION' : "My University of My State"
,
'DESCRIPTION' : '''This is an example for folks out there to add catalogs to
the image simulation databases.
'''
,
### Catalog Metadata ###
'ABSOLUTE_POSITIONS' : True #Absolute postitions -- default = True
,
'FIT_SPECTRUM' : False #Fit the spectrum using supplied colors.  All 6 LSST bands must be populated.
                     #If False, only one LSST magnitude and one spectrum name need to be supplied.
                     #Will warn if other mags do not agree to within 1 milimag
                     #default = False
,
'FIT_LIBRARY' : 'star' #options are star or galaxy.  This determines whether to
                     #use star spectra or galaxy spectra to try to fit the colors.
,
'SCALE_FILTER' : 'g' #If FIT_SPECTRUM is False, this parameter specifies the
                    #filter to use to scale the spectrum
                    #default = 'r'
,
'LC_METHOD' : 'applyMySpecialVariability' #If this is not None, the specified
                                      #method will be executed with the
                                      #supplied variability dictionary and
                                      #applied to the magnitude at the
                                      #specified time. default = None
,
'LC_DICTIONARY_KEYS' : ['t0', 'lcfile', 'period'] # default = None

,
### Catalog Columns ###
'CATALOG_DELIMITER' : '|' #Field delimiter.  For catalogs that contain
                          #variability information, this cannot be , or : since the dictionary string
                          #contains both characters
,
'CATALOG_COLUMNS' : ['id', 'ra', 'decl', \
		'umag', 'gmag', 'rmag', 'imag', 'zmag', 'ymag', \
		'specname', 'muRa', 'muDec', 'distance',\
                'doRed', 'varDictionary'] #This is the list of column names. This must be supplied.
,
'CATALOG_DTYPES' : [numpy.uint16, numpy.float64, numpy.float64, \
		numpy.float32, numpy.float32, numpy.float32, numpy.float32, numpy.float32, numpy.float32, \
		'S30', numpy.float32, numpy.float32, numpy.float32, \
	        numpy.bool, 'S60'] #This is the list of column data types.  This must be supplied.
,
'CATALOG_COLUMN_DESCRIPTIONS' : {'id': 'integer identifier per object',\
            'ra': 'Right Ascention in degrees:  May be either absolute or relative',\
            'dec': 'Declination in degrees: May be either absolute or relative',\
            '[ugrizy]mag': 'Apparent un-redenned magnitude in LSST bands',\
            'specname': 'Name of spectrum -- must be accompalied by at least one magnitude: default None',\
            'muRa/Dec': 'Proper motion in arcsec/yr: default 0',\
            'distance': 'Distance to the object in kpc: default 10',\
            'doRed': 'Redden source with 3D dust model?: default True',\
	    'varDictionary': 'The dictionary of variability parameters to be passed to LC_METHOD'} # This list of descriptions is here mostly for documentation.  The units are what are expected.)
}
