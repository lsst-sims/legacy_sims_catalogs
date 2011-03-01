""" SiteDescription Class

    Class defines the attributes of the site unless overridden
    ajc@astro 2/23/2010

"""

class SiteDescription (object):
    """ Class  describes the defintions of types of InstanceClasses"""
    def __init__(self):
        self.parameters = {"longitude":-1.2320792, "latitude" :
            -0.517781017, "height" : 2650, "xPolar" : 0., "yPolar" : 0.,
            "meanTemperature" : 284.655, "meanPressure" : 749.3,
            "meanHumidity" : 0.40, "lapseRate" : 0.0065}



"""
**     elongm d      mean longitude of the observer (radians, east  
+ve)
**     phim   d      mean geodetic latitude of the observer (radians)
**     hm     d      observer's height above sea level (metres)
**     xp     d      polar motion x-coordinate (radians)
**     yp     d      polar motion y-coordinate (radians)
**     tdk    d      local ambient temperature (DegK; std=273.155)
**     pmb    d      local atmospheric pressure (mB; std=1013.25)
**     rh     d      local relative humidity (in the range 0.0-1.0)
**     wl     d      effective wavelength (micron, e.g. 0.55)
**     tlr    d      tropospheric lapse rate (DegK/metre, e.g. 0.0065)
"""
