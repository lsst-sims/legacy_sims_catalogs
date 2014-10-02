"""
This file will define classes that control who ObservationMetaData describes
its field of view (i.e. is it a box in RA, Dec, is it a circle in RA, Dec....?)

Hopefully it will be extensible so that we can add different shapes in the
future
"""

import numpy

class FieldOfViewMetaClass(type):
    """
    Meta class for fieldOfView.  The idea is to build a registry of all
    valid fields of view so that fields can be instantiated from just a
    dictionary key.

    Largely this is being copied from the DBObjectMeta class in
    dbConnection.py
    """

    def __init__(cls,name,bases,dct):
        if not hasattr(cls,'foVregistry'):
            cls.foVregistry={}
        else:
            cls.foVregistry[cls.boundType] = cls

        return super(FieldOfViewMetaClass, cls).__init__(name,bases,dct)

class FieldOfView(object):
    __metaclass__ = FieldOfViewMetaClass

    @classmethod
    def getFieldOfView(self,name,*args,**kwargs):
        if name in self.foVregistry:
            return self.foVregistry[name](*args,**kwargs)
        else:
            raise RuntimeError("There is no FieldOfView class keyed to %s" % name)

class CircularFieldOfView(FieldOfView):

    boundType = 'circle'

    def __init__(self,ra,dec,radius):
        self.RA = ra
        self.DEC = dec
        self.radius = radius

    def to_SQL(self, RAname, DECname):

        if self.DEC != 90.0 and self.DEC != -90.0:
            RAmax = self.RA + \
            360.0 * numpy.arcsin(numpy.sin(0.5*numpy.radians(self.radius)) / numpy.cos(numpy.radians(self.DEC)))/numpy.pi
            RAmin = self.RA - \
            360.0 * numpy.arcsin(numpy.sin(0.5*numpy.radians(self.radius)) / numpy.cos(numpy.radians(self.DEC)))/numpy.pi
        else:
           #just in case, for some reason, we are looking at the poles
           RAmax = 360.0
           RAmin = 0.0

        DECmax = self.DEC + self.radius
        DECmin = self.DEC - self.radius

        #initially demand that all objects are within a box containing the circle
        #set from the DEC1=DEC2 and RA1=RA2 limits of the haversine function
        bound = ("%s between %f and %f and %s between %f and %f "
                     % (RAname, RAmin, RAmax, DECname, DECmin, DECmax))

        #then use the Haversine function to constrain the angular distance form boresite to be within
        #the desired radius.  See http://en.wikipedia.org/wiki/Haversine_formula
        bound = bound + ("and 2 * ASIN(SQRT( POWER(SIN(0.5*(%s - %s) * PI() / 180.0),2)" % (DECname,self.DEC))
        bound = bound +("+ COS(%s * PI() / 180.0) * COS(%s * PI() / 180.0) * POWER(SIN(0.5 * (%s - %s) * PI() / 180.0),2)))"
             % (DECname, self.DEC, RAname, self.RA))
        bound = bound + (" < %s " % (self.radius*numpy.pi/180.0))

        return bound

class BoxFieldOfView(FieldOfView):

    boundType = 'box'

    def __init__(self,ra,dec,length):
        self.RA = ra
        self.DEC = dec

        if isinstance(length,float):
            self.RAmin = self.RA-length
            self.RAmax = self.RA+length
            self.DECmin = self.DEC-length
            self.DECmax = self.DEC+length
        elif len(length)==1:
            self.RAmin = self.RA-length[0]
            self.RAmax = self.RA+length[0]
            self.DECmin = self.DEC-length[0]
            self.DECmax = self.DEC+length[0]
        else:
            try:
                self.RAmin = self.RA-length[0]
                self.RAmax = self.RA+length[0]
                self.DECmin = self.DEC-length[1]
                self.DECmax = self.DEC+length[1]
            except:
                raise RuntimeError("BoxFieldOfView is unsure how to handle length %s " % str(length))

        self.RAmin %= 360.0
        self.RAmax %= 360.0

    def to_SQL(self, RAname, DECname):
        #KSK:  I don't know exactly what we do here.  This is in code, but operating
        #on a database is it less confusing to work in degrees or radians?
        #(RAmin, RAmax, DECmin, DECmax) = map(math.radians,
        #                                     (RAmin, RAmax, DECmin, DECmax))

        #Special case where the whole region is selected
        if self.RAmin < 0 and self.RAmax > 360.:
            bound = "%s between %f and %f" % (DECname, self.DECmin, self.DECmax)
            return bound

        if self.RAmin > self.RAmax:
            # XXX is this right?  It seems strange.
            bound = ("%s not between %f and %f and %s between %f and %f"
                     % (RAname, self.RAmax, self.RAmin,
                        DECname, self.DECmin, self.DECmax))
        else:
            bound = ("%s between %f and %f and %s between %f and %f"
                     % (RAname, self.RAmin, self.RAmax, DECname, self.DECmin, self.DECmax))

        return bound


