import numpy

import os
import unittest
import warnings
import sys
import lsst.utils.tests as utilsTests

from lsst.sims.catalogs.measures.instance import InstanceCatalog, compound, cached
from lsst.sims.catalogs.generation.db import DBObject, ObservationMetaData

class testCatalog(InstanceCatalog):
    catalog_type = 'MISC'
    default_columns=[('Opsim_expmjd',5000.0,float)]
    def db_required_columns(self):
        return ['Unrefracted_Dec'],['Opsim_altitude']

