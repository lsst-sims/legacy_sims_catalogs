from __future__ import with_statement
from builtins import object
import unittest
import lsst.utils.tests
from lsst.sims.catalogs.decorators import register_class, register_method


def setup_module(module):
    lsst.utils.tests.init()


@register_class
class ClassA(object):

    def call(self, key):
        return self._methodRegistry[key](self)

    @register_method('a')
    def _a_method(self):
        return 'a'


@register_class
class ClassB(ClassA):

    @register_method('b')
    def _b_method(self):
        return 'b'


@register_class
class ClassC(ClassB):

    @register_method('c')
    def _c_method(self):
        return 'c'


@register_class
class ClassD(ClassA):

    @register_method('d')
    def _d_method(self):
        return 'd'


class MethodRegistryTestCase(unittest.TestCase):

    def testMethodInheritance(self):
        """
        Test that the register_class and register_method decorators
        behave appropriately and preserve inheritance.
        """

        aa = ClassA()
        self.assertEqual(aa.call('a'), 'a')

        # below, we test to make sure that methods which
        # should not be in ClassA's _methodRegistry are not
        # spuriously added to the registry
        self.assertRaises(KeyError, aa.call, 'b')
        self.assertRaises(KeyError, aa.call, 'c')
        self.assertRaises(KeyError, aa.call, 'd')

        bb = ClassB()
        self.assertEqual(bb.call('a'), 'a')
        self.assertEqual(bb.call('b'), 'b')
        self.assertRaises(KeyError, bb.call, 'c')
        self.assertRaises(KeyError, bb.call, 'd')

        cc = ClassC()
        self.assertEqual(cc.call('a'), 'a')
        self.assertEqual(cc.call('b'), 'b')
        self.assertEqual(cc.call('c'), 'c')
        self.assertRaises(KeyError, cc.call, 'd')

        dd = ClassD()
        self.assertEqual(dd.call('a'), 'a')
        self.assertEqual(dd.call('d'), 'd')
        self.assertRaises(KeyError, dd.call, 'b')
        self.assertRaises(KeyError, dd.call, 'c')


class MemoryTestClass(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
