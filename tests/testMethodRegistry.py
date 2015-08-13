from __future__ import with_statement
import unittest
import lsst.utils.tests as utilsTests
from lsst.sims.catalogs.measures.instance import register_class, register_method

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

        Above, we have created a bunch of classes that inherit from
        each other, each with its own method registry.  Because of the
        way the decorators work, when you create a daughter class with
        a method registry, it alters the parent class's registry (i.e.
        after creating ClassB, ClassA's registry will contain _b_method,
        even though ClassA does not contain _b_method.  The test below
        verifies that, even though this is the case, you still cannot
        call _b_method from Class A.  The test also verifies that methods
        which are inherited can be called via the registry.
        """

        aa = ClassA()
        self.assertTrue(aa.call('a')=='a')
        self.assertRaises(KeyError, aa.call, 'b')
        self.assertRaises(KeyError, aa.call, 'c')
        self.assertRaises(KeyError, aa.call, 'd')

        bb = ClassB()
        self.assertTrue(bb.call('a')=='a')
        self.assertTrue(bb.call('b')=='b')
        self.assertRaises(KeyError, bb.call, 'c')
        self.assertRaises(KeyError, bb.call, 'd')

        cc = ClassC()
        self.assertTrue(cc.call('a')=='a')
        self.assertTrue(cc.call('b')=='b')
        self.assertTrue(cc.call('c')=='c')
        self.assertRaises(KeyError, cc.call, 'd')

        dd = ClassD()
        self.assertTrue(dd.call('a')=='a')
        self.assertTrue(dd.call('d')=='d')
        self.assertRaises(KeyError, dd.call, 'b')
        self.assertRaises(KeyError, dd.call, 'c')




def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(MethodRegistryTestCase)

    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
