#!/usr/bin/env python3

import sys
import unittest
import os
import os.path
from miscPyUtils import *

"""
These are tests for miscPyUtils.py

Usage:   python test_miscPyUtils.py [-v]
"""
######################################

class runShCommand_tests(unittest.TestCase):
    def setUp(self):
        pass

    def test_runShCommand1(self):
        retcode, stout, sterr = runShCommand('outtest "foot ball" "soccer" 23')
        self.assertEqual(retcode, 23)
        self.assertEqual(stout, 'foot ball\n')
        self.assertEqual(sterr, 'soccer\n')

    def test_runShCommand2(self):
        retcode, stout, sterr = runShCommand('outtest "foot ball" "" 0')
        self.assertEqual(retcode, 0)
        self.assertEqual(stout, 'foot ball\n')
        self.assertEqual(sterr, '\n')

    def test_runShCommand_nooutput(self):
        retcode, stout, sterr = runShCommand('errcode 5')
        self.assertEqual(retcode, 5)
        self.assertEqual(stout, '')
        self.assertEqual(sterr, '')
# end class runShCommand_tests

class sublistFind_tests(unittest.TestCase):
    def setUp(self):
        self.list = [1,2,3,4,5,6,3,4,7]

    def test_sublistFind1(self):
        i = sublistFind(self.list,[])   # empty list
        self.assertEqual(i, -1)

        i = sublistFind(self.list,[1,])
        self.assertEqual(i, 0)

        i = sublistFind(self.list,[1,2])
        self.assertEqual(i, 0)

        i = sublistFind(self.list,[47,48,49,50,51,52,53,54,55,56,57])
        self.assertEqual(i, -1)

        i = sublistFind(self.list,[3,4,7],0,100)
        self.assertEqual(i, 6)

        i = sublistFind(self.list,[3,4],2,100)
        self.assertEqual(i, 2)

        i = sublistFind(self.list,[3,4],3,100)
        self.assertEqual(i, 6)

        i = sublistFind(self.list,[3,4],3,7)
        self.assertEqual(i, -1)

        i = sublistFind(self.list,[3,4],3,8)
        self.assertEqual(i, 6)

# end class sublistFind_tests

if __name__ == '__main__':
    unittest.main()
