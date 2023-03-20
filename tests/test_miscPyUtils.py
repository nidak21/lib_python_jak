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
######################################

if __name__ == '__main__':
    unittest.main()
