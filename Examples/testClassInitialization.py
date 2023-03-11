
# make sure I understand when things get instantiated

import sys
import re

class foo (object):
    sys.stdout.write('initializing x\n')
    x = 'initial x'

    def __init__(self):
        y = 'initial y'

    def printx(self):
        sys.stdout.write("value for x: '%s'\n" % self.x)
    def setx(self, t):
        self.x = t

    sys.stdout.write('initializing z\n')
    z = 'initial z'
    def printz(self):
        sys.stdout.write("value for z: '%s'\n" % self.z)

class blah (object):
    myFoo = foo()

    def __init__(self):
        pass

    def showX(self):
        self.myFoo.printx()

    def setX(self, val):
        self.myFoo.setx(val)

    def showZ(self):
        self.myFoo.printz()

sys.stdout.write('instantiating 2 blahs\n')
b1 = blah()
sys.stdout.write('done 1st blah\n')
b2 = blah()
sys.stdout.write('done 2nd blah\n')
b1.showX()
b2.showX()
b1.setX('something else')
b1.showX()
b2.showX()
