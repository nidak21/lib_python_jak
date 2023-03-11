'''
config file looks like:
[DEFAULT] # a section heading

HOMEDIR: /Users/jak/work/gxd_htLearning
# a comment
DATADIR: %(HOMEDIR)s/Data		# refer to another config var
'''
from ConfigParser import ConfigParser

#-----------------------------------
# Config, constants, ...
# ---------------------------
cp = ConfigParser()
cp.optionxform = str # make keys case sensitive
cp.read(["config.cfg", "../config.cfg"])

DATADIR = cp.get("DEFAULT", "DATADIR")
print DATADIR

