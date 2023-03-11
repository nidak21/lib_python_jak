#!/usr/bin/env python

#  runsql.py
###########################################################################
#
#  Purpose:
#	   run one or more sql commands from stdin (separated by "||"),
#		and send the output in tab-delimited form to stdout
#
#  Usage:  
USAGETEXT = """Usage: runsql.py [-s server] [-d database] [-sep string] [-delim string] [-q]
	Run one or more sql commands from stdin,
	and send output in tab-delimited form to stdout.
	-s (or -S) server   defaults to ADHOC_MGI.
	-d (or -D) database defaults to mgd.
	Everything runs as MGD_PUBLIC.
	-sep string defines the sql command separator string, default is "||"
	-delim string defines the output field separator, default is tab
	-q means quiet - don't write diagnositic msgs to stderr
"""
#
#  Env Vars:  None
#
#  Inputs:	SQL commands from stdin
#
#  Outputs:     SQL output to stdout	
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Assumes:  Nothing
#
###########################################################################
#import ignoreDeprecation
import sys
import os
import string
import time
#import symbolsort
import db
#import loadlib

#from tabledatasetlib import *
#from gtcmplib import *

#
#  CONSTANTS
#
DEBUG = 0

def usage():
# Purpose: print usage message and exit
    sys.stderr.write( USAGETEXT)
    sys.exit(1)
# end usage() --------------------------------------------------------

def getArgs():
# Purpose: parse command line args
# Returns: a dict mapping arg names to values
#	   "script" -> "$0" - the name of this script
# Effects: dies with usage if invalid args are passed

    argDict = {}	# dict mapping argument "names" to their values
    myArgs = sys.argv	# local copy of args

    argDict[ "script"] = myArgs[0]	# always the name of the invoked script
    del myArgs[0]

    # Set defaults:
    argDict[ "DBSERVER"]     = "mgi-adhoc"
    argDict[ "DBNAME"]       = "mgd"
    argDict[ "SQLSEPARATOR"] = "||"
    argDict[ "DELIMITER"]    = "\t"
    argDict[ "QUIET"]        = False

    # Process "-" flag arguments
    while len(myArgs) > 0 and myArgs[0][0] == "-": # while next arg start w/ -
	arg = myArgs[0]
	del myArgs[0]		# throw arg away, i.e., "shift"
	if arg == "-d" or arg == "-D":
	    getRequredFlagValue( myArgs, argDict, "DBNAME")
	elif arg == "-s" or arg == "-S":
	    getRequredFlagValue( myArgs, argDict, "DBSERVER")
	elif arg == "-sep":
	    getRequredFlagValue( myArgs, argDict, "SQLSEPARATOR")
	elif arg == "-delim":
	    getRequredFlagValue( myArgs, argDict, "DELIMITER")
	elif arg == "-q":
	    argDict[ "QUIET"] = True
	else:
	    usage()

    # Process non-flag (non "-") args
    if len(myArgs) != 0:
	usage()

    # Apply any cleanups and error checks

    return argDict

# end getArgs() ----------------------------------------------------------

def getRequredFlagValue( args,	# arg list (like sys.argv)
		  argDict,	# dictionary to hold labels mapping to arg vals
		  label		# dict label (key) to use for this arg
		  ):
# Purpose: check that args has at least one more arg in it and assign
#	   argDict[ label] = arg[0]
# 	   Invoke usage() (and die) if args is empty
# Effects: Remove the arg from args.

    if len(args) == 0:
	usage()
    else:
	argDict[ label] = args[0]
	del args[0]

# end getRequredFlagValue()-------------------------------------------------

def process ():
# Purpose: Main routine of this script
# Returns: nothing

    args = getArgs()

    notQuiet = not args[ "QUIET"]

    db.set_sqlServer  ( args["DBSERVER"])
    db.set_sqlDatabase( args["DBNAME"])
    db.set_sqlUser    ("mgd_public")
    db.set_sqlPassword("mgdpub")
    #db.setAutoTranslate(False)

    query = sys.stdin.read()
    queries = string.split(query, args[ "SQLSEPARATOR"])

    if notQuiet:
	sys.stderr.write("Running %d SQL command(s) on %s..%s\n" % \
			( len( queries), args[ "DBSERVER"], args[ "DBNAME"]) )
	sys.stderr.flush()

    startTime = time.time()
    results = db.sql( queries, 'auto')
    endTime = time.time()
    if notQuiet:
	sys.stderr.write( "Total SQL time: %8.3f seconds\n" % \
							(endTime-startTime))

    delim = args[ "DELIMITER"]
    #print results
    cmdnum = 0			# number of SQL commands processed so far
    for result in results:
	cmdnum = cmdnum +1
	if len(result) == 0:	# result w/ no output, skip
	    continue

	# print number of rows in this result
	if notQuiet:
	    sys.stderr.write( "------------\n")
	    sys.stderr.write( "Command %d:  %d row(s)\n" % \
						(cmdnum, len( result)) )
	    sys.stderr.write( "%s\n" % queries[cmdnum-1])
	    sys.stderr.write( "------------\n")
	    sys.stderr.flush()

	# print column headers
	# (JIM sometime, try to figure out the order of the columns in the
	#  select statement, and output them in that order)
	r = result[0]		# grab 1st line of result
	if r == None:
	    continue
	sys.stdout.write( string.join( r.keys(), delim) )
	sys.stdout.write( "\n")

	# print results, one line per row (result), tab-delimited
	for r in result:
	    #print r
	    first = True	# =true if 1st column in this row
	    for c in r.keys():
		if not first:
		    sys.stdout.write( delim)
		first = False
		s = str( r[c] )
		# clean up the string (replace field delims and newlines
		#  not sure this is the best thing to do, but messing
		#  up you (tab) delimited file is a pain.
		s2 = s.replace(delim, ' ').replace('\n', '')
		sys.stdout.write( "%s" % s2)
	    sys.stdout.write( "\n")

	sys.stdout.flush()
    # end for loop for results
    
# end process() ----------------------------------

#
#  MAIN
#
process()
sys.exit(0)
