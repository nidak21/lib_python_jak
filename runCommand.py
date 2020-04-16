#!/usr/local/bin/python

# Name: runCommand.py
# Purpose: This module contains routines to encapsulate the forking of
#	processes to run Bourne shell commands and capture their output.  It
#	is loosely based on code Geoff previously wrote for MouseBLAST.
# Author: Jon Beal
# Notes: There are two public items in this module -- the UnixCommand class
#	and the runCommand() function.  If one wants to get really involved
#	and run the same command multiple times with different environment
#	variables, then you may want to use Command directly.  For most cases,
#	however, it would be simpler to just use the runCommand() function
#	which serves as a wrapper over the UnixCommand class.
#
#	Remember that the unix shell command you specify will be run in the
#	Bourne shell (sh), regardless of the value of the SHELL environment
#	variable.  If you'd like to run it using another shell, you can
#	specify it as part of the command.  For example, doing...
#		runCommand ('/usr/bin/csh -c ps')
#	will (by default) open up a Bourne shell, then invoke the C-shell
#	within it, then have csh execute the command specified after
#	the -c (ps, in this case).

import os
import sys

###--- Public Functions ---###

def runCommand (command,	# string; Unix command to execute
	envvars = {}		# dictionary; maps env var names to values
	):
	# Purpose: runs the given 'command' in a Bourne shell with the given
	#	'envvars' specified.  captures and returns the contents of
	#	stdout, stderr, and the exit code.
	# Returns: (string stdout, string stderr, integer exit code)
	# Assumes: nothing
	# Effects: varied depending on the parameters
	# Throws: nothing

	c = UnixCommand (command, envvars)
	c.go()
	return c.getStdOut(), c.getStdErr(), c.getExitCode()

###--- Public Classes ---###

class UnixCommand:
	# IS:	one Unix shell command to be run in a Bourne shell.  It may
	#	include pipes, wildcards, and such.
	# HAS:	the shell command itself, along with (optional) environment
	#	variable settings
	# DOES:	runs the command; captures stdout, stderr, and the exit code,
	#	and provides accessor functions for them

	def __init__ (self,
		command,	# string; command to execute
		envvars = {}	# dictionary; maps env var names to values
		):
		# Purpose: instantiate the UnixCommand object
		# Returns: nothing
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing
		# Example:
		#	c = UnixCommand ('ls -l /tmp')

		self.command = command
		self.envvars = envvars
		self.stdout = ''
		self.stderr = ''
		self.exitCode = -1
		return

	def setEnvVar (self,
		envvar,		# string; name of an environment variable
		value		# string; value for that environment variable
		):
		# Purpose: allow the user to specify environment variables
		#	which should be in-place when this UnixCommand is
		#	executed
		# Returns: nothing
		# Assumes: nothing
		# Effects: overwrites any previous value for 'envvar'
		# Throws: nothing

		self.envvars[envvar] = value
		return

	def setEnvVars (self,
		envvars = {}	# dictionary; maps env var names to values
		):
		# Purpose: updates the set of environment variables for this
		#	UnixCommand to be those given in 'envvars'
		# Returns: nothing
		# Assumes: nothing
		# Effects: overwrites the previous set of environment
		#	variables and values for this UnixCommand
		# Throws: nothing

		self.envvars = envvars
		return

	def go (self):
		# Purpose: runs this UnixCommand and captures stdout, stderr,
		#	and the exit code
		# Returns: integer -- exit code returned by the command
		# Assumes: nothing
		# Effects: invokes the UnixCommand in a Bourne shell, so
		#	effects will vary depending on the command
		# Throws: nothing

		sys.stdout.flush()		# clean out stdout and stderr
		sys.stderr.flush()		# before we capture them

		(out_r, out_w) = os.pipe()	# create a pipe for stdout
		(err_r, err_w) = os.pipe()	# create a pipe for stderr
		pid = os.fork()			# fork a child process

		if pid != 0:
			# We are in the parent process, so we will close down
			# the writing ends of the pipes and collect data from
			# the child process.  Finally, close the reading ends
			# of the pipes.

			os.close(out_w)
			os.close(err_w)
			self.stdout = readAllFrom(out_r)
			self.stderr = readAllFrom(err_r)
			p, self.exitCode = os.wait()
			self.exitCode = shiftExitCode(self.exitCode)
			os.close(out_r)
			os.close(err_r)
		else:
			# We are in the child process, so we will actually
			# run the command here.  First, close the reading
			# ends of the pipes, and map the writing ends
			# to stdout and stderr.

			os.close(out_r)
			os.close(err_r)
			os.dup2(out_w, 1)
			os.dup2(err_w, 2)

			# set up the environment for the command:

			for envvar in self.envvars.keys():
				os.environ[envvar] = self.envvars[envvar]

			# run the command, catch the exit code, and bail out:

			self.exitCode = shiftExitCode(os.system(self.command))
			os.close(out_w)
			os.close(err_w)
			os._exit(self.exitCode)
		return self.exitCode

	def getStdOut (self):
		# Purpose: accessor
		# Returns: string - contains everything written to stdout when
		#	the command was last run
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.stdout

	def getStdErr (self):
		# Purpose: accessor
		# Returns: string - contains everything written to stderr when
		#	the command was last run
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.stderr

	def getExitCode (self):
		# Purpose: accessor
		# Returns: integer - the exit code returned when the command
		#	was last run
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.exitCode

###--- Private Functions ---###

def shiftExitCode (
	i		# integer; exit code from a child process
	):
	# Purpose: move a return code from the high order byte of an integer
	#	to the low order byte
	# Returns: integer -- 'i' divided by 256
	# Assumes: nothing
	# Effects: nothing
	# Throws: nothing
	# Notes: For some reason, it seems that whenever Python gets an exit
	#	code from a child process, its value is 256 times whatever the
	#	value was in the child process.  My suspicion is that the
	#	code was shifted into the high order byte rather than leaving
	#	it in the low-order byte.  This function is meant to shift it
	#	back down, leaving the original exit code intended by the
	#	child process.

	return i >> 8

def readAllFrom (
	fd,			# integer; file descriptor
	bufferSize = 2048	# integer; how many bytes to read at once
	):
	# Purpose: read all the data available on 'fd' and return it as a
	#	single string
	# Returns: string; see Purpose
	# Assumes: 'fd' is open for reading
	# Effects: reads from 'fd'
	# Throws: posix.error if 'fd' is not open for reading

	allout = ''
	out = os.read(fd, bufferSize)
	while out != '':
		allout = allout + out
		out = os.read(fd, bufferSize)
	return allout

###--- Self-test ---###

if __name__ == '__main__':
	stdout, stderr, code = \
		runCommand ('env | sort | grep -c AAAAAA',
			envvars={'AAAAAA' : 'BBBB'}
			)
	print '----------stdout'
	print stdout[:-1]
	print '----------stderr'
	print stderr[:-1]
	print '----------exit code'
	print code
#
# Warranty Disclaimer and Copyright Notice
# 
#  THE JACKSON LABORATORY MAKES NO REPRESENTATION ABOUT THE SUITABILITY OR 
#  ACCURACY OF THIS SOFTWARE OR DATA FOR ANY PURPOSE, AND MAKES NO WARRANTIES, 
#  EITHER EXPRESS OR IMPLIED, INCLUDING MERCHANTABILITY AND FITNESS FOR A 
#  PARTICULAR PURPOSE OR THAT THE USE OF THIS SOFTWARE OR DATA WILL NOT 
#  INFRINGE ANY THIRD PARTY PATENTS, COPYRIGHTS, TRADEMARKS, OR OTHER RIGHTS.  
#  THE SOFTWARE AND DATA ARE PROVIDED "AS IS".
# 
#  This software and data are provided to enhance knowledge and encourage 
#  progress in the scientific community and are to be used only for research 
#  and educational purposes.  Any reproduction or use for commercial purpose 
#  is prohibited without the prior express written permission of the Jackson 
#  Laboratory.
# 
# Copyright (c) 1996, 1999, 2002 by The Jackson Laboratory
# All Rights Reserved
#

