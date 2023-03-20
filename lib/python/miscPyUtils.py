#!/usr/bin/env python3
#
# misc, basic utilities that Jim uses
#
##############################
import sys
import subprocess

def runShCommand(cmd):
    """ Run the specified shell cmd (string)
        Return (exitCode, stdout, stderr) from the command
        If the shell cmd does not write to stdout (or stderr), the return
            value will be ''
        If the shell cmd does write, they are '\n' terminated
    """
    cp = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return cp.returncode, cp.stdout, cp.stderr
# end runCommand() ------------------------------

def importPyFile(pyFile):
    ''' Given a python file pathname (w/ or w/o .py),
        import the file as a module.
        return the module.
    '''
    pyDir, pyFile = os.path.split(pyFile)
    if pyFile.endswith('.py'): pyFile = pyFile[:-3]

    if pyDir != '': sys.path.insert(0, pyDir)
    myModule =  __import__(pyFile)
    if pyDir != '': del sys.path[0]

    return myModule
# end importPyFile()-----------------------------------

def sublistFind(biglist, sublist, *args):
    """
    Return index of first occurance of sublist in biglist.
    Return -1 if not found
    optional args[0] = 1st index to start the search from
    optional args[1] = 1st index that should not participate in a match

    Intended to work like string.find(subStr [,start [, end]]) for lists.

    Maybe there is a more elegant or simpler way to do this...
    """
    lenSublist = len(sublist)
    lenBiglist = len(biglist)

    if  lenSublist == 0 or lenBiglist == 0: return -1
    firstElement = sublist[0]

    start = 0		# assume no optional args
    end   = lenBiglist
    if len(args) >= 1: start = args[0]	# got one, override
    if len(args) >= 2: end   = args[1]	# got two, override

    realEnd = end - lenSublist +1 # 1st index in biglist that cannot participate
                                  #   in match because of sublist length
    while start < realEnd:
        try:		# use index() assuming it is faster then my own loop
            firstI = biglist.index(firstElement, start, realEnd)
        except ValueError:			# first element not found
            return -1

        # matched first element at firstI
        j = 1		# index in sublist of next element to check
                        #   when j == len(sublist), we matched the whole sublist
        i = firstI +1	# index in biglist of next element to check
        while j < lenSublist:
            if biglist[i] != sublist[j]:
                break
            else: 	# keep going
                j += 1
                i += 1

        if j == lenSublist: return firstI
        else:               start += 1		# try again

    return -1
# end sublistFind()-----------------------------------

if __name__ == "__main__":
    pass
