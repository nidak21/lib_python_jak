#!/usr/bin/env python2.7 
#
# basic utilities that Jim uses
#
##############################

def removeNonAscii(text):
    """ return a string with all the non-ascii characters in text replaced
        by a space.
        Gets rid of those nasty unicode characters.
    """
    return  ''.join([i if ord(i) < 128 else ' ' for i in text])
##############################

def sublistFind(biglist, sublist, *args):
    """
    Return index of first occurance of sublist in biglist.
    Return -1 if not found
    optional args[0] = 1st index to start the search from
    optional args[1] = 1st index that should not participate in a match

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
##############################

if __name__ == "__main__":	# ad hoc test code
    x = [1,2,3,4,5,6,3,4,7]
    print sublistFind(x,[3,4,7],0,100)
    print sublistFind(x,[3,4,7],6,9)

    print sublistFind(x,[3,4,5])
    print sublistFind(x,[3,4,5], 2)
    print sublistFind(x,[3,4,5], 3)
    print sublistFind(x,[3,4,5], 4)
    print sublistFind(x,[3,4,], 2)
    print sublistFind(x,[3,4,], 3)
    print sublistFind(x,[3,4,], 7)
    print sublistFind(x,[3,4,], 18)
#    print sublistFind(x,[3,4,8])
#    print sublistFind(x,[9,4,8])
    print sublistFind(x,[3,4,7],0,8)
    print sublistFind(x,[3,4,7],7,9)
#    print sublistFind(x,[])
#    print sublistFind([3],[3,4,5])
#    print sublistFind([3],[3,])

#    print sublistFind(x,[3,4,8])
#    print sublistFind(x,[9,4,8])
#    print sublistFind(x,[3,4,7])
