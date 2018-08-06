#!/usr/bin/python

"""
Simple Library for reading URLs.
Simple readURL(url, ...) function
Simple ThrottledURLReader class
  - read from URLs with a min number of seconds between reads.
"""

import time
import urllib
import urllib2

def readURL(url,
	    GET=True,
	    params=None,
	    headers={},
	    ):
    """ Return results of the response from the URL.
	If params == None, we assume everything is encoded in the url.
	    and we do a GET
	Elif GET=True, params (if any) are added to the URL, & we do GET
	Else do a post with params. 

	Can pass in http headers if you like.
	Raises "Exception" with helpful msges for url errors.
    """
    data = params
    if params != None and GET == True: # need to encode params in the URL
	url = url + '?' +  urllib.urlencode(params)
	data = None

    request = urllib2.Request(url, data, headers )
    try:
	response = urllib2.urlopen(request)
	responseText = response.read()
    except urllib2.URLError, e:
	if hasattr(e, 'reason'):
	    raise Exception("Failed to reach server, reason: %s\nURL: '%s'\n" \
						% (e.reason,url))
	elif hasattr(e, 'code'):
	    raise Exception("Cannot fulfill request, code: %s\nURL: '%s'\n" \
						% (e.code,url))
    response.close()

    return responseText
# -------------------------

class ThrottledURLReader (object):
    """
    Provides a "read from a URL" method with a specified number (float) of
	seconds between reads so we don't overwhelm our welcome at a site.
    """
    def __init__(self,
		seconds=0.5	# float, minimum num of seconds between reads
		):
	self.minSeconds = seconds
	self.lastReadTime = 0.0	# initial last read was the beginning of time!
    #------------------------

    def readURL(self, url,
		GET=True,
		params=None,
		headers={},
		):
	""" see readURL() above"""
	secondsSinceLast = time.time() - self.lastReadTime
	if secondsSinceLast < self.minSeconds:
	    #print "sleeping for %10.7f" % (self.minSeconds - secondsSinceLast)
	    time.sleep(self.minSeconds - secondsSinceLast)

	output = readURL(url, GET=GET, params=params, headers=headers)
	self.lastReadTime = time.time()

	return output
    #------------------------

# end class ThrottledURLReader -------------------------

if __name__ == "__main__":	# test code
    r = ThrottledURLReader(seconds=0.5)
    for i in [1,2,3,4]:
	x = r.readURL("http://python.org")
	print "read %d, time: %10.7f" % (i,time.time())

