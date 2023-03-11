
import sys
from urllib import urlopen, urlparse, request

numToShow = 6

try:
    url = sys.argv[1]
except:
    url = 'learning-python.com'

url = "http://%s" % url

print
print 'URL: "%s"' % url
print '-----------------------'

remoteFile = urlopen(url)
remoteData = remoteFile.readlines()
remoteFile.close()

for line in remoteData[:numToShow]:
    print line
