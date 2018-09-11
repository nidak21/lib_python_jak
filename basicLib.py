#
# basic utilities that Jim uses
#
def removeNonAscii(text):
    """ return a string with all the non-ascii characters in text replaced
        by a space.
        Gets rid of those nasty unicode characters.
    """
    return  ''.join([i if ord(i) < 128 else ' ' for i in text])
