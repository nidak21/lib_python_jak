
def removeNonAscii(text
return ''.join([i if ord(i) < 128 else ' ' for i in text])
