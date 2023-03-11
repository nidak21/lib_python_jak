#!/usr/bin/env python3

"""
Convert leading tab chars to spaces.
Leave all other tabs/chars alone.
On linux:   expand -i
    has this functionality. On MacOS, expand does not support the -i option.
Read from stdin, write to stdout.
"""
import sys

SPACES = '        '             # 8 spaces

for line in sys.stdin:
    output = ''
    for i in range(len(line)):

        if line[i] == '\t': output += SPACES
        else:
            output += line[i:]
            break
    sys.stdout.write(output)
