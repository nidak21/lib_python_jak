#!/usr/bin/env python
"""
    Format and Print Python profiler output from cProfile
    To use:
        1) python -m cProfile -o profile.out some_script_Plus_args
        2) printProfile.py profile.out

    See: https://docs.python.org/3/library/profile.html
"""
import argparse
import pstats
from pstats import SortKey

def parseCmdLine():
    parser = argparse.ArgumentParser( \
                description='format a cProfile output file to stdout.',)

    parser.add_argument("filename", help="the cProfile output file")

    return parser.parse_args()

args = parseCmdLine()

p = pstats.Stats(args.filename)
p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
