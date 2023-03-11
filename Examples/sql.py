#!/usr/bin/env python2.7
#  Purpose:
#	   run sql to ...
#
#  Outputs:     writes ...
###########################################################################
import sys
import string
import time
import argparse
import db
#-----------------------------------
def getArgs():
    parser = argparse.ArgumentParser( \
                    description='do something')

    parser.add_argument('-s', '--server', dest='server', action='store',
        required=False, default='dev',
        help='db server: adhoc, prod, or dev (default)')

    parser.add_argument('-q', '--quiet', dest='verbose', action='store_false',
        required=False, help="skip helpful messages to stderr")

    args =  parser.parse_args()

    if args.server == 'adhoc':
	args.host = 'mgi-adhoc.jax.org'
	args.db = 'mgd'
    if args.server == 'prod':
	args.host = 'bhmgidb01'
	args.db = 'prod'
    if args.server == 'dev':
	args.host = 'bhmgidevdb01'
	args.db = 'prod'

    return args
#-----------------------------------
SQLSEPARATOR = '||'
QUERY =  \
'''
select a.accid pubmed, bd.haspdf, r.year, r.journal
from bib_refs r join bib_workflow_data bd on (r._refs_key = bd._refs_key)
     join acc_accession a on
	 (a._object_key = r._refs_key and a._logicaldb_key=29 -- pubmed
	  and a._mgitype_key=1 )
where
r.year >= 2010
-- limit 10
'''
#-----------------------------------
def main():
    args = getArgs()

    db.set_sqlServer  ( args.host)
    db.set_sqlDatabase( args.db)
    db.set_sqlUser    ("mgd_public")
    db.set_sqlPassword("mgdpub")

    if args.verbose:
	sys.stderr.write( "Hitting database %s %s as mgd_public\n\n" % \
							(args.host, args.db))
    queries = string.split(QUERY, SQLSEPARATOR)

    startTime = time.time()
    results = db.sql( queries, 'auto')
    endTime = time.time()
    if args.verbose:
	sys.stderr.write( "Total SQL time: %8.3f seconds\n\n" % \
							(endTime-startTime))
    sys.stdout.write( '\t'.join( [			# write header line
	'journal',
	] ) + '\n' )
    for i,r in enumerate(results[-1]):
	sys.stdout.write( '\t'.join( [			# write data line
	    r['journal'],
	    ] ) + '\n' )
	if args.verbose and i % 1000 == 0:	# write progress indicator
	    sys.stderr.write('%d..' % i)
# end main() ----------------------------------
if __name__ == "__main__": main()
