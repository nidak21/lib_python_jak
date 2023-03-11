#!/usr/bin/env python

import sys
import string






# This is an automatically generated script to run your query
# to use it you will require the intermine python client.
# To install the client, run the following command from a terminal:
#
#     sudo easy_install intermine
#
# For further documentation you can visit:
#     http://www.intermine.org/wiki/PythonClient

# The following two lines will be needed in every python script:
from intermine.webservice import Service
service = Service("http://www.mousemine.org/mousemine/service")

def getGXD ( symbol	# gene symbol to get GXD annotations for
	):
    '''
	Return a list of rows from MouseMine of GXD annotations from the
	given aspect for the given gene symbol
    '''
    # Get a new query on the class (table) you will be querying:
    query = service.new_query("GXDExpression")

    # The view specifies the output columns
    query.add_view(
	"feature.symbol", "feature.primaryIdentifier", "assayType",
	"structure.identifier", "structure.name", "stage", "strength"
    )

    # You can edit the constraint values below
    query.add_constraint("feature.organism.taxonId", "=", "10090", code = "B")
    query.add_constraint("feature", "LOOKUP", symbol, code = "A")
    query.add_constraint("strength", "!=", "Absent", code = "C")

    # Uncomment and edit the code below to specify your own custom logic:
    # query.set_logic("A and B and C")

    return query.rows()
# end getGXD() ---------------------------

def writeGXD (symbol
		):
    ''' Write a file of GXD annotations for the given gene symbol
    '''
    fileName = "%s_GXD.tsv" % (symbol)
    fp = open( fileName, "w")

    hdr = '\t'.join([ "gene ID",
		      "symbol",
		      "assayTYpe",
		      "term ID",
		      "term",
		      "stage",
		      "strength"])
    fp.write(hdr + '\n')

    for row in getGXD(symbol):
	line = '\t'.join([ \
			    row["feature.primaryIdentifier"],
			    row["feature.symbol"],
			    row["assayType"], \
			    row["structure.identifier"],
			    "%s: %s" %(row["stage"], row["structure.name"]),
			    row["stage"],
			    row["strength"]
			    ])
	fp.write(line + '\n')

    fp.close()
# end writeGXD() ------------------

# Main
symbol = sys.argv[1]
writeGXD(symbol)
