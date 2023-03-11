#!/usr/local/bin/python
#!/usr/bin/python

import sys
import os
import string
import db

query =  \
'''
    select count(*) from mrk_marker
'''

db.set_sqlServer  ( "mgi-adhoc")
db.set_sqlDatabase( "mgd")
db.set_sqlUser    ("mgd_public")
db.set_sqlPassword("mgdpub")

queries = string.split(query, '||')

results = db.sql( queries, 'auto')
print results
