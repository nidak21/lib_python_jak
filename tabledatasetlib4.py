
#
# This module contains the definitions classes:
#	TableDataSet
#	TextFileTableDataSet
#	TableDataSetBucketizer
#	TableDataSetBucketizerReporter
#
# It also defines functions that operate on TableDataSet's (these would likely
#   be class methods if this were in Java.)

#import ignoreDeprecation
#import sys
#import os
import string
import types

DEBUG = 0

#
# Global functions that operate on TableDataSets and records of TableDataSets
#

def joinOnEq ( \
    joinname,	# the name for the TableDataSet created by the join
    ds1,	# 1st TableDataSet to join
    ds2,	# 2nd TableDataSet
    field1,	# field in ds1 to join on (on equality)
    field2,	# field in ds2 to join on
    ds1Fields,	# list of fields from ds1 to return in the join
    new1Fields,	# list of the new names of those ds1 fields in the join
    ds2Fields,	# list of fields from ds2 to return in the join
    new2Fields	# list of the new names of those ds2 fields in the join
    ):
# Purpose: join TableDataSets ds1 and ds2 for records whose
#		ds1.field1 = ds2.field2
# Returns: return a new TableDataSet
# Assumes: both datasets have an index for field1 and field2 respectively.
#	   ds1Fields are a subset of ds1's fields.
#	   ds2Fields are a subset of ds2's fields.
#	   new1Fields has same length as ds1Fields
#	   new2Fields has same length as ds2Fields
#	   new1Fields and new2Fields do not share any names between them
# Effects: 
# Throws : 
    newFieldNames = new1Fields + new2Fields
    
    newDS = TableDataSet( joinname, newFieldNames)

    for rcd1 in ds1.getRecords():
	value = rcd1[ field1]
	for rcd2 in ds2.getRecordsByIndex( field2, value):
	    newRcd = {}
	    copyfields( newRcd, new1Fields, rcd1, ds1Fields)
	    copyfields( newRcd, new2Fields, rcd2, ds2Fields)
	    newDS.addRecord( newRcd)
    
    return newDS
# end joinOnEq() ----------------------------------

def simpleJoinOnEq (	\
    joinname,	# the name for the TableDataSet created by the join
    ds1,	# 1st TableDataSet to join
    ds2,	# 2nd TableDataSet
    field1,	# field in ds1 to join on (on equality)
    field2	# field in ds2 to join on
    ):
# Purpose: join TableDataSets ds1 and ds2 for records whose
#		ds1.field1 = ds2.field2.
#		Fields for the join are all the fields from ds1 (w/ "1"
#		concatenated on front) and all fields from ds2 (w/ "2"
#		concatenated on front)
# Returns: return a new TableDataSet
# Assumes: both datasets have an index for field1 and field2 respectively
# Effects: 
# Throws : 
    newFieldNames1 = []
    newFieldNames2 = []
    for fn in ds1.getFieldNames():
	newFieldNames1.append( "1" + fn)
    for fn in ds2.getFieldNames():
	newFieldNames2.append( "2" + fn)
    
    return joinOnEq(joinname, ds1, ds2, field1, field2,	\
		    ds1.getFieldNames(),		\
		    newFieldNames1,			\
		    ds2.getFieldNames(),		\
		    newFieldNames2)

# end simpleJoinOnEq() ----------------------------------

def copyfields ( \
    rcd1,		# record to copy to
    fieldNames1,	# list of fieldnames from rcd1 to copy into
    rcd2,		# record to copy from
    fieldNames2		# list of fieldnames form rcd2 to copy from
    ):
# Purpose: copy specified fields in rcd2 to rcd1
# Returns: nothing
# Assumes: len(fieldNames1) == len(fieldNames2)
# Effects: see Purpose
# Throws : 
    for i in range(len(fieldNames1)):
        rcd1[ fieldNames1[i]] = rcd2[ fieldNames2[i]]
    
# end copyfields() ----------------------------------

#
# CLASSES
#
class TableDataSet:
#
# IS: a TableDataSet is a table of records (rows). Each record has the same
#     set of fields (columns).
# 	The value of a field in any record can be any value.
#	Any field can also be specified as a multi-valued field, meaning that
#	  its value for each record is actually a (python) list of values.
#	The main functionality TableDataSet provides is that it maintains
#	  indexes of user specified fields to support:
#	  * rapid lookup (see getRecordsByIndex()),
#	  * finding records with duplicate values (see getKeysForDups()),
#	  * efficient bucketization (see TableDataSetBucketizer class below)
#	  (multi-valued fields are indexed by all the values in the list so
#	   you can look up a record by any of its values)
#	A TableDataSet can be set to be case sensitive (or case insensitive)
#	  meaning that index lookups ignore case (or don't ignore case).
#	Records are python dictionaries whose keys are fieldnnames.
#	  But I've been working on reimplementing records to be TDSRecords
#	     (see class definition below). NOT FINISHED YET
#	Records also have integer keys. Many methods take a key or list of
#	  keys to operate on records.
#	
# HAS: has a (string) name,
#      As discussed above, has:
#	a list of records,
#	a list of field names (strings)
#	a collection of indexes on specific fields
#	  
# DOES: Add records, delete records, and update the fields of records.
#	Add fields. Add or delete indexes on specific fields.
#	get Records or Keys in a variety of ways (w/ or w/o using indexes)
#	  - lists of returned records/keys can be sorted by multiple fields
#	    with std (python) or user passed comparison functions.
#	Write records to output files (sorted or not)
#	Bucketize (see TableDataSetBucketizer class below)
#

    def __init__ (self,
	name,			# printable name of this TableDataSet
	fieldnames,		# list of field names in this TableDataSet
	caseSensitive=0,	# =1 to make indexes case sensitive
	multiValued = {}	# dict mapping fieldnames to delim string
				#   for all multiValued fields
				# Assumes the delim string is not empty.
	):
    # Purpose: constructor
    # Returns: nothing
	self.name = name[:]
	self.fieldnames = fieldnames[:]
	self.multiValuedFields = multiValued.copy()
				# dict whose keys are fieldnames that 
				#   are multiple value fields
				# the value each key is the delim string
				# for the field.

	self.records   = {}	# dict of rcds. Dict keys are record keys
	self.nextkey   = 0	# record key to use for the next record
	self.indexes   = {}	# dict w/ fieldname -> dict w/ value->
				#  list of rcd keys

	self.caseSensitive = caseSensitive	# =1 if indexes and
				#    index lookup are case sensitive

	self.sortField = None	# name of the field to use for sorting in
				#  self.getRecords()
				# =None means printRecords() won't sort.
				# (set on each invocaton of getRecords())
				# see self.sortFunc().

	self.cmpFunc   = None	# name of an optional function to call to
				#  compare two values from self.sortField
				#  for sorting in getRecords()
				# =None means, just use the Python built-in
				#   comparison operators (< > ==)
				# (set on each invocaton of getRecords())
				# see self.sortFunc().

    # end __init__() class TableDataSet ------------------------------

    def selectSubset (self,
	name,			# name of the new TableDataSet to hold the
				#   subset
	rcdKeys			# list of keys from this (self) TableDataSet to
				#  to copy the rcds from to the new TableDataSet
				# =None means copy all rcds
        ):
    # Purpose: select a subset of the TableDataSet records and copy them to a
    #		new TableDataSet.
    # Returns: returns a new TableDataSet object w/ just the specified records.
    # Assumes: nothing
    # Effects: For the rcds copied to the new TableDataSet, the _rcdkey values
    #		will be copied from the old TableDataSet
    #	       No indexes are defined or copied for the new TableDataSet

	newds = TableDataSet(name, self.getFieldNames())

	if rcdKeys == None:
	    rcdKeys = self.records.keys()

	for rcdKey in rcdKeys:
	    rcd = self.records[rcdKey]
	    newRcd = rcd.copy()	  # IMPORTANT make a copy so when
				  #  we set the _rcdkey in newds,
				  #  we don't mess up this TableDataSet's keys
	    newds.records[rcdKey] = newRcd
	
	newds.nextkey = self.nextkey	# start off key generation
	
	return newds
    # end selectSubset() ----------------------------------

    ###############
    # methods for adding/deleting rcds, adding fields
    ###############
    def addRecord (self,
	rcd		# dict whose keys are the fieldnames
        ):
    # Purpose: add 'rcd' to the data set
    # Returns: returns the key of the new rcd
    # Assumes: 
    # Effects: generates a new key
    #	       If rcd is missing any of this DataSet's fields, those fields
    #		  are added to the rcd with a value of None.
    # Warning: The caller should be careful NOT to pass a rcd that is a rcd
    #		from any other TableDataSet. Doing so will cause that rcd
    #	        (dict object) to be on two TableDataSets and rcd['_rcdkey']
    #		will be set to the key of the latest TableDataSet potentially
    #		causing very hairy bugs.
    #	       I debated having this method make a copy of the rcd and adding
    #		 that to the TableDataSet but decided not to do that because
    #		 I wasn't sure about the semantics of the copy (deep or
    #	         shallow?), which would be best in all situations.
    #	       So the responsibility for this rests with the caller.
	
        key = self.newKey()	# get a new key
	for f in self.getFieldNames():	# make sure rcd has all needed fields
	    if not rcd.has_key(f):
		rcd[f] = None
	rcd['_rcdkey'] = key	# add key as an attr
	self.records[key] = rcd
	self.updateIndexesForNewRecord( key)

	return key
    # end addRecord() ----------------------------------

    def deleteRecords (self,
	keys	# list of keys to delete, or a single key to delete
        ):
    # Purpose: delete the records specified by keys
        
	if type(keys) != types.ListType:
	    keys = [keys]

	for key in keys:
	    if self.records.has_key( key):
		self.updateIndexesForDeletedRecord(key)
		del self.records[key]
    # end deleteRecords() ----------------------------------

    def newKey (self):
    # Purpose: return the next key to use for this dataset
    # Returns: int
    # Assumes: 
    # Effects: increments self.nextkey
    # Throws : %%
        key = self.nextkey
	self.nextkey = self.nextkey +1
	return key
    # end newKey() ----------------------------------

    def addField (self,
	fieldname,	# new field to add
	value,		# default value to assign to each existing rcd
	delim=None	# (string) delimiter if this is a multiValued field
			# =None if this is a single valued field
        ):
    # Purpose: add a new field to each record
    # Assumes: fieldname is not already defined as a field for this dataSet
    #	       if delim!=None, then value is a list
    #	       if delim!=None, then delim is a string and NOT the empty string

	self.fieldnames.append(fieldname)
	if delim != None:
	    self.multiValuedFields[fieldname] = delim
        for rcd in self.getRecords():
	    rcd[ fieldname] = value
    # end addField() ----------------------------------

    ###############
    # methods for retrieving records/keys, sorted or not
    ###############
    def selectRecordsWhere (self,
	fieldName,		# field to select on
	value,			# select rcds whose field == this value
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
	):
    # Purpose: return the list of records where fieldName == value
    # Returns: list of records. Optionally sorted by sortField.
    # Assumes: std assumptions about sortField/cmpFunc, see self.sortFunc()
    # Effects: sets self.sortField and self.cmpFunc.
	keys = self.selectKeysWhere(fieldName, value)
	return self.getRecords(keys, sortField, cmpFunc)
    # end selectRecordsWhere() --------------------------------------

    def selectKeysWhere (self,
	fieldName,		# field to select on
	value,			# select rcds whose field == this value
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
	):
    # Purpose: return the list of keys where fieldName == value
    # Returns: list of records. Optionally sorted by sortField.
    # Assumes: std assumptions about sortField/cmpFunc, see self.sortFunc()
    # Effects: sets self.sortField and self.cmpFunc.
	# if we have an index on fieldName and value != None, use index
	# otherwise, scan all records

	if self.indexes.has_key(fieldName) and value != None:
	    return self.getKeysByIndex(fieldName, value, sortField, cmpFunc)
	else:
	    keys = []		# list of keys whose rcd matches
	    for rcd in self.getRecords():
		if (self.isMultiValued( fieldName) \
		    and rcd[fieldName].count(value) != 0) \
		    or rcd[fieldName] == value:
		    keys.append(rcd["_rcdkey"])
	    return self.sortKeys(keys, sortField, cmpFunc)

    # end selectKeysWhere() --------------------------------------

    def getRecordByKey (self,
	key
	):
    # Purpose: return a single record given its key
    # Note: can also just call getRecords(key)
    #       so this method is not needed anymore
        return self.records[key]
    # end getRecordByKey() ----------------------------------
	
    def getRecords (self,
	keys = None,		# optional key or list of keys
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
	):
    # Purpose: return a list of records. If keys==None, return all
    #		records in the TableDataSet, ow return just the specified rcds.
    # Returns: list of specified records. Optionally sorted by sortField.
    #	       If keys are given, and sortField is None, the list of records
    #	         returned will be in the order of the keys given.
    # Assumes: if keys!=None, then every item in keys is a valid rcd key
    # 	       std assumptions about sortField/cmpFunc, see self.sortFunc()
    # Effects: sets self.sortField and self.cmpFunc.

	# short circuit the simple case, no keys given, no sortField
	if keys == None and sortField == None:
	    return self.records.values()
	# short circuit simple case: just an individual key given
	if keys != None and type(keys) != types.ListType:
	    return [self.records[keys]]

        if keys == None:	# no key list specfied
	    keys = self.records.keys()	# return all records
	
	keys = self.sortKeys(keys, sortField, cmpFunc)

	rcds = []
	for k in keys:
	    rcds.append(self.records[k])

        return rcds
    # end getRecords() ----------------------------------
    
    def getRecordsByIndex (self,
	fieldname,		# fieldname to look up
	inputValue,		# find rcds w/ this value
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
        ):
    # Purpose: return list of records whose fieldname = value, optionally
    #		sorted by sortField (if not None)
    # Returns: list of records
    # Assumes: we have an index on fieldname.
    #	       inputValue is not None (we don't index "None" values).
    # 	       Std assumptions about sortField/cmpFunc, see self.sortFunc().
    # Effects: sets self.sortField and self.cmpFunc.
    # Throws : 
	keys = self.getKeysByIndex( fieldname, inputValue)
	return self.getRecords(keys, sortField, cmpFunc)
    # end getRecordsByIndex() ----------------------------------

    def getKeysByIndex (self,
	fieldname,		# fieldname to look up
	inputValue,		# find rcds w/ this value
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
        ):
    # Purpose: return list of records whose fieldname = value
    # Returns: list of records, if sortField!=None, these will be sorted
    #		by sortField and cmpFunc as specified.
    # Assumes: we have an index on fieldname.
    #	       inputValue is not None (we don't index "None" values)
    # 	       Std assumptions about sortField/cmpFunc, see self.sortFunc().
    # Effects: sets self.sortField and self.cmpFunc.
    # Throws : 
	indexValue = self.getIndexValue( inputValue)

	if self.indexes[ fieldname].has_key( indexValue):	# have the key
	    return self.sortKeys( self.indexes[ fieldname][indexValue][:], \
				  sortField, cmpFunc)
	else:
	    return []
    # end getKeysByIndex() ----------------------------------
    
    def sortKeys (self,
	keys,			# list of keys whose rcds should be sorted
	sortField = None,	# optional field or field list to sort by
	cmpFunc = None		# optional cmp function or function list for
				#   comparing 2 values in sortField
	):
    # Purpose: Sort the keys based on sortField and cmpFunc.
    # Returns: list; the list of sorted keys or just return input keys
    #		if sortField==None
    # Assumes: every item in keys is a valid rcd key
    # 	       Std assumptions about sortField/cmpFunc, see self.sortFunc().
    # Effects: sets self.sortField and self.cmpFunc.

	if sortField != None:		# have a sort field
	    self.sortField = sortField	# fieldname used by self.sortFunc
	    self.cmpFunc   = cmpFunc
	    keys = keys[:]
	    keys.sort(self.sortFunc)
	else:
	    self.sortField = None
	    self.cmpFunc   = None
	return keys
    # end sortKeys() ----------------------------------

    def sortFunc(self,
	k1,		# key of first rcd to compare
	k2		# key of second rcd to compare
	):
    # Purpose: sort comparison function for comparing two records, given their
    #	       keys: k1, k2
    # Returns: -1, 0, or 1 based on the record comparison
    # Assumes: self.sortField & self.cmpFunc are both singletons or parallel
    #		lists. The singletons or list elements are assumed to be:
    #		sortField: is the name of the field to compare.
    #	       	cmpFunc:   is None or is bound to a function that takes
    #		           two values from sortField and returns -1, 0, or 1
    #		if sortField is a multivalued field, then cmpFunc must not
    #		   be None and must be a function that can compare two
    #		   value lists.
    # Effects: Using the self.sortField self.cmpFunc singletons or lists,
    #	       (in the order given) compares the sortFields of the two records
    #	       using respective cmpFunc if not None or Python cmp() otherwise
    # 	       until it finds fields < or > (and returns that),
    #	       or if all fields compare ==, returns ==.

	if type(self.sortField) != types.ListType:	# convert to lists
	    sortFieldList = [self.sortField]
	    cmpFuncList   = [self.cmpFunc]
	else:
	    sortFieldList = self.sortField
	    cmpFuncList   = self.cmpFunc

	# iterate through the sortFields/cmpFuncs lists.
	# the 1st one we find that gives < or >, return -1,+1 respectively,
	# if they are ==, keep looping to the next sortField
	# if we fall out of the loop, then all comparisons return == (0),
	#   so we return 0.
	for i in range(len(sortFieldList)):
	    sortField = sortFieldList[i]
	    cmpFunc   = cmpFuncList[i]

	    v1 = self.records[k1][sortField]
	    v2 = self.records[k2][sortField]
	    if cmpFunc != None:
		cmpVal =  cmpFunc(v1, v2)
	    else: 
		cmpVal =  cmp(v1, v2)
		# JIM: should raise an exception if sorfField is multiValued
		#   and cmpFunc is None?

	    if cmpVal != 0:
		return cmpVal
	    # else cmpVal == 0, the vals are equal for this field,
	    # so loop to next field to compare or drop out of loop

	return 0
    # end sortFunc() -----------------------------------

    ###############
    # methods for maintaining indexes.
    ###############

    def buildIndexes (self,
	fieldnames	# list of field names to build indexes for
			#  or a single string = name of a field
			# or [] to delete/remove all indexes
	):
    # Purpose: build indexes for the specified fieldnames, throwing away
    #		any existing indexes (if any)
    # Returns: nothing

	self.indexes = {}
	self.addIndexes( fieldnames)

    # end buildIndexes() ----------------------------------

    def addIndexes (self,
	fieldnames	# list fieldnames to add indexes for
			#  or a single string = name of a field
        ):
    # Purpose: (re)build indexes for the specified fieldnames.
    #		(don't touch other indexes)

	# set fields to the list of fieldnames to add indexes for
	fields = fieldnames
	if type(fieldnames) != types.ListType:
	    fields = [fieldnames]

        for fn in fields:
	    self.indexes[fn] = {}
	    for key in self.records.keys():
		self.updateIndexForNewRecord( fn,key)
    # end addIndexes() ----------------------------------
    
    def hasIndex (self,
	fieldname	# fieldname to check index for
        ):
    # Purpose: return 1 if we have an index for the specified fieldname
    # Returns:  0 otherwise

	return fieldname in self.indexes.keys()
    
    # end hasIndex() ------------------------------------------

    def getIndexValue (self,
        inputValue	# the value to convert to an index value
        ):
    # Purpose: convert 'inputValue' to one suitable as a key for an index.
    # Returns: string - the converted value (or None if inputValue == None)
    # Effects: converted values are always strings.
    #		If self.caseSensitive == 0, converted value is all lower case.
	if inputValue == None:
	    return None

	if self.caseSensitive:
	    value = str(inputValue)	# convert to string
	else:
	    value = str(inputValue).lower() # all lower case
	
	return value
    # end getIndexValue() ----------------------------------

    def updateIndexesForNewRecord (self,
        key
        ):
    # Purpose:  update all existing indexes for the specified record key
	for fn in self.indexes.keys():
	    self.updateIndexForNewRecord( fn,key)
    # end updateIndexesForNewRecord() ----------------------------------

    def updateIndexForNewRecord (self,
	fieldname,	# fieldname for the index
	key		# the rcd key
        ):
    # Purpose: update the index for 'fieldname' for the new record whose
    #		key is 'key'
    # Returns: nothing
    # Assumes: there is an index on 'fieldname'
    #	       key is a valid rcd key (i.e., the rcd has been added already)
    #	       If fieldname is a multValued field, its value in the rcd is
    #		a list.
    # Effects: for index 'fieldname',
    #		if the value of 'fieldname for rcd w/ 'key' is not None,
    #		    add 'key' to the list of keys w/ that value

        rcd = self.records[key]
	if self.isMultiValued(fieldname):
	    values = rcd[fieldname]
	else:	# single valued field, but value into the values list
	    values = [ rcd[fieldname] ]

	for val in values:

	    if val != None:	# we don't index "None" as a value
		value = self.getIndexValue( val)
		if self.indexes[fieldname].has_key( value):
		    # add key to list
		    self.indexes[fieldname][value].append( key)
		else:
		    # start list
		    self.indexes[fieldname][value] = [ key]

    # end updateIndexForNewRecord() ----------------------------------

    def updateIndexesForDeletedRecord (self,
        key		# the rcd key to be deleted
        ):
    # Purpose: update all indexes in preparation for deleting the rcd w/ 'key'
    # Returns: nothing
    # Assumes: key is still a valid key (i.e., rcd has not been deleted yet)


	for fn in self.indexes.keys():
	    rcd = self.records[key]
	    self.updateIndexForDeletedRecord( fn, key, rcd[fn])
    # end updateIndexesForDeletedRecord() ----------------------------------
    
    def updateIndexForDeletedRecord (self,
	fieldname,	# string; fieldname whose index should be updated
	key,		# key of the rcd to be deleted
	oldValue	# old value of the fieldname for the record.
        ):
    # Purpose: update the index for the specified field in preparation for
    #		deleting the rcd w/ 'key'
    # Returns: nothing
    # Assumes: there is an index on the specified fieldname
    #	       If fieldname is a multValued field, oldValue is a list of values.
    # Effects: for the 'fieldname' index, remove 'key' from the list of
    #		rcds with value 'oldValue'

        rcd = self.records[key]
	if self.isMultiValued(fieldname):
	    values = oldValue
	else:	# single valued field, but value into the values list
	    values = [ oldValue ]

	for val in values:
	    if val != None:
		value = self.getIndexValue( val)
		if self.indexes[fieldname].has_key( value):
		    self.indexes[fieldname][value].remove( key)
		    if len(self.indexes[fieldname][value]) == 0: #last one
			del self.indexes[fieldname][value]

    # end updateIndexForDeletedRecord() ----------------------------------
    
    def updateFields (self,
	key,		# key of the record to update
	fieldNames,	# string field name or list of field names
			#   whose values to update
	values		# single value or list of values to set
			#  (parallel to fieldNames)
        ):
    # Purpose: For the record w/ the given key, set the values of the specified
    #	        fieldNames to the specified values. Update existing indexes
    #		accordingly
    # Returns: nothing
    # Assumes: fieldNames and values are both singletons or parallel lists.
    #	       For each field that is multiValued, the corresponding value is
    #		a list of values.
    # Effects: see Purpose
	if type(fieldNames) == types.StringType:
	    fieldNames = [fieldNames]
	    values     = [values]

        rcd = self.records[key]
	for fieldName in fieldNames:
	    oldValue = rcd[ fieldName]
	    newValue = values[0]	# 1st value
	    values = values[1:]		# remove 1st value

	    rcd[fieldName] = newValue	# set new value for the field

	    if fieldName in self.indexes.keys():	# field is indexed
		# first remove index entry for oldValue
		self.updateIndexForDeletedRecord( fieldName, key, oldValue)

		# add index entry for newValue
		self.updateIndexForNewRecord( fieldName, key)

    # end updateFields() ----------------------------------
    
    ###############
    # methods for detecting duplicate records (on certain fields)
    ###############
    def getKeysForDups (self,
	fieldname,	# field to look for dup values in
	omitFirst=0	# =1 means in the list of keys returned
		    	#   omit the key of the 1st rcd for each value
        ):
    # Purpose: return list of keys of records that have duplicate
    #		values for 'fieldname'
    #		- potentially omitting 1st rcd for each value
    # Returns: see purpose
    # Assumes: there is an index on 'fieldname'
    # Example: if you want to delete all rcds w/ duplicate values in "ID" field
    #		    ds.deleteRecords( ds.getKeysForDups("ID"))
    #
    # 	       if you want to delete all rcds w/ duplicate values in "ID" field
    #	       EXCEPT the first rcd w/ each value:
    #		    ds.deleteRecords( ds.getKeysForDups("ID", omitFirst=1))

        keys = []
	dups = self.getDupsDict( [fieldname])
	for value in dups[fieldname].keys():
	    if omitFirst:
		keys = keys + dups[fieldname][value][1:]
	    else:
		keys = keys + dups[fieldname][value]
	
	return keys
    # end getKeysForDups() ----------------------------------
    
    def getDupsDict (self,
	fieldnames	# list of fieldnames to report dups for
        ):
    # Purpose: return a data structure containing all records with
    #		duplicate values for the specified fieldnames.
    # Returns: dict[fieldname] -> dict [value] -> list of rcd keys
    #		with 'value' for that 'fieldname'
    # Assumes: we have indexes on all the fields in fieldnames
	finaldict = {}
	for fn in fieldnames:
	    finaldict[fn] = {}
	    for value in self.indexes[fn].keys():
		if len( self.indexes[fn][value]) > 1:
		    finaldict[fn][value] = self.indexes[fn][value][:]

	return finaldict
    # end getDupsDict() ----------------------------------

    ###############
    # printing methods
    ###############

    def printRecords (self,
	fp,			# open output file to write to
	fieldnames=None,	# list of fields to print
				# =None, print self.getFieldNames()
	keys = None,		# optional key or list of keys
	delim = '\t',		# optional field delim string
	sortField = None,	# optional field or field listto sort by
	cmpFunc = None,		# optional cmp function or funct list for
				#   comparing 2 values in sortField
	headerline = 'n',	# ='y' to print a header line w/ field names
        ):
    # Purpose: print the specified records to fp. If sortField is specified
    #		the rcds are sorted by that field. cmpFunc is used to compare
    #		the values in that field (if cmpFunc==None, use default cmp)
    # Returns: nothing
    # Assumes: if keys!=None, then every item in keys is a valid rcd key
    # Effects: sets self.sortField and self.cmpFunc.

	encoding = 'utf-8'		# Unicode encoding to use,
					# needs to become an instance var
	if fieldnames == None:
	    fieldnames = self.getFieldNames()

	if headerline == 'y':
	    self.printHeaders(fp, fieldnames, delim)

	last = len( fieldnames) -1	# index of last field to print
	for rcd in self.getRecords(keys, sortField, cmpFunc):
	    i = 0			# index of field to print
	    fieldDelim = delim		# the delim to write after a field
	    while i < len( fieldnames):

		fn = fieldnames[i]
		if self.isMultiValued( fn):
		    # this is what we did pre-Unicode issues (1/25/2012)
		    #values = map(str, rcd[fn]) # convert all values to strings
		    #value = string.join( values, self.multiValuedFields[fn])

		    values = []		# list of values, converted to Unicode
		    for v in rcd[fn]:
			if type(v) != unicode:		# convert it
			    v = str(v).decode( encoding)
			values.append( v )

		    # now, we can join all the unicode values
		    value = string.join( values, self.multiValuedFields[fn])
		else:	# single valued field
		    value = rcd[fn]
		    if value == None:
			value = ''
		if i == last:
		    fieldDelim = '\n'	# use EOL for last field printed
		if type(value) == unicode:	#must encode it first
		    value = value.encode( encoding)
		fp.write("%s%s" % (value, fieldDelim))

		i = i+1
	    
    # end printRecords() ----------------------------------

    def printHeaders (self,
	 fp,			# open output file to write to
	 fieldnames=None,	# list of fieldnames to write
				# =None, use self.getFieldNames()
	 delim = '\t'		# (string) delimiter between field names
        ):
    # Purpose: write the specified 'fieldnames' as a 'delim'ited header
    #		line to 'fp'
    # Returns: nothing
    # Assumes: fp is open for output.

	if fieldnames == None:
	    fieldnames = self.getFieldNames()

        for fn in fieldnames[:-1]:
	    fp.write( fn + delim)
	
	fp.write( fieldnames[-1] + '\n')
    # end printHeaders() ----------------------------------

    ###############
    # Simple getters and setters
    ###############

    def getValues (self, fieldname):
    # Purpose: return the list of distinct values for 'fieldname' in this
    #	       TableDataSet
    # Assumes: there is an index for 'fieldname'
        return self.indexes[fieldname].keys()
    # end getValues() ----------------------------------

    def getKeys (self):
    # Purpose: return the list of rcd keys in this TableDataSet
        return self.records.keys()
    # end getKeys() ----------------------------------

    def getFieldNames (self):
    # Purpose: return the list of fieldnames
        return self.fieldnames[:]
    # end getFieldNames() ----------------------------------
    
    def isMultiValued (self,
		       fieldname
	):
    # Purpose: return true if the fielnameis multiValued, false otherwise
        return self.multiValuedFields.has_key( fieldname)
    # end isMultiValued() ----------------------------------
    
    def getName (self):
    # Purpose: return the name (string)
        return self.name	# JIM return a copy?
    # end getName() ----------------------------------
    
    def setName (self,
	 name
        ):
    # Purpose: set the name (string)
        self.name = name
    # end setName() ----------------------------------
    
    def getNumRecords (self):
    # Purpose: return the number of records in the TableDataSet (int)
        return len(self.records)
    # end getNumRecords() ----------------------------------
    
    def getCaseSensitive (self):
    # Purpose: return the case Sensitivity value
    # Returns: =1 indexes are case Sensitivity, =0 index are case insensitive
        return self.caseSensitive	# JIM return a copy?
    # end getCaseSensitive() ----------------------------------
    
    def setCaseSensitive (self,
	caseSensitive		# = 1 to set case sensitive, =0 otherwise
	):
    # Purpose: set the case Sensitivity value
	self.caseSensitive = caseSensitive
    # end setCaseSensitive() ----------------------------------
    
# End class TableDataSet ------------------------------------------------------

class TDSRecord:
#
# WORK IN PROGRESS, NOT USED YET
#
# IS: a record in a TableDataSet.
#
# Has: is associated with a specific TableDataSet instance.
#      Has all the fields of that TableDataSet.
#      Has a key in that TableDataSet
#
# Does:
#	Allows you to set and get at the field values by treating the
#	  record as a python dictionary.
#	As you set field values, TableDataSet indexes are updated.
#
# Usage Pattern:
#	rcd = fooDataSet.getNewRecord() # get an empty record w/ a key assigned
#	rcd['field1'] = 'hi'		# updates index if field1 is indexed
#	rcd['field2'] = 'there'		# updates index if field2 is indexed
#	

    def __init__ (self,
	tableDataSet,		# the TableDataSet this TDSRecord is a record in
	key			# the key for this record in the TableDataSet
	):
    # Purpose: TDSRecord constructor
    #	       USERS SHOULD NOT CALL THIS CONSTRUCTOR DIRECTLY
    #	       INSTEAD, use  fooTableDataSet.getNewRecord()
	self.tableDataSet = tableDataSet
	self.key = key		# this record's key
	self.dict = {}		# the dict mapping fieldnames to values
	for fn in tableDataSet.getFieldNames():
	    self.initializeField(fn)
    # end __init__() class TDSRecord ------------------------------

    def addField (self,
	fn	# (string) name of the new field to add
	):
    # Purpose: add the specified field to this record
    # Returns: nothing
    # Assumes: nothing
    # Effects: sets the default value for the field
	self.initializeField(fn)

    # end addField() ---------------------------------------------
    
    def initializeField (self,
	fn	# (string) name of the field to initialize
	):
    # Purpose: initialize the specified field to its default value
    # Returns: nothing
    # Assumes: nothing
    # Effects: the default value for single valued fields is None, for
    #		multi valued fields is [] (empty list)
	if self.tableDataSet.isMultiValued(fn):
	    self.dict[fn] = []	# empty list
	else:
	    self.dict[fn] = None

    # end initializeField() ---------------------------------------------
    
    def __setattr__ (self,		# __setitem__ ??
	fn,	# (string) name of the field to set
	value	# the value to set
	):
    # Purpose: sets the specified field to the value
    # Returns: nothing
    # Assumes: nothing
    # Throws:  KeyError if 'fn' is not a field for this dataset
    # Effects: updates self.tableDataSet's index for 'fn' if it exists
	if self.dict[fn]:	# raise KeyError if field not defined
	    pass
	if self.tableDataSet.isMultValued(fn):
	    self.dict[fn].append(value)
	else:
	    self.dict[fn] = value

	self.tableDataSet.updateIndex(self.key, fn, value)

    # end __setattr__() ---------------------------------------------
# JIM HERE	    

# End class TDSRecord ---------------------------------------------------

class TextFileTableDataSet (TableDataSet):

    def __init__ (self,
	name,			# string;the name for this TableDataSet.
	filename,		# string; name of the file to read.
	fieldnames=None,	# list of fields (strings) in the file- in order
				# =None means get fieldnames from the input
				#  file (last hdr line assumed to be col names)
	multiValued = {},	# dict mapping fieldnames to delim string
				#   for all multiValued fields
				# Assumes the delim string is not empty.
	numheaderlines=1,	# number of header lines at top of file to skip
				#   if fieldnames==None, assume the last
				#   header line has column names
	fieldDelim='\t',	# the field delimiter in the input file
	ignoreComments=0,	# =1 to ignore comments & blank lines as
				#    we read the input file
	readNow=0,		# =1 to read the file on instantiation.
	caseSensitive=0		# =1 to make indexes case sensitive
	):
    # Purpose: constructor
    # Assumes: if fieldnames==None, then numheaderlines>0

	self.filename       = filename
	self.numheaderlines = numheaderlines
	self.fieldDelim          = fieldDelim
	self.ignoreComments = ignoreComments	# =1 to skip over blank
						# lines or comment lines
						# in the input file
						# (comments begin w/ #)
	self.fp	= None			# filepointer for reading
					# =None if file is not open and
					#  file header not read.

	self.numLinesRead = 0		# number of lines read from the file
	self.numHeaderLinesRead = 0	# number of header lines read 

	if fieldnames == None:	# get fieldnames from file header
	    # should raise exception if numheaderlines == 0
	    # should check that fieldnames are unique and raise exception
	    hdrLine = self.readHeader()
	    fieldnames = self.splitLine(hdrLine)

	TableDataSet.__init__(self,name,
				fieldnames,
				multiValued=multiValued,
				caseSensitive=caseSensitive)
	if readNow:
	    self.readRecords()

    # end __init__() class TextFileTableDataSet ------------------------------

    def readHeader (self):
    # Purpose: read the header lines from the file
    # Returns: string; last header line.
    # Assumes: self.filename is set and the file is not open
    # Effects: opens the file, sets self.fp, reads self.numheaderlines,

	self.fp = open( self.filename, 'r')
	self.numLinesRead = 0
	line = None

	for i in range(self.numheaderlines):	# read header lines
	    line = self.fp.readline()
	    self.numLinesRead = self.numLinesRead +1

	self.numHeaderLinesRead = self.numLinesRead
	
	return line
    # end readHeader() ---------------------------------------------

    def readRecords (self):
    # Purpose: read the records from the file, if not already read
    # Returns: nothing
    # Assumes: self.records is an empty dictionary

	if self.fp == None:		# not open yet, header not read yet
	    self.readHeader()

	line = self.fp.readline()
	while (line != ""):
	    self.numLinesRead = self.numLinesRead +1
	    if self.keepLine(line):
		rcd = self.parseLine( line)
		rcd["_linenumber"] = self.numLinesRead
		rcd = self.processRecord( rcd)
		if (rcd != None):
		    self.addRecord( rcd)

	    line = self.fp.readline()
	
	self.fp.close()
	self.doneReading()

        return
    # end readRecords() ----------------------------------

    def keepLine (self,
	line		# string holding the current line read in
        ):
    # Purpose: determine if this line is a comment or blank line that
    #		should be skipped as we process the lines of the Text File.
    #	       COULD OVERRIDE this method to detect other syntaxes for comments
    # Returns: =1 to keep the line, =0 to through the line away
    # Assumes: self.ignoreComments is set
    # Effects: If self.ignoreComments is false, just return 1
    #	       if self.ignoreComments is true, return 1 unless
    #	         the first non-blank on the line is "#" or if the line is
    #	         blank
    # Throws : nothing
        if self.ignoreComments:
	    # see if the line is blank or starts w/ "#"
	    trimmedLine = string.strip(line)
	    if len(trimmedLine) == 0 or trimmedLine[0] == "#":
		return 0

	return 1
    # end keepLine() ----------------------------------
    
    def splitLine(self,
	line		# line to split into fields based on self.fieldDelim):
	):
    # Purpose: split the 'line' into fields
    # Returns: list, containing the fields on this line
    # Assumes: nothing
    # Effects: special processing for the last field on the line:
    #  remove trailing \r and \n's so that they do not become part of the
    #  field's value
    # Could use rtrim (or trim in general for all fields) but there might
    #   cases where spaces within a field are a significant part of the
    #   value.

	global DEBUG

        fieldvalues = string.split( line, self.fieldDelim)
	if DEBUG:
	    print fieldvalues

	while len(fieldvalues[-1]) != 0 and ( \
		fieldvalues[-1][-1] == "\n" or fieldvalues[-1][-1] == "\r"):
	    # last field has '\n' or '\r'
	    fieldvalues[-1] = fieldvalues[-1][0:-1]	# remove it

	return fieldvalues
    # end splitLine() -------------------------------------------------

    def parseLine (self, line
        ):
    # Purpose: parse the 'line', map its self.fieldDelim fields to the fields
    #		of this TableDataSet.
    # Returns: new record: a dictionary mapping fieldnames to values from the
    #		  line
	rcd = {}	# empty dictionary to return

        fieldvalues = self.splitLine(line)

	# loop through the fieldnames, grabbing cooresponding values.
	#  remove fieldvalue[0] as we go, so fieldvalue[0] is always the value
	#  for the next field.
	for fieldname in self.fieldnames:

	    defaultValue = None 	# default value for this field
	    if self.isMultiValued( fieldname):
		defaultValue = []

	    if (len(fieldvalues) > 0):  # have a col for the field on the line
		value = fieldvalues[0]
		if self.isMultiValued( fieldname):
		    value = self.parseMultiValue( fieldname, value)
		elif value == '':	# value is empty string
		    value = defaultValue
		del fieldvalues[0]
	    else:			# we've run out of cols on this line
	        value = defaultValue	# use default value

	    rcd[fieldname] = value
	return rcd
	
    # end parseLine() ----------------------------------

    def parseMultiValue (self, 
	fieldname, 		# (string) the name of the field being parsed
	value			# (string) the chars between the field delims
        ):
    # Purpose: parse the 'value', splitting it based on the field's delimiter.
    # Returns: a list of trimmed, non-empty strings that are the delimited
    #		values for the field.

	unstrippedValues = string.split( value,
				        self.multiValuedFields[ fieldname])
	values = []		# the values to return
	for s in unstrippedValues:
	    s = s.strip()
	    if s != '':		# non-empty, so add it to the list
		values.append(s)
	
	return values
    # end parseMultiValue() ----------------------------------

    #####################
    # Simple getters and setters
    #####################
    def setIgnoreComments(self,
	value		# =1 to ignore comments lines as we read input file
			#   (and blank lines)
			# =0 to not ignore.
	):
    # Purpose: determine whether to ignore comment and blank lines as we
    #	         read the input file
    # Returns: nothing
	self.ignoreComments = value
    # end ignoreComments() -----------------------------------------

    def getFilename(self):
    # Purpose: return the filename
    # Returns: string
	return self.filename
    # end getFilename()-----------------------------------

    def getNumLinesRead(self):
    # Purpose: return the number of lines read from the file
    # Returns: string
	return self.numLinesRead
    # end getNumLinesRead()-----------------------------------

    def getNumHeaderLinesRead(self):
    # Purpose: return the number of lines read from the file
    # Returns: string
	return self.numHeaderLinesRead
    # end getNumHeaderLinesRead()-----------------------------------

    #####################
    # ABSTRACT methods that you might want to override
    #####################
    def processRecord (self, rcd
        ):
    # Purpose: abstract method for subclasses to do any processing for
    #		an individual record before it is added to the rcd list
    # Returns: the updated (or unchanged) rcd. Return None if you do not
    #		want to include the rcd in the rcd list.
        return rcd
    # end processRecord() ----------------------------------

    def doneReading(self):
    # Purpose: abstract method for subclasses to do any post processing
    #		after all lines of the file have been read
	return
    # end doneReading() ------------------------------
    
# End class TextFileTableDataSet -------------------------------------------

class TableDataSetBucketizer:
# IS:   an object that knows how to "bucketize" two TableDataSets
#
# HAS:  %%
#
# DOES: %%

    def __init__ (self,
	ds1,		# 1st TableDataSet
	fieldNames1,	# names of fields in ds1 to match
			#  against fields in ds2
	ds2,		# 2nd TableDataSet
	fieldNames2	# parallel to fieldNames1
	):
    # Purpose: constructor
    # Returns: nothing
    # Assumes: 'fieldNames1' are valid fieldnames for ds1 and have indexes.
    #	       'fieldNames2' are valid fieldnames for ds2 and have indexes.
    #	       len(fieldNames1) == len(fieldNames2)
    # Effects: see Purpose
    # Throws : %%

	self.ds1         = ds1
	self.fieldNames1 = fieldNames1
	self.ds2         = ds2
	self.fieldNames2 = fieldNames2
	self.key1Hash  = {}	# dict mapping each ds1 rcd key to the
				#  BucketItem containing that ds1 rcd
	self.key2Hash  = {}	# dict mapping each ds2 rcd key to the
				#  BucketItem containing that ds2 rcd

				# self.bucketItems is the current set of
				#   all unmerged BucketItems implemented
				#  as a dictionary.
	self.bucketItems = {}	# dict mapping BucketItem Id to BucketItem
	self.idCounter	 = 0	# counter for assigning BucketItem Ids

	self.b0_1	= []	# list of ds2 rcd keys in the 0:1 bucket
	self.b1_0	= []	# list of ds1 rcd keys in the 1:0 bucket
	self.b1_1	= []	# list of (ds1,ds2) rcd key pairs in the 1:1
				#  bucket
	self.b1_n	= []	# list of (ds1 key, [ds2 keys]) pairs
				#   in the 1:n bucket
	self.bn_1	= []	# list of ([ds1 keys], ds2 key) pairs
				#   in the n:1 bucket
	self.bn_m	= []	# list of ([ds1 keys], [ds2 keys]) in the
				#      n:m bucket

    # end __init__() class TableDataSetBucketizer ------------------------------
    
    def run (self):
    # Purpose: run the bucketizing algorithm, create the buckets
    # Returns: %%
    # Assumes: %%
    # Effects: %%
    # Throws : %%
	
	# initialize BucketItems (one for each ds1 node and ds2 node)
	# and the keyHash

	for rcd1Key in self.ds1.getKeys():
	    self.key1Hash[ rcd1Key] = self.getNewBucketItem([rcd1Key], [])
		
	for rcd2Key in self.ds2.getKeys():
	    self.key2Hash[ rcd2Key] = self.getNewBucketItem([], [rcd2Key])

	# iterate through the field arrays, looking for rcds to associate
	for i in range( len(self.fieldNames1)):	# for each fieldname 1/2 pair
	    fn1 = self.fieldNames1[i]
	    fn2 = self.fieldNames2[i]

	    for val in self.ds1.getValues(fn1):	# for each value in ds1
		keys2 = self.ds2.getKeysByIndex(fn2, val)
		if len(keys2) > 0:		# that value is in ds2 too
		    keys1 = self.ds1.getKeysByIndex(fn1, val)
		    for k1 in keys1:		# for each pair...
			for k2 in keys2:	#  ...(r1,r2) w/ this value

			    # merge BucketItems containing r1 and r2
			    r1BucketItem = self.key1Hash[ k1]
			    r2BucketItem = self.key2Hash[ k2]
			    self.mergeBucketItems( r1BucketItem, r2BucketItem )

	# iterate through all the BucketItems to populate the "buckets"
	self.partitionBuckets()
        
    # end run() ----------------------------------
    
    def getNewBucketItem(self,
	keys1,		# list of keys from ds1 to put into this BucketItem
	keys2		# list of keys from ds2 to put into this BucketItem
	):
    # Purpose: instantiates a new BucketItem
    # Returns: returns the new BucketItem
    # Assumes: 
    # Effects: adds the new BucketItem to our set of BucketItems
    # Throws : 
	bi = BucketItem(self.idCounter, keys1, keys2)
	self.bucketItems[ self.idCounter] = bi

	self.idCounter = self.idCounter +1

	return bi

    # end getNewBucketItem() ----------------------------------------

    def mergeBucketItems(self,
	bi1,	# BucketItem
	bi2	# BucketItem
	):
    # Purpose: merge the two BucketItems into one and return the result
    # Returns: merged BucketItem
    # Assumes: 
    # Effects: merges bi2 into bi1 and deletes bi2 from the BucketItem set
    #	       Also updates self.key[12]Hash's for each node in the merged BI.
	
	if bi1 != bi2: 	# distinct BucketItems, merge them
	    for k1 in bi2.getDs1Nodes():	# for each Ds1Node in bi2
		self.key1Hash[k1] = bi1		#  update its dictentry
						#  to point to merged BI

	    for k2 in bi2.getDs2Nodes():	# for each Ds2Node in bi2
		self.key2Hash[k2] = bi1		#  update its dictentry
						#  to point to merged BI

	    bi1.merge(bi2)			# merge bi2 into bi1
	    del self.bucketItems[ bi2.id]	# remove bi2 from cur set

	return bi1
    # end mergeBucketItems() ----------------------------------------
    	
    def partitionBuckets(self):
    # Purpose: partition all the BucketItems into the 1:0, 0:1, 1:1, 1:n
    #	   n:1 and n:m buckets.
    # Returns: nothing

	for bi in self.bucketItems.values():
	    bt = bi.getBucketType()

	    if   bt == "1:0":
		self.b1_0.append(bi.getDs1Nodes()[0])
	    elif bt == "0:1":
		self.b0_1.append(bi.getDs2Nodes()[0])
	    elif bt == "1:1":
		self.b1_1.append( \
			(bi.getDs1Nodes()[0],bi.getDs2Nodes()[0]))
	    elif bt == "1:n":
		self.b1_n.append( \
			(bi.getDs1Nodes()[0],bi.getDs2Nodes()))
	    elif bt == "n:1":
		self.bn_1.append( \
			(bi.getDs1Nodes(),bi.getDs2Nodes()[0]))
	    elif bt == "n:m":
		self.bn_m.append( \
			(bi.getDs1Nodes(),bi.getDs2Nodes()))

    # end partitionBuckets() ----------------------------------------
    	
    def getBucketType(self,
	ds,		# one of the 2 TableDataSets this bucketizer is for
	key		# the key of one of the records in ds
	):
    # Purpose: return the type of the bucket the specified record is in.
    # Returns: (string) "1:0" "0:1" "1:1" "1:n" "n:1" or "n:m"
    #		 depending on which bucket the record is in.
    # Assumes: self.run() has been called??

	if ds == self.ds1:		# key is for rcd in 1st dataset
	    bi = self.key1Hash[key]
	elif ds == self.ds2:		# key is for rcd in 2nd dataset
	    bi = self.key2Hash[key]
	else:	# raise exception?
	    pass

	return bi.getBucketType()

    # end getBucketType() ----------------------------------------
    	
    def getBucketCohorts(self,
	ds,		# one of the 2 TableDataSets this bucketizer is for
	key		# the key of one of the records in that ds
	):
    # Purpose: return the keys of all records associated w/ 'key' through
    #		the bucketization process
    # Returns: tuple consisting of two lists ([...], [...])
    #		The 1st list is the list of keys of associated records from
    #		    the first dataset
    #		The 2nd list is the list of keys of associated records from
    #		    the second dataset
    #		The input parameter 'key' is not included in these lists.
    #		Either of the two lists may be empty.
    # Assumes: self.run() has been called

	if ds == self.ds1:		# key is for rcd in 1st dataset
	    bi = self.key1Hash[key]
	elif ds == self.ds2:		# key is for rcd in 2nd dataset
	    bi = self.key2Hash[key]
	else:	# raise exception?
	    pass
	
	list1 = bi.getDs1Nodes()[:]
	list2 = bi.getDs2Nodes()[:]

	if ds == self.ds1:		# key is for rcd in 1st dataset
	    list1.remove(key)		#  remove input key
	else: 				# key is for rcd in 2nd dataset
	    list2.remove(key)		#  remove input key

	return (list1, list2)

    # end getBucketCohorts() ----------------------------------------
    	
    def updateDataSet(self,
	ds,			# one of the 2 TableDataSets this bucketizer
				#     is for
	newBkTypeField,		# string: name of the new field to hold the
				#    bucket type string ("0:1", "1:0", ...)
	newOtherCohortsFlds,	# list of strings, the names of new fields to
				#   add to hold values from associated bucket
				#   cohorts from *other* dataset in this
				#   bucketizer.
				#   (may be empty list)
	otherCohortsFlds,	# list of strings, the names of existing fields
				#   in the *other* dataset to take values from
				#    cohorts & populate the newOtherCohortsFlds
				#    with. (parallel to newOtherCohortsFlds)
	newThisCohortsFlds=[],	# list of strings, the names of new fields to
#	  = [],			#   add to hold values from associated bucket
				#   cohorts from this dataset (ds)
				#   (may be empty list)
	thisCohortsFlds=[],	# list of strings, the names of existing fields
#	  = [],			#   in ds to take values from cohorts & 
				#   populate the newThisCohortsFlds with
				# (parallel to noThisCohortsFlds)
	multiValued={}		# dict specifying which newOtherCohortsFlds &
				#  newThisCohortsFlds are multi-valued fields.
				#  if a field is to be multi-valued, this dict
				#  should map the fieldname to the field
				#  delimiter string. E.g., {"new field" : ","}
	):
    # Purpose: update 'ds' with the results of this bucketization by adding
    #		fields that (1) contain each rcd's bucket type, (2) holds
    #		values from specified fields of cohorts from the *other*
    #		dataset, (3) holds values from specified fields of cohorts
    #		from *this* (ds) dataset.
    # Returns: nothing
    # Assumes: self.run() has been called.
    #	       ds is really one of the two datasets of this bucketizer.
    #	       All the "new" fields to create are truly new and distinct.
    #	       The newBkTypeField is single-valued, all the other new fields
    #		may be single or multi-valued (but see Effects for the rules)
    # Effects: Adds the specified fields to 'ds', single or multi-valued as
    #		specified (default value is None (single valued) or [] (multi)).
    #	       Populates the new fields as follows (sorry, this is complex!)
    #	       For each rcd in ds:
    #	         Populate the newBkTypeField w/ a string= "0:1", "1:0", "1:1",
    #		    ... or "n:m" based on the type of bucket rcd is in.
    #		 for each field in newOtherCohortsFlds:
    #		    copy values from the cooresponding fields of the cohorts:
    #		    if the new field is single valued:
    #			leave it null if there are zero or multiple cohorts
    #		    	if there is a single cohort, copy its value as is
    #			 (so if the cohort's field is multi-valued, the
    #			  value will be a copy of the list)
    #		    if the new field is multi-valued:
    #			if the src dataset's corresponding fld is single-Val'd
    #			   populate the new fld w/ the list of all values
    #			   from all the cohorts (may include duplicates!)
    #			if the src dataset's corrsponding fld is multi-val'd
    #			   populate the new field w/ a list of lists of
    #			   all the cohorts values (may have duplicate lists
    #			   or duplicates across the lists!)
    #		 for each field in newThisCohortsFlds:
    #		    copy values from the cooresponding fields of the cohorts
    #		    using the same rules as above, BUT OMIT THE rcd's VALUES
    #		    (i.e., only copy values from "proper" cohorts, not itself)
    #		Whew!
    # Examples: (simple)
    #		idFields = ["MGI ID", "Ensembl ID"]
    #		bucketizer = Bucketizer(ds1, idFields, ds2, idFields)
    #		bucketizer.run()
    #
    #		# add bucket type column to ds1 & a (single-valued) symbol
    #		#   field from ds2
    #		bucketizer.updateDataSet(ds1, "bucket type",
    #					 ["ds2 symbol"], ["symbol"])
    #
    #		# ds2: add bucket type column & a field for keys of cohorts
    #		#      in ds1 and ds2 (these are multi-valued)
    #		bucketizer.updateDataSet(ds2, "bucket type",
    #					 ["ds1 keys"], ["_rcdkey"],
    #					 ["ds2 keys"], ["_rcdkey"],
    #					 {"ds1 keys": ",", "ds2 keys": ","})
    #
	# set otherDs to the other TableDataSet in this bucketizer
	otherDs = self.ds1
	if ds == self.ds1:
	    otherDs = self.ds2
	
	# add all the new fields
	newFields = [newBkTypeField] + newOtherCohortsFlds + newThisCohortsFlds
	for f in newFields:
	    if multiValued.has_key(f):
		ds.addField(f, [], multiValued[f])
	    else:
		ds.addField(f, None)

	# iterate through all the rcds in ds, and set the values for the new
	#   fields
	for key in ds.getKeys():
	    valuesToSet = []	# list of values to set for the new fields

	    # get cohort key lists
	    (list1, list2) = self.getBucketCohorts(ds, key)
	    if ds == self.ds1:
		thisCohorts  = list1
		otherCohorts = list2
	    else:
		thisCohorts  = list2
		otherCohorts = list1

	    # collect the bucket type as the value for 1st new field
	    valuesToSet.append(self.getBucketType(ds, key))

	    # iterate through newOtherCohortsFlds
	    i = 0
	    for f in newOtherCohortsFlds:
		cohortField = otherCohortsFlds[i]
		if not ds.isMultiValued(f):	# new is single-valued field
		    value = None
		    if len(otherCohorts) == 1:	# one other cohort
			cohortKey = otherCohorts[0]
			value = otherDs.getRecordByKey(cohortKey) [cohortField]
		else:				# new is multi-Valued field
		    value = []
		    for cohortKey in otherCohorts:
			value.append(otherDs.getRecordByKey(cohortKey) [cohortField])
		valuesToSet.append( value)
		i = i+1

	    # iterate through newThisCohortsFlds
	    i = 0
	    for f in newThisCohortsFlds:
		cohortField = thisCohortsFlds[i]
		if not ds.isMultiValued(f):	# new is single-valued field
		    value = None
		    if len(thisCohorts) == 1:	# one other cohort
			cohortKey = thisCohorts[0]
			value = otherDs.getRecordByKey(cohortKey) [cohortField]
		else:				# new is multi-Valued field
		    value = []
		    for cohortKey in thisCohorts:
			value.append(ds.getRecordByKey(cohortKey) [cohortField])
		valuesToSet.append( value)
		i = i+1
	    # Finally, update the values of all the new fields for this rcd
	    ds.updateFields(key, newFields, valuesToSet)
	return

    # end updateDataSet() ----------------------------------------
    
    def get0_1(self):
    # Purpose: return the 0:1 bucket
    # Returns: list of ds2 record keys from 0:1 BucketItems

	return self.b0_1

    # end get0_1() ----------------------------------------
    	
    def get1_0(self):
    # Purpose: return the 1:0 bucket
    # Returns: list of ds2 record keys from 1:0 BucketItems

	return self.b1_0

    # end get1_0() ----------------------------------------
    	
    def get1_1(self):
    # Purpose: return the 1:1 bucket
    # Returns: list of ds1, ds2 record key pairs from 1:1 BucketItems

	return self.b1_1

    # end get1_1() ----------------------------------------
    	
    def get1_n(self):
    # Purpose: return the 1:n bucket
    # Returns: list of (ds1 record key, [ds2 keys]) from 1:n BucketItems

	return self.b1_n

    # end get1_n() ----------------------------------------
    	
    def getn_1(self):
    # Purpose: return the n:1 bucket
    # Returns: list of ([ds1 keys], ds2 key) from n:1 BucketItems

	return self.bn_1

    # end getn_1() ----------------------------------------
    	
    def getn_m(self):
    # Purpose: return the n:m bucket
    # Returns: list of ([ds1 keys], [ds2 keys]) from n:m BucketItems

	return self.bn_m

    # end getn_m() ----------------------------------------
    
# End class TableDataSetBucketizer ---------------------------------------

class BucketItem:
# IS:   a BucketItem is a connected component from the bipartite graph
#	of nodes from TableDataSets ds1 and ds2 that are being bucketized
#
# HAS:  Two "sets" - one set of nodes representing the rcds from ds1
#	 and one set of nodes representing the rcds from ds2.
#
# DOES: merge two BucketItems and return the merged one.

    def __init__ (self,
	id,		# BucketItem ID
	rcds1,		# list of rcds from ds1 to put into this BucketItem
	rcds2		# list of rcds from ds2 to put into this BucketItem
	):
    # Purpose: constructor
    # Returns: nothing
    # Assumes: 
    # Effects: see Purpose
    # Throws : %%

	self.ds1Nodes = {}	# dict whose keys are the nodes from ds1
	self.ds2Nodes = {}	# dict whose keys are the nodes from ds2
	self.id	      = id
	for r in rcds1:
	    self.ds1Nodes[r] = 1
	for r in rcds2:
	    self.ds2Nodes[r] = 1

    # end __init__() class BucketItem ------------------------------
    
    def merge (self,
	biToMerge		# BucketItem to merge into this one (self)
        ):
    # Purpose: merges BucketItem 'biToMerge' into self
    # Returns: self, the merged BucketItem
    # Assumes: %%
    # Effects: %%
    # Throws : %%

	if self != biToMerge:	# is this how you test for object inequality?
	    for r in biToMerge.ds1Nodes.keys():
		self.ds1Nodes[r] = 1
	    for r in biToMerge.ds2Nodes.keys():
		self.ds2Nodes[r] = 1
	return self

    # end merge() ----------------------------------
    
    def getDs1Nodes (self):
    # Purpose: return the list of nodes from ds1 in this BucketItem
    # Returns: list of nodes

        return self.ds1Nodes.keys()
    # end getDs1Nodes() ----------------------------------
    
    def getDs2Nodes (self):
    # Purpose: return the list of nodes from ds2 in this BucketItem
    # Returns: list of nodes

        return self.ds2Nodes.keys()
    # end getDs2Nodes() ----------------------------------
    
    def getBucketType (self):
    # Purpose: return the "bucket type" of this bucket item
    # Returns: (string) "1:0" "0:1" "1:1" "1:n" "n:1" or "n:m"
    #		 depending on which bucket this bucketItem is in.

	bt = ""		# the bucket type string to return

	ds1Count = len(self.getDs1Nodes())
	ds2Count = len(self.getDs2Nodes())

	if   ds1Count == 1 and ds2Count == 0:
	    bt = "1:0"
	elif ds1Count == 0 and ds2Count == 1:
	    bt = "0:1"
	elif ds1Count == 1 and ds2Count == 1:
	    bt = "1:1"
	elif ds1Count == 1 and ds2Count >  1:
	    bt = "1:n"
	elif ds1Count >  1 and ds2Count == 1:
	    bt = "n:1"
	elif ds1Count >  1 and ds2Count >  1:
	    bt = "n:m"

        return bt
    # end getBucketType() ----------------------------------

    def printBI(self):
    # Purpose: print the BucketItem (for debugging purposes)
	print self.ds1Nodes.keys(), self.ds2Nodes.keys()
    # end printBI() --------------------------------------
    
# End class BucketItem ------------------------------------------------------

class TableDataSetBucketizerReporter:
# IS:   an object that formats and writes out buckets from a
#	 TableDataSetBucketizer.
#
# HAS:  a TableDataSetBucketizer (that presumably has been "run")
#
# DOES: outputs buckets to specified output files.
#
# Can imagine expanding this class to define options for how to format various
# bucket outputs (which fields to display, delimiter char, etc.). Perhaps in
# the future...

    def __init__ (self,
	bucketizer	# an already "run" TableDataSetBucketizer
	):
    # Purpose: constructor
    # Returns: nothing
    # Assumes: nothing
    # Effects: see Purpose
    # Throws : nothing

	self.bucketizer = bucketizer

    # end __init__() class TableDataSetBucketizerReporter ------------------

    def write_1_0(self,
	fp,		# open output filepointer to write to
	fields1=None	# list of fields from ds1 to print for ds1 rcds
	):
    # Purpose: write the 1_0 bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    #	       'fields1' is a list of fields from ds1 (or None)
    # Effects: writes buckets to 'fp', writing specified fields for each rcd.
    #	       If 'fields1' is None, writes all fields for ds1

	ds1 = self.bucketizer.ds1

	if fields1 == None:
	    fields1 = ds1.getFieldNames()

	ds1.printRecords(fp, fields1, self.bucketizer.get1_0())

    # end write_1_0() ----------------------------------------
    
    def write_0_1(self,
	fp,		# open output filepointer to write to
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the 0_1 bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    #	       'fields2' is a list of fields from ds2 (or None)
    # Effects: writes buckets to 'fp', writing specified fields for each rcd.
    #	       If 'fields2' is None, writes all fields for ds2

	ds2 = self.bucketizer.ds2

	if fields2 == None:
	    fields2 = ds2.getFieldNames()

	ds2.printRecords(fp, fields2, self.bucketizer.get0_1())

    # end write_0_1() ----------------------------------------
    	
    def write_1_1(self,
	fp,		# open output filepointer to write to
	fields1=None,	# list of fields from ds1 to print for ds1 rcds
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the 1_1 bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    # 	       fields1 and fields2 are None or subsets of fields from the
    #		respective TableDataSets.
    # Effects: writes buckets to 'fp', writing specified fields for
    #		each type of record.
    #	       If 'fields1' or 'fields2' is None, writes all fields for the
    #		respective TableDataSet
	
	self.write_nonZeroBucket(fp,
				 "1-1 BucketItem",
				 self.bucketizer.get1_1(),
				 fields1,
				 fields2)
    # end write_1_1() ----------------------------------------
    	
    def write_1_n(self,
	fp,		# open output filepointer to write to
	fields1=None,	# list of fields from ds1 to print for ds1 rcds
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the 1_n bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    # 	       fields1 and fields2 are None or subsets of fields from the
    #		respective TableDataSets.
    # Effects: writes buckets to 'fp', writing specified fields for
    #		each type of record.
    #	       If 'fields1' or 'fields2' is None, writes all fields for the
    #		respective TableDataSet

	self.write_nonZeroBucket(fp,
				 "1-n BucketItem",
				 self.bucketizer.get1_n(),
				 fields1,
				 fields2)
    # end write_1_n() ----------------------------------------
    	
    def write_n_1(self,
	fp,		# open output filepointer to write to
	fields1=None,	# list of fields from ds1 to print for ds1 rcds
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the n_1 bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    # 	       fields1 and fields2 are None or subsets of fields from the
    #		respective TableDataSets.
    # Effects: writes buckets to 'fp', writing specified fields for
    #		each type of record.
    #	       If 'fields1' or 'fields2' is None, writes all fields for the
    #		respective TableDataSet

	self.write_nonZeroBucket(fp,
				 "n-1 BucketItem",
				 self.bucketizer.getn_1(),
				 fields1,
				 fields2)
    # end write_n_1() ----------------------------------------
    	
    def write_n_m(self,
	fp,		# open output filepointer to write to
	fields1=None,	# list of fields from ds1 to print for ds1 rcds
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the n_m bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    # 	       fields1 and fields2 are None or subsets of fields from the
    #		respective TableDataSets.
    # Effects: writes buckets to 'fp', writing specified fields for
    #		each type of record.
    #	       If 'fields1' or 'fields2' is None, writes all fields for the
    #		respective TableDataSet

	self.write_nonZeroBucket(fp,
				 "n-m BucketItem",
				 self.bucketizer.getn_m(),
				 fields1,
				 fields2)
    # end write_n_m() ----------------------------------------

    def write_nonZeroBucket(self,
	fp,		# open output filepointer to write to
	label,		# string; label to print for each bucket item.
	bucket,		# a list of bucket items from a 1:1 1:n n:1 or n:m
			#     bucket
	fields1=None,	# list of fields from ds1 to print for ds1 rcds
	fields2=None	# list of fields from ds2 to print for ds2 rcds
	):
    # Purpose: write the bucket to fp
    # Returns: nothing
    # Assumes: associated TableDataSetBucketizer has been "run"
    # 	       fields1 and fields2 are None or subsets of fields from the
    #		respective TableDataSets.
    # Effects: writes buckets to 'fp', writing specified fields for
    #		each type of record.
    #	       If 'fields1' or 'fields2' is None, writes all fields for the
    #		respective TableDataSet

	ds1 = self.bucketizer.ds1
	ds2 = self.bucketizer.ds2
	if fields1 == None:
	    fields1 = ds1.getFieldNames()
	if fields2 == None:
	    fields2 = ds2.getFieldNames()

	for (keys1,keys2) in bucket:
	    fp.write("[ %s\n" % label)
	    ds1.printRecords(fp, fields1, keys1)
	    fp.write("----\n")
	    ds2.printRecords(fp, fields2, keys2)
	    fp.write("]\n")

    # end write_nonZeroBucket() ----------------------------------------

# end class TableDataSetBucketizerReporter
