#!/usr/bin/env python

import pymongo
import sys
import warnings
import argparse


class MongoDBClient:
		""" This class provides user interface to connect to MongoDB. The functionality includes query database, insert documents to the collection in the database. It does not support create and drop database nor collection. 
		"""
	def __init__(self):
		""" Initialze an instance
		"""
		self.client = None
		self.host = ''
		self.username = ''
		self.password = ''
		self.port = ''
		self.database = None
		self.collection = None
	def _db_con_init(self, host, username, password, port, database, collection):
		"""Connect to MongoDB internally
		"""
		self.host = host
		self.username = username
		self.password = password
		self.port = port
		self.database = database
		self.client = pymongo.MongoClient(host, port)
		try: 
			self.client.the_database.authenticate(name=username, password=password, source=database, mechanism='DEFAULT')
			self.database = self.client[database]
		except Exception as e:
			raise RuntimeError("%s %s" %(type(e), e))
		if collection in self.database.collection_names():
			self.collection = self.database[collection]
		else:
			raise RuntimeError("Collection (%s) does not exist in the database (%s) " % (collection, database))
	def db_con(self, host, username, password, port, database, collection):
		"""An interface for user to connect to MongoDB
		"""
		self._db_con_init(host, username, password, port, database, collection)
	def use_collection(self,collection):
		if collection in self.database.collection_names():
			self.collection = self.database[collection]
		else:
			raise RuntimeError("Collection (%s) does not exist in the database (%s) " % (collection, database))
	def data_insert_one(self,data):
		"""An user interace to insert a document.
		
		args:
			data (a json object): a document to insert to database
		
		return:
			an InsertOneResult object: A pymongo object which can be used to check the insertion result.
		
		Note: It does not raise any error or exception when it fails to insert a document. The reason is that this function could be called inside a loop, it allows accidental failures so that it will not lose other insertions.
		"""
		insertOneResult = self.collection.insert_one(data)
		if insertOneResult.acknowledged != True:
			warnings.warn("Insertion failed")
	def query_data(self, query=None, projection=None, skip=0, limit=0):
		"""An user interace to query database. Default it returns all documents.

		args:
			query (a dictionary): used to filter database

			projection (a dictionary) : used to project retults. It only returns the projected fields.
		"""
		try:
			cursor = self.collection.find(query,projection, skip=skip, limit=limit)
		except Exception as e:
			raise RuntimeError("Unexcepted error %s %s " %(type(e), e))
	def query_and_print_data(self, query=None, projection=None, skip=0, limit=0):
		"""An user interace to query database. Default it returns all documents.

		args:
			query (a dictionary): used to filter database

			projection (a dictionary) : used to project retults. It only returns the projected fields.
		"""
		try:
			cursor = self.collection.find(query,projection, skip=skip, limit=limit)
		except Exception as e:

		for doc in cursor:
			 print doc


if __name__ == '__main__':
	mongoDB = MongoDBClient()
	mongoDB.db_con(host='131.160.80.65', username='druids', password='druids', port=27017, database='ericsson',collection='terminology')

	#mongoDB.query_data()
	#mongoDB.data_insert_one({"first": "Jussi", "last": 'Jussi'})
	parser = argparse.ArgumentParse()
	parser = add_argment('-i', '--insert', required=True, dest=insert, help='insert a terminalogy')
	parser = add_argment('-q', '--query', required=True, dest=query, help='query a terminalogy')
	if parser.action == "insert":
		print "insert"
	elif parser.action == 'query':
		print "query"
		
	#parser = add_argment('-i', '--insert', required=True, dest=insert, help='insert a terminalogy')
