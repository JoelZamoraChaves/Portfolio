from pymongo import MongoClient
import sys

user = 'root'
password = sys.argv[1]
host='localhost'

connecturl = "mongodb://{}:{}@{}:27017/?authSource=admin".format(user,password,host)

connection = MongoClient(connecturl)

db = connection.training

mongodb_glossary = db.python

# create a sample document

docs = [
    { "database": "a database contains collections" },
    { "collection": "a collection stores the documents" },
    { "document": "a document contains the data in the form of key value pairs." }
]

db.collection.insert_many(docs)

docs_result = db.collection.find()

for doc in docs_result:
    print(doc)

connection.close()