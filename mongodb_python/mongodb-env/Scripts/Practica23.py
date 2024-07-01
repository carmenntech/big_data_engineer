from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

categories = [
   { 'id': "MongoDB", 'parent': "Databases" },
   { 'id': "dbm", 'parent': "Databases" },
   { 'id': "Databases", 'parent': "Programming" },
   { 'id': "Languages", 'parent': "Programming" },
   { 'id': "Programming", 'parent': "Books" },
   { 'id': "Books", 'parent': 'null' }
]

with client:

    db = client.testdb

    res = db.categories.insertMany(categories)

    db.categories.findOne({_id: "MongoDB"}).parent

    db.categories.createIndex({parent: 1})

    db.categories.find({parent: "Databases"})

    print(res)