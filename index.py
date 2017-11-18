from flask import Flask
from pymongo import MongoClient
import mongfun
from bson.json_util import dumps
app = Flask(__name__)

@app.route("/get_movie_list")
def hello():
    collection = mongfun.get_mongo_collection('db_douban', 'movie','')
    return dumps(collection.find())
