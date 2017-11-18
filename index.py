from flask import Flask
from pymongo import MongoClient
from bson.json_util import dumps
app = Flask(__name__)

@app.route("/get_movie_list")
def hello():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['db_douban']
    col = db['movie']
    return dumps(list(col.find()))
