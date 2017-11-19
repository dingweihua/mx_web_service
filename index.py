# -*- coding: utf-8 -*-

from flask import Flask
import mongfun
import ujson
app = Flask(__name__)

@app.route("/get_movie_list")
def hello():
    collection = mongfun.get_mongo_collection('db_douban', 'movie','')
    return ujson.dumps(collection.find())
