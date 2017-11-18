#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Name: mongfun.py
# Purpose:
#
# Author: 张歆韵(sean.Zhang)
#
# Created: 16/6/30 下午2:15
from bson import Code
from pymongo import ReadPreference
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import MongoClient
import config as cfg

READ_SLAVE = ReadPreference.SECONDARY_PREFERRED
READ_PRIMARY = ReadPreference.PRIMARY


def get_mongo_conn():
    """
    One or more mongos instances. The mongos instances are the routers for the cluster. Typically, deployments
have one mongos instance on each application server.
    You may also deploy a group of mongos instances and use a proxy/load balancer between the application and the mongos.
In these deployments, you must configure the load balancer for client affinity so that every connection from a
single client reaches the same mongos.
    Because cursors and other resources are specific to an single mongos instance, each client must interact with
only one mongos instance.
    也就是说,一旦启动时确定了用哪个mongos,就不要换了
    :return:
    """
    return MongoClient(cfg.g_mongodb_url)


def get_mongo_collection(db_name, collection_name, is_read_slave):
    """
    获取collection
    :param db_name:
    :param collection_name:
    :param is_read_slave: 读写分离salve
    :return:
    """
    if is_read_slave:
        return get_mongo_conn().get_database(db_name).get_collection(
            collection_name, read_preference=READ_SLAVE)
    else:
        return get_mongo_conn().get_database(db_name).get_collection(collection_name)


def to_mongo_id(objid):
    """
    转换为mongodb的objid
    :param objid:
    :return:
    """
    if isinstance(objid, (str, unicode)):
        return ObjectId(objid)
    else:
        return objid


def to_mongo_dict(py_map):
    """
    转换为mongodb的dict
    By default, the mongo shell treats all numbers as floating-point values.
    http://docs.mongodb.org/manual/core/shell-types/#types
    :param py_map:
    :return:
    """
    return SON(py_map)


def get_mongo_dict():
    """
    mongodb的dict
    :return:
    """
    return SON()


def mongo_find(collection, last_id=None, page_size=10,
               sort_filter='c', sorttype=-1, page_type='page_down',
               self=0, find_filter={}, select_filter={}):
    """ mgdb find 翻页大礼包
    [必选]
    :collection: collection对象
    [可选]
    :last_id: 翻页用标记位 _id
    :page_size: 每页条目个数 默认:10条
    :sort_filter: 排序选项 默认根据 m 排序
    :sorttype: 1:正序 -1:倒序
    :page_type: page_down:下一页 page_up:上一页
    :find_filter: 查找过滤器字典 mongodb格式 例:{'s_id':123456}
    :select_filter: 选择过滤器字典 mongodb格式 例:{'or_id':1,'or_num':1}

    """
    rollover = 0
    if sorttype == -1:  # -1:倒序, 1:正序
        page_type = '$gt' if page_type == 'page_up' else '$lt'
        sorttype = 1 if page_type == '$gt' else -1
        rollover = 1 if page_type == '$gt' else 0

    else:
        page_type = '$lt' if page_type == 'page_up' else '$gt'

    if self:  # 包括自己
        page_type = ''.join([page_type, 'e'])

    '''
    $gt |   1           |  正序下一页
        v   12          v
            123
            1234
            12345
        ^   123456      ^
    $lt |   1234567     |  倒序下一页
    '''

    if last_id:
        find_filter.update({'_id': {page_type: last_id}})

    if select_filter:
        cursor = collection.find(find_filter, select_filter).limit(page_size).sort(sort_filter, sorttype)
        if rollover:
            cursor = [x for x in cursor][::-1]
    else:
        cursor = collection.find(find_filter).limit(page_size).sort(sort_filter, sorttype)
        if rollover:
            cursor = [x for x in cursor][::-1]

    return cursor


def mongo_find_pagenum(collection, skip_num=0, page_size=10,
                           sort_filter='c', sorttype=-1,
                           find_filter={}, select_filter={}, sort_filter_list=[]):

    """ mgdb find 页码翻页大礼包
    [必选]
    :collection: collection对象
    [可选]
    :skip: 跳过数目
    :page_size: 每页条目个数 默认:10条
    :sort_filter: 排序选项 默认根据 c 排序
    :sorttype: 1:正序 -1:倒序
    :find_filter: 查找过滤器字典 mongodb格式 例:{'s_id':123456}
    :select_filter: 选择过滤器字典 mongodb格式 例:{'or_id':1,'or_num':1}
    :sort_filter_list:
    """
    total = collection.find(find_filter).count()
    if select_filter:
        if sort_filter_list:
            cursor = collection.find(find_filter, select_filter).sort(
                sort_filter_list).skip(skip_num).limit(page_size)
        else:
            cursor = collection.find(find_filter, select_filter).sort(
                sort_filter, sorttype).skip(skip_num).limit(page_size)
    else:
        if sort_filter_list:
            cursor = collection.find(find_filter).sort(
                sort_filter_list).skip(skip_num).limit(page_size)
        else:
            cursor = collection.find(find_filter).sort(
                sort_filter, sorttype).skip(skip_num).limit(page_size)

    return cursor, total


def mongo_group_by(collection, key, condition, initial):
    """
    mongodb分组统计查询:
    :param collection: 数据集
    :param key: 分组关键字
    :param condition: 分组查询条件
    :param initial: 统计字段别名
    :return:
    """
    reducer = Code("""function(obj, prev){prev.count++;}""")
    # 查询所在区域各商户数量
    result_lst = collection.group(key, condition, initial, reducer)
    return result_lst


if __name__ == '__main__':
    print str(ObjectId())
