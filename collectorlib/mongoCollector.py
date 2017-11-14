#!/usr/local/python/bin/python

# coding: utf-8

import sys
import os
import datetime, time
import redis
import simplejson

from pymongo import MongoClient, DESCENDING
from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from bson.timestamp import Timestamp

from daemonCollector import DaemonCollector
from util import *


class MongoCollector(DaemonCollector):
    """Subclass of :class:`.Daemon`.
    """

    def __init__(self, config):
        super(MongoCollector, self).__init__(config)
        db = self.conf.get('mongo', 'database')
        collections = self.conf.get('mongo', 'collections').split(",")
        self._ns_filter = []
        for collection in collections:
            self._ns_filter.append(db + '.' + collection)
        self.oplog_db = self.conf.get('mongo', 'oplog_database')
        self.oplog_collection = self.conf.get('mongo', 'oplog_collection')
        self.poll_time = 0.5
        self.connection = None

    def init_mongo(self):
        dsn = "mongodb://" + self.conf.get('mongo', 'host')
        port = self.conf.get('mongo', 'port')
        try:
            port = int(port)
            self.logger.info("Trying to connect to mongo [%s:%d]" % (dsn, port))
            self.connection = MongoClient(dsn, port=port, replicaSet=None, socketTimeoutMS=500000)
            self.oplog = self.connection.get_database(self.oplog_db).get_collection(self.oplog_collection)
        except Exception,e:
            self.logger.error("connect to mongo[%s:%d] error: %s" % (dsn, port, e))
            sys.exit(1)

    def log_oplog_position(self, timestamp):
        self.logger.info('locate oplog timestamp [%d] inc[%d]' % (timestamp.time, timestamp.inc))
        open(self.positionFile, 'w').write("%d:%d" % (timestamp.time, timestamp.inc))

    def last_oplog_position(self):
        if not os.path.exists(self.positionFile):
            return (None, None)
        try:
            with open(self.positionFile) as f:
                res = f.readline()
                if res.find(":") == False:
                    return (None, None, False)
                timestamp, inc = res.split(":")
                timestamp = int(timestamp)
                inc = int(inc)
        except IOError:
            return (None, None)

        return (timestamp, inc)

    def run(self):
        self.init_mongo()
        self.init_redis()

        if self._ns_filter is None:
            query = {}
        else:
            query = {'ns': {'$in': self._ns_filter}}

        last_ts, last_inc = self.last_oplog_position()
        if last_ts is None:
            # if self.oplog.count():
            #    query['ts'] = {"$gte": self.oplog.find().sort('$natural', DESCENDING).limit(-1).next()['ts']}
            # else:
            #    self.logger.warning("OpLog is Empty, Please Check mongo configuration")
            #    sys.exit(1);
            now_ts = int(time.time())
            query['ts'] = {"$gte": Timestamp(now_ts, 1)}
        else:
            query['ts'] = {"$gte": Timestamp(last_ts, last_inc)}

        options = {
            'cursor_type': CursorType.TAILABLE_AWAIT,
            'no_cursor_timeout': True,
        }

        while True:
            refresh = False
            cursor = self.oplog.find(query, **options)
            try:
                while cursor.alive:
                    try:
                        op = cursor.next()
                        refresh = True
                        id = self.__get_id(op)
                        # handle oplog
                        self.handle(ns=op['ns'], ts=op['ts'], op=op['op'], id=id, raw=op)
                        # record oplog position
                        self.log_oplog_position(op['ts'])
                        # update nexst oplog query ts condition
                        query['ts'] = {"$gte": op['ts']}
                    except StopIteration:
                        self.logger.info("NO new input oplog, current timestamp: [%s:%d]" % (query['ts']["$gte"].time, query['ts']["$gte"].inc))
                        # time.sleep(self.poll_time)
            except AutoReconnect,e:
                print e;sys.exit();
                self.logger.info("reconnect to mongo, current timestamp: [%s:%d]" % (query['ts']["$gte"].time, query['ts']["$gte"].inc))
                time.sleep(self.poll_time)

            # if not refresh:
            #     self.logger.info("NO new input oplog, current timestamp: [%s:%d]" % (query['ts']["$gte"].time, query['ts']["$gte"].inc))

    # only process operation: i/u/d
    def handle(self, ns, ts, op, id, raw):
        if op == 'n':
            self.noop(ts=ts)
        elif op == 'c':
            pass
        elif op == 'db':
            pass
        else:
            oplog = {}
            oplog['storage'] = 'mongo'
            database, collection = raw['ns'].split(".")
            oplog['database'] = database
            oplog['table'] = collection
            oplog['sql'] = raw
            oplog['timestamp'] = raw['ts'].time
            hash_key = self.conf.get('redis', 'queue_key') + ":" + oplog['database'] + ":" + oplog['table']
            self.push_to_redis(hash_key, formatOpLog(oplog))

    def __get_id(self, op):
        id = None
        o2 = op.get('o2')
        if o2 is not None:
            id = o2.get('_id')

        if id is None:
            id = op['o'].get('_id')

        return id

    def __del__(self):
        if self.connection is not None:
            self.connection.close()
