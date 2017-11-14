#!/usr/local/python/bin/python

import sys
import os
import datetime
import time
import simplejson
from bson.objectid import ObjectId


def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.strftime('%Y-%m-%d %H:%M:%S')


def formatBinlog(binlog):
    return simplejson.dumps(binlog, default=date_handler)


def timestamp_handler(obj):
    if hasattr(obj, 'time') and hasattr(obj, 'inc'):
        return [obj.time, obj.inc]
    if isinstance(obj, ObjectId):
        return "%s" % obj
    if hasattr(obj, 'isoformat'):
        return formatMongoDate(obj)


def formatOpLog(oplog):
    return simplejson.dumps(oplog, default=timestamp_handler)


# format mongo date, add 8 hours, to be stardard
def formatMongoDate(obj):
    try:
        new_obj = obj + datetime.timedelta(hours=8)
        return new_obj.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "0000-00-00 00:00:00"

def string2timestamp(strValue):
    try:
        d = datetime.datetime.strptime(strValue, "%Y-%m-%d %H:%M:%S.%f")
        t = d.timetuple()
        timeStamp = int(time.mktime(t))
        timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond))/1000000
        print timeStamp
        return timeStamp
    except ValueError as e:
        print e
        d = datetime.datetime.strptime(str2, "%Y-%m-%d %H:%M:%S")
        t = d.timetuple()
        timeStamp = int(time.mktime(t))
        timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond))/1000000
        print timeStamp
        return timeStamp


def timestamp2string(timeStamp):
    try:
        d = datetime.datetime.fromtimestamp(timeStamp)
        str1 = d.strftime("%Y-%m-%d %H:%M:%S.%f")
        # 2015-08-28 16:43:37.283000'
        return str1
    except Exception as e:
        print e
        return None
