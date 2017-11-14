#!/usr/local/python/bin/python

# coding: utf-8

import sys
import os
import datetime
import time
import redis
import simplejson

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import (
    QueryEvent,
)
from daemonCollector import DaemonCollector
from util import *


class MysqlCollector(DaemonCollector):
    """Subclass of :class:`.Daemon`.
    """

    def __init__(self, config):
        super(MysqlCollector, self).__init__(config)
        self.stream = None
        self.init_redis()

    def log_binlog_position(self, log_file, log_pos):
        self.logger.info('locate binlog file[%s] position [%d]' % (log_file, log_pos))
        open(self.positionFile, 'w').write("%s:%d" % (log_file, log_pos))

    def last_binlog_position(self):
        if not os.path.exists(self.positionFile):
            return (None, None, False)
        try:
            with open(self.positionFile) as f:
                res = f.readline()
                if res.find(":") == -1:
                    return (None, None, False)
                log_file, log_pos = res.split(":")
                log_pos = int(log_pos)
        except IOError:
            return (None, None, False)

        return (log_file, log_pos, True)

    def run(self):
        mode = self.conf.get('mysql', 'mode')
        if mode == 'row':
            self.run_by_row()
        else:
            self.run_by_statment()

    def run_by_row(self):
        # server_id is your slave identifier, it should be unique.
        # set blocking to True if you want to block and wait for the next event at
        # the end of the stream
        mysql_settings = {
            "host": self.conf.get('mysql', 'host'),
            "port": self.conf.getint('mysql', 'port'),
            "user": self.conf.get('mysql', 'user'),
            "passwd": self.conf.get('mysql', 'password'),
        }

        watch_database = set(self.conf.get('mysql', 'databases').split(','))
        # load last binlog reader position
        log_file, log_pos, resume_stream = self.last_binlog_position()

        self.stream = BinLogStreamReader(connection_settings=mysql_settings,
                                         server_id=self.conf.getint('mysql', 'slaveid'),
                                         only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                         blocking=True,
                                         resume_stream=resume_stream,
                                         log_file=log_file,
                                         log_pos=log_pos,
                                         # only_schemas=self.conf.get('mysql', 'databases').split(',')
                                         )

        while True:
            refresh = False
            try:
                for binlogevent in self.stream:
                    refresh = True
                    self.log_binlog_position(self.stream.log_file, self.stream.log_pos)
                    log_file, log_pos = self.stream.log_file, self.stream.log_pos

                    # filter no watch database
                    if binlogevent.schema not in watch_database:
                        # self.logger.info("Not Watching database[%s] table[%s], current position: [%s:%d], timestamp:%s" % (binlogevent.schema, binlogevent.table, self.stream.log_file, self.stream.log_pos, datetime.datetime.fromtimestamp(binlogevent.timestamp).strftime('%Y-%m-%d %H:%M:%S')))
                        continue

                    binlog = {}
                    binlog['storage'] = 'mysql'
                    binlog['database'] = '%s' % binlogevent.schema
                    binlog['table'] = '%s' % binlogevent.table
                    binlog['timestamp'] = '%s' % datetime.datetime.fromtimestamp(binlogevent.timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    binlog['event_type'] = binlogevent.event_type

                    for row in binlogevent.rows:
                        if isinstance(binlogevent, DeleteRowsEvent):
                            binlog['values'] = row["values"]
                            binlog['type'] = 'DELETE'
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            binlog["before"] = row["before_values"]
                            binlog["values"] = row["after_values"]
                            binlog['type'] = 'UPDATE'
                        elif isinstance(binlogevent, WriteRowsEvent):
                            binlog['values'] = row["values"]
                            binlog['type'] = 'INSERT'

                        binlog_row = formatBinlog(binlog)
                        hash_key = self.conf.get('redis', 'queue_key') + ":" + binlog['database']
                        self.push_to_redis(hash_key, binlog_row)
                        # self.binlog_logger.info(binlog_row)

                if not refresh:
                    self.logger.info("NO new input binlog, current position: [%s:%d]" % (log_file if log_file is not None else "", log_pos if log_pos is not None else 0))
                    time.sleep(0.5)
            except Exception,e:
                print e; sys.exit();
                self.stream = BinLogStreamReader(connection_settings=mysql_settings,
                                                 server_id=self.conf.getint('mysql', 'slaveid'),
                                                 only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                                 blocking=True,
                                                 resume_stream=resume_stream,
                                                 log_file=log_file,
                                                 log_pos=log_pos,
                                                 # only_schemas=self.conf.get('mysql', 'databases').split(',')
                                                 )

    def run_by_statment(self):
        # server_id is your slave identifier, it should be unique.
        # set blocking to True if you want to block and wait for the next event at
        # the end of the stream
        mysql_settings = {
            "host": self.conf.get('mysql', 'host'),
            "port": self.conf.getint('mysql', 'port'),
            "user": self.conf.get('mysql', 'user'),
            "passwd": self.conf.get('mysql', 'password'),
        }

        # load last binlog reader position
        log_file, log_pos, resume_stream = self.last_binlog_position()

        watch_database = set(self.conf.get('mysql', 'databases').split(','))

        self.stream = BinLogStreamReader(connection_settings=mysql_settings,
                                         server_id=self.conf.getint('mysql', 'slaveid'),
                                         only_events=[QueryEvent],
                                         # blocking=False,
                                         resume_stream=resume_stream,
                                         log_file=log_file,
                                         log_pos=log_pos,
                                         # skip_to_timestamp=1475055800,
                                         # only_schemas=self.conf.get('mysql', 'databases').split(',')
                                         )

        self.logger.info("binlog parser start runing from binlog file[%s] log position[%d]" % ((log_file if log_file is not None else ''), (log_pos if log_pos is not None else 0)))
        while True:
            try:
                refresh = False
                for binlogevent in self.stream:
                    refresh = True
                    self.log_binlog_position(self.stream.log_file, self.stream.log_pos)
                    log_file, log_pos = self.stream.log_file, self.stream.log_pos
                    if binlogevent.schema not in watch_database:
                        # self.logger.info("Not Watching database[%s] binlog[%s], current position: [%s:%d], timestamp:%s" % (binlogevent.schema, binlogevent.query, self.stream.log_file, self.stream.log_pos, datetime.datetime.fromtimestamp(binlogevent.timestamp).strftime('%Y-%m-%d %H:%M:%S')))
                        continue

                    binlog = {}
                    binlog['storage'] = 'mysql'
                    binlog['database'] = '%s' % binlogevent.schema
                    binlog['sql'] = '%s' % binlogevent.query
                    binlog['timestamp'] = '%s' % datetime.datetime.fromtimestamp(binlogevent.timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    binlog['event_type'] = binlogevent.event_type

                    binlog_row = formatBinlog(binlog)
                    hash_key = self.conf.get('redis', 'queue_key') + ":" + binlog['database']
                    self.push_to_redis(hash_key, binlog_row)
                    # self.binlog_logger.info(binlog_row)

                if not refresh:
                    self.logger.info("NO new input binlog, current position: [%s:%d]" % (log_file, log_pos))
                    time.sleep(0.5)
            except Exception,e:
                print e; sys.exit();
                self.stream = BinLogStreamReader(connection_settings=mysql_settings,
                                                 server_id=self.conf.getint('mysql', 'slaveid'),
                                                 only_events=[QueryEvent],
                                                 # blocking=False,
                                                 resume_stream=resume_stream,
                                                 log_file=log_file,
                                                 log_pos=log_pos,
                                                 # skip_to_timestamp=1475055800,
                                                 # only_schemas=self.conf.get('mysql', 'databases').split(',')
                                                 )

    def __del__(self):
        if self.stream is not None:
            self.stream.close()
