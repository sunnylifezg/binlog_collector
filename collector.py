#!/usr/local/python/bin/python

# coding: utf-8

import sys
import os
import logging
import logging.handlers
import configparser
import argparse
import logging.config

from collectorlib.daemonCollector import DaemonCollector
from collectorlib.mysqlCollector import MysqlCollector
from collectorlib.mongoCollector import MongoCollector

collector_map = {
    'mysql': 'MysqlCollector',
    'mongo': 'MongoCollector'
}

def validate_parser():
    parser = argparse.ArgumentParser(description='Replicate MySQL/MongoDB database binlog/oplog to redis')
    parser.add_argument('--conf', dest='conf', action='store', type=str)
    parser.add_argument('--start', dest='start', action='store_true', help="Start the daemon process", default=False)
    parser.add_argument('--stop', dest='stop', action='store_true', help="Stop the daemon process", default=False)
    parser.add_argument('--restart', dest='restart', action='store_true', help="Restart the daemon process", default=False)
    parser.add_argument('--status', dest='status', action='store_true', help="Status of the daemon process", default=False)

    return parser

if __name__ == '__main__':
    parser = validate_parser()
    args = parser.parse_args()

    if not args.conf:
        parser.print_help()
        sys.exit(1)

    cur_dir = os.path.dirname(sys.argv[0])
    if len(cur_dir) > 0:
        os.chdir(cur_dir)
    conf_file = "conf/%s.ini" % (args.conf)
    log_conf_file = "conf/%s_logging.conf" % (args.conf)

    config = configparser.ConfigParser()
    config.read(conf_file)

    logging.config.fileConfig(log_conf_file)
    logger = logging.getLogger("%s_collector" % args.conf)

    logger.info('start binlog collector...')
    collector_module = config.get('general', 'collector')
    if collector_module not in collector_map:
        logger.error('config file[%s] collector[%s] invalid(mysql/mongo)' % (conf_file, collector_module))
        print 'config file[%s] collector[%s] invalid(mysql/mongo)' % (conf_file, collector_module)
        sys.exit(1)

    if collector_module == 'mysql':
        collector = MysqlCollector(config)
    else:
        collector = MongoCollector(config)
    if args.start:
        collector.start()
        logger.info('start binlog collector of %s' % conf_file)
    elif args.stop:
        collector.stop()
        logger.info('stop binlog collector of %s' % conf_file)
    elif args.restart:
        collector.restart()
        logger.info('restart binlog collector of %s' % conf_file)
    elif args.status:
        collector.status()
    else:
        parser.print_help()
    sys.exit(0)
