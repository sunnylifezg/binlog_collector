#!/usr/local/python/bin/python
#coding:utf8

# 检查所有数据库
# for i in `ls conf/ |grep 'ini' |awk -F'.' '{print $1}'`; do echo $i;/usr/local/python27/bin/python position.py --database $i;  done;
import sys
import os
import logging
import logging.handlers
import configparser
import argparse
import logging.config
import pymysql.cursors

from pymongo import MongoClient, DESCENDING


def validate_parser():
    parser = argparse.ArgumentParser(description='Show Master instance binlog position info, only support mysql')
    parser.add_argument('--database', dest='database', action='store', type=str, help="Binlog position of specify database, all mysql position will be show by default", required=True)

    return parser

if __name__ == '__main__':
    parser = validate_parser()
    args = parser.parse_args()

    cur_dir = os.path.dirname(sys.argv[0])
    if len(cur_dir) > 0:
        os.chdir(cur_dir)

    if len(args.database) >  0:
        if args.database == 'all':
            file_names = os.listdir('conf')
            database_list = []
            for fn in file_names:
                if '.ini' in fn:
                    database_list.append(fn[:-4])
        else:
            database_list = [args.database]
    else:
        parser.print_help()
        sys.exit(1)

    for database in database_list:
        conf_file = "conf/%s.ini" % database
        log_conf_file = "conf/%s_logging.conf" % database

        config = configparser.ConfigParser()
        config.read(conf_file)

        logging.config.fileConfig(log_conf_file)
        logger = logging.getLogger("%s_collector" % database)

        collector_module = config.get('general', 'collector')

        if collector_module.strip() == 'mysql':
            connection = pymysql.connect(host=config.get('mysql', 'host'),
                                         port=int(config.get('mysql', 'port')),
                                         user=config.get('mysql', 'user'),
                                         password=config.get('mysql', 'password'),
                                         db=database,
                                         charset='utf8',
                                         cursorclass=pymysql.cursors.DictCursor)

            try:
                with connection.cursor() as cursor:
                    master_status_sql = "show master status"
                    cursor.execute(master_status_sql)
                    status_result = cursor.fetchone()

                    binlog_format_sql = "show variables like 'binlog_format'"
                    cursor.execute(binlog_format_sql)
                    format_result = cursor.fetchone()

                    slave_status_sql = "show slave status"
                    cursor.execute(slave_status_sql)
                    slave_status_result = cursor.fetchone()

                    print "Database: %s" % database
                    print "File: %s, Position: %s, BinlogFormat: %s" % (status_result['File'], status_result['Position'], format_result['Value'])
                    print "%(File)s:%(Position)s" % status_result
                    if slave_status_result is not None:
                        print "Slave_IO_State: %s\nLast_Error: %s" % (slave_status_result['Slave_IO_State'], slave_status_result['Last_Error'])
                    else:
                        print "No slave status acquired"

                    print "\n"

            finally:
                connection.close()

    sys.exit(0)
