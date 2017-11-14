#!/usr/local/python/bin/python

# coding: utf-8

import sys
import os
import time
import logging
import atexit
import redis

from signal import SIGTERM


class DaemonCollector(object):

    """Class to demonize the application.
    """
    def __init__(self, config):
        log_name = config['log']['log_name'] if 'log_name' in config['log'] else __name__
        self.logger = logging.getLogger(log_name)
        binlog_name = config['log']['binlog_name'] if 'binlog_name' in config['log'] else __name__
        self.binlog_logger = logging.getLogger(binlog_name)
        self.pidfile = config['general']['pid_file']
        self.positionFile = config['general']['binlog_position_file']
        self.conf = config
        self.stdin = os.devnull
        self.stdout = os.devnull
        self.stderr = os.devnull

    def daemonize(self):
        """Deamonize, do double-fork magic.
        """
        try:
            pid = os.fork()
            if pid > 0:
                # Exit first parent.
                self.logger.info("first fork Done")
                sys.exit(0)
        except OSError as e:
            message = "Fork #1 failed: %s\n" % e
            self.logger.error(message)
            sys.exit(1)

        # Decouple from parent environment.
        # os.chdir("/")
        os.setsid()
        os.umask(0)

        # Do second fork.
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from second parent.
                self.logger.info("second fork Done")
                sys.exit(0)
        except OSError as e:
            message = "Fork #2 failed: %s\n" % e
            self.logger.error(message)
            sys.exit(1)

        self.logger.info('deamon going to background, PID: %d' % os.getpid())

        # Redirect standard file descriptors.
        '''sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        '''

        # Write pidfile.
        pid = str(os.getpid())
        open(self.pidfile, 'w+').write("%s" % pid)
        self.logger.info("pid file %s[%s] saved" % (self.pidfile, pid))

        # Register a function to clean up.
        atexit.register(self.__del__)

    def __del__(self):
        # delete pid file
        os.remove(self.pidfile)

    def start(self):
        """Start daemon.
        """
        pids = None
        # Check pidfile to see if the daemon already runs.
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError:
            pids = None

        if pids:
            message = "pid file %s[%s] already exist. Daemon already running!\n" % (self.pidfile, pids)
            self.logger.error(message)
            sys.exit(1)

        # Start daemon.
        self.daemonize()
        self.logger.info("collector demonized. Start to run...")
        self.run()

    def status(self):
        """Get status of daemon.
        """
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError:
            self.logger.error("There is no PID file. Daemon is not running\n")
            sys.exit(1)

        try:
            for pid in pids:
                procfile = open("/proc/%s/status" % pid.strip(), 'r')
                procfile.close()
                message = "There is a process with the PID %s in pid file[%s]\n" % (pid.strip(), self.pidfile)
                self.logger.error(message)
                sys.exit(0)
        except IOError:
            message = "There is not a process with the PID[%s] in pid file[%s]\n" % (pid.strip(), self.pidfile)
            self.logger.error(message)
            sys.exit(1)

    def stop(self):
        """Stop the daemon.
        """
        # Get the pid from pidfile.
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError as e:
            self.logger.error("No daemon runing, error: %s"  % e)
            return None
            # sys.exit(1)

        # Try killing daemon process.
        try:
            for pid in pids:
                logging.info('Trying to kill pid[%s]' % pid.strip())
                os.kill(int(pid.strip()), SIGTERM)
                self.logger.info('Killed pid[%s]' % pid.strip())
                time.sleep(1)
        except OSError as e:
            self.logger.error('Cannot kill process with pid[%s], error:%s' % (pid.strip(), e))
            # sys.exit(1)

        try:
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
        except IOError as e:
            self.logger.error("Can not remove pid file [%s]" % self.pidfile)
            # sys.exit(1)

    def restart(self):
        """Restart daemon.
        """
        self.stop()
        time.sleep(1)
        self.start()

    def run(self):
        """You should override this method when you subclass Daemon.
        
        It will be called after the process has been daemonized by start() or restart().
        """
        pass

    def init_redis(self):
        try:
            self.redis = redis.Redis(host=self.conf.get('redis', 'host'), port=self.conf.getint('redis', 'port'), db=self.conf.getint('redis', 'db'))
        except Exception,e:
            self.logger.error("try to connect redis[%s:%d] db[%d] error: %s" % (self.conf.get('redis', 'host'), self.conf.getint('redis', 'port'), self.conf.getint('redis', 'db'), e))
            sys.exit(1)

    def push_to_redis(self, hash_key, row):
        try:
            try_num = 3
            while(try_num > 0):
                try_num -= 1
                queue_length = self.redis.lpush(hash_key, row)
                if queue_length > 0:
                    self.binlog_logger.info(row)
                    return True
        except Exception,e:
            '''redis may timeout, reconnect redis'''
            try:
                self.redis = redis.Redis(host=self.conf.get('redis', 'host'), port=self.conf.getint('redis', 'port'), db=self.conf.getint('redis', 'db'))
            except Exception, e:
                self.logger.error("try to reconnect redis[%s:%d] db[%d] error: %s" % (self.conf.get('redis', 'host'), self.conf.getint('redis', 'port'), self.conf.getint('redis', 'db'), e))
                sys.exit(1)
            queue_length = self.redis.lpush(hash_key, row)
            if queue_length > 0:
                self.binlog_logger.info(row)
                return True
            self.logger.error("try to push to queue[%s] row[%s] on redis[%s:%d] db[%d] error: %s" % (self.conf.get('redis', 'queue_key'), row, self.conf.get('redis', 'host'), self.conf.getint('redis', 'port'), self.conf.getint('redis', 'db'), e))
            sys.exit(1)

        self.logger.error("try to push to queue[%s] row[%s] on redis[%s:%d] db[%d] error: %s" % (self.conf.get('redis', 'queue_key'), row, self.conf.get('redis', 'host'), self.conf.getint('redis', 'port'), self.conf.getint('redis', 'db'), e))
        return False
