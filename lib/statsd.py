#!/usr/bin/env python

import copy
import csv
import re
import socket
import threading
import time
import timeit
import types
import logging
import signal

import mongodbInterface
from datetime import datetime
from subprocess import call
from warnings import warn

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None

__all__ = ['Server']

class Server(object):
    def __init__(self, hostname, port=8125):
    	self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        assert type(port) is int, 'port is not an integer: %s' % (port)
        addr = (hostname, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(addr)
        self.counters = {}
        self.timers = {}
        self.gauges = {}
        self.flusher = 0
        self.buf = 10240
        self.batch = {}
        self.pre_batch = {}
        self.cur_batch = {}
        self.mongo = mongodbInterface.MongoDBClient()
        self.mongo.db_con(host='localhost', username='admin', password='admin', port=27017, database='test',collection='table')
        #self.mongo.export2disk(filename="aaa", file_format='csv')
        self.version = '0.1'
        self.batch['db_version'] = self.version

    def process(self, data):
        self.logger.debug("Processing statsd.")
        # the data is a sequence of newline-delimited metrics
        # a metric is in the form "name:value|rest"  (rest may have more pipes)
        data.rstrip('\n')

        for metric in data.split('\n'):
            if metric == "":
                continue;
            match = re.match('\A([^:]+):([^|]+)\|(.+)', metric)
            #print "metric: {}".format(metric)
            if match == None:
                self.logger.warn("Skipping malformed metric: %s", metric)
                print "metric: {}".format(metric)
                continue

            key   = match.group(1)
            value = match.group(2)
            rest  = match.group(3).split('|')
            mtype = rest.pop(0)
            if mtype == 'c':
                print "metric: {}".format(metric)
            self.__key_value_update(key, value, mtype, rest)

    def __key_value_update(self, key, value, mtype, rest):
        self.logger.debug("Updating {key}|{mtype}:{value}".format(key=key, mtype=mtype,value=value))
        count = 0
        key_levels = key.split('.')
        key_levels_length = len(key_levels)
        tmp = self.batch
        for level in key_levels:
            count += 1
            try:
                tmp = tmp[level]
                if count == key_levels_length:
                    self.__update_value(tmp, value, mtype, rest)
            except KeyError:
                sub_levels = level.split(',')
                if len(sub_levels) > 1:
                    tmp_pointer = tmp
                    for sub_level in sub_levels:
                        tmp_subkey, tmp_subvalue = sub_level.split('=')
                        try:
                            tmp = tmp[tmp_subkey][tmp_subvalue]
                        except KeyError:
                            if tmp_subkey in tmp:
                                tmp[tmp_subkey][tmp_subvalue] = {}
                            else:
                                tmp[tmp_subkey] = {tmp_subvalue: {}}
                            tmp = tmp[tmp_subkey][tmp_subvalue]
                else:
                    if count == key_levels_length:
                        tmp[sub_levels[0]] = {};
                        self.__update_value(tmp[sub_levels[0]], value, mtype, rest)
                    else:
                        tmp[sub_levels[0]] = {}
                    tmp = tmp[sub_levels[0]]

    def __finditem(obj, key):
        self.logger.debug("Find item corresponding to {}.".format(key))
        if key in obj:
            return obj[key]
        for k, v in obj.items():
            if isinstance(v,dict):
                item = _finditem(v, key)
                if item is not None:
                    return item

    def __update_value(self, obj, value, mtype, rest):
        self.logger.debug("Updating key|{mtype}:{value}".format(mtype=mtype,value=value))
        #if   (mtype == 'ms'): self.__record_timer(level, value, rest)
        if   (mtype == 'g' ): obj['value'] = float(value)
        elif (mtype == 'c' ): obj['value'] = float(value or 1)
        #elif (mtype == 'c' ): obj['value'] += float(value or 1)
        else:
            self.logger.warn("Encountered unknown metric type in <%s>" % (mtype))

    def __record_timer(self, key, value, rest):
        ts = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        timer = self.timers.setdefault(key, [ [], ts ])
        timer[0].append(float(value or 0))
        timer[1] = ts


    def __record_counter(self, key, value, rest):
        ts = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        sample_rate = 1.0
        if len(rest) == 1:
            sample_rate = float(re.match('^@([\d\.]+)', rest[0]).group(1))
            if sample_rate == 0:
                warn("Ignoring counter with sample rate of zero: <%s>" % (metric))
                return

        counter = self.counters.setdefault(key, [ 0, ts ])
        counter[0] += float(value or 1) * (1 / sample_rate)
        counter[1] = ts

    def start(self, hostname='', port=8125, listen_interval=20, client_ip=None):
        start_time = timeit.default_timer()
        def signal_handler(signal, frame):
            self.stop()
        while 1:
            elapsed = timeit.default_timer() - start_time
            if elapsed > listen_interval:
                time.sleep(listen_interval)
                self.batch['timestamp'] = datetime.utcnow()
                self.mongo.insert_one(copy.deepcopy(self.batch))
                print "pushed to database from port: {}".format(threading.currentThread().getName())
                start_time = timeit.default_timer()
            else:
                data, addr = self._sock.recvfrom(self.buf)
                try:
                    self.process(data)
                except Exception as error:
                    logging.error("Bad data from %s: %s", addr, error)
            
    def stop(self):
        #self._timer.cancel()
        self._sock.close()

if __name__ == '__main__':
    log_format = '[%(asctime)-15s][%(name)-10s] %(levelname)-8s: %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)
    
    ports = [8125]

    threads = []
    for port in ports:
        server = Server(hostname='127.0.0.1', port=port)
        t = threading.Thread(name=str(port), target=server.start)
        #t.daemon = True
        threads.append(t)
        t.start()
    #server = Server(hostname='131.160.80.65')
    #server.start()

