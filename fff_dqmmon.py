import os
import sys
import logging
import re
import datetime
import subprocess
import socket
import time
import select
import json

logging.basicConfig(level=logging.INFO)
log = logging

import _inotify as inotify
import watcher

# minihack
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError

class DQMMonitor(object):
    def __init__(self, top_path, rescan_timeout=30):
        self.path = top_path
        self.rescan_timeout = rescan_timeout
        self.es = None

        self.create_index()

        try:
            os.makedirs(self.path)
        except OSError:
            pass

        self.mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
        self.w = watcher.Watcher()
        self.w.add(self.path, self.mask)

        self.process_dir()

    def create_index(self):
        self.index_name = "dqm_online_monitoring"
        self.settings = {
            "analysis": {
                "analyzer": {
                    "prefix-test-analyzer": {
                        "type": "custom",
                        "tokenizer": "prefix-test-tokenizer"
                    }
                },
                "tokenizer": {
                    "prefix-test-tokenizer": {
                        "type": "path_hierarchy",
                        "delimiter": "_"
                    }
                }
            },
            "index":{
                'number_of_shards' : 16,
                'number_of_replicas' : 1
            }
        }

        self.mappings = {
            'dqm-source-state' : {
                'properties' : {
                    'type' : {'type' : 'string' },
                    'pid' : { 'type' : 'integer' },
                    'hostname' : { 'type' : 'string' },
                    'sequence' : { 'type' : 'integer', "index" : "not_analyzed" },
                    'run' : { 'type' : 'integer' },
                    'lumi' : { 'type' : 'integer' },
                },
                '_timestamp' : { 'enabled' : True, 'store' : True, },
                '_ttl' : { 'enabled' : True, 'default' : '24h' }
            },
        }

        self.es = ElasticSearch("http://127.0.0.1:9200")
        try:
            self.es.create_index(self.index_name, settings={ 'settings': self.settings, 'mappings': self.mappings })
        except IndexAlreadyExistsError:
            pass

    def upload_file(self, fp):
        log.info("uploading: %s", fp)

        try:
            f = open(fp, "r")
            document = json.load(f)
            f.close()

            i = os.path.basename(fp)
            ret = self.es.index(self.index_name, "dqm-source-state", document, id=i)
            print ret
            
        finally:
            pass

    def process_file(self, fp):
        fname = os.path.basename(fp)

        if fname.startswith("."):
            return

        if not fname.endswith(".jsn"):
            return

        self.upload_file(fp)
        os.unlink(fp)

    def process_dir(self):
        for f in os.listdir(self.path):
            fp = os.path.join(self.path, f)
            self.process_file(fp) 

    def run(self):
        poll = select.poll()
        poll.register(self.w, select.POLLIN)
        poll.poll(self.rescan_timeout*1000)
        
        # clear the events
        for event in self.w.read(bufsize=0):
            pass
            #print event

        self.process_dir()

# use a named socket check if we are running
# this is very clean and atomic and leave no files
# from: http://stackoverflow.com/a/7758075
def lock(pname):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.bind('\0' + pname)
        return sock
    except socket.error:
        return None

def daemon(service, delay_seconds=30):
    while True:
        service.run()
   
import sys 
if __name__ == "__main__":
    # try to take the lock or quit
    sock = lock("fff_dqmmon")
    if sock is None:
        log.info("Already running, exitting.")
        sys.exit(0)

    service = DQMMonitor(
        top_path = "/tmp/dqmmon/",
    )

    #print service.upload_file(sys.argv[1])
    daemon(service)
