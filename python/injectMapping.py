#!/bin/env python

import sys,traceback
import os
import datetime
import time

#from aUtils import *
import mappings

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import *

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
import simplejson as json
import socket

class elasticBandInjector:

    def __init__(self,es_server_url,subsys,update_run_mapping=True,update_box_mapping=True,update_logs_mapping=True):
        self.es_server_url=es_server_url
        self.runindex_write="runindex_"+subsys+"_write"
        self.runindex_read="runindex_"+subsys+"_read"
        self.runindex_name="runindex_"+subsys
        self.boxinfo_write="boxinfo_"+subsys+"_write"
        self.boxinfo_read="boxinfo_"+subsys+"_read"
        self.boxinfo_name="boxinfo_"+subsys
        self.hltdlogs_write="hltdlogs_"+subsys+"_write"
        self.hltdlogs_read="hltdlogs_"+subsys+"_read"
        self.hltdlogs_name="hltdlogs_"+subsys
        self.ip_url=None
        if update_run_mapping:
            self.updateIndexMaybe(self.runindex_name,self.runindex_write,self.runindex_read,mappings.central_es_settings,mappings.central_runindex_mapping)
        if update_box_mapping:
            self.updateIndexMaybe(self.boxinfo_name,self.boxinfo_write,self.boxinfo_read,mappings.central_es_settings,mappings.central_boxinfo_mapping)
        if update_logs_mapping:
            self.updateIndexMaybe(self.hltdlogs_name,self.hltdlogs_write,self.hltdlogs_read,mappings.central_es_settings_hltlogs,mappings.central_hltdlogs_mapping)
        #silence

    def updateIndexMaybe(self,index_name,alias_write,alias_read,settings,mapping):
        self.es = ElasticSearch(self.es_server_url,timeout=20)
        if requests.get(self.ip_url+'/_alias/'+alias_write).status_code == 200:
            print 'writing to elastic index '+alias_write + ' on '+self.es_server_url+' - '+self.ip_url
            self.createDocMappingsMaybe(alias_write,mapping)

    def createDocMappingsMaybe(self,index_name,mapping):
        #update in case of new documents added to mapping definition
        for key in mapping:
            doc = {key:mapping[key]}
            res = requests.get(self.ip_url+'/'+index_name+'/'+key+'/_mapping')
            #only update if mapping is empty
            if res.status_code==200:
                if res.content.strip()=='{}':
                    print 'inserting new mapping for '+str(key)
                    requests.post(self.ip_url+'/'+index_name+'/'+key+'/_mapping',json.dumps(doc))
                else:
                    #still check if number of properties is identical in each type
                    inmapping = json.loads(res.content)
                    for indexname in inmapping:
                        properties = inmapping[indexname]['mappings'][key]['properties']

                        print 'checking mapping '+ indexname + '/' + key + ' which has ' + str(len(mapping[key]['properties'])) + '(index:' + str(len(properties)) + ') entries..'
                        for pdoc in mapping[key]['properties']:
                            if pdoc not in properties:
                                print 'inserting mapping for ' + str(key) + ' which is missing mapping property ' + str(pdoc)
                                requests.post(self.ip_url+'/'+index_name+'/'+key+'/_mapping',json.dumps(doc))
                                break
            else:
                print 'requests error code '+res.status_code+' in mapping request'

if __name__ == "__main__":

    print "Elastic mapping injector. Parameters: server URL, subsystem name, all|run|box|log"
    url = sys.argv[1]
    if not url.startswith('http://'):url='http://'+url
    subsys = sys.argv[2]
    upd_run = sys.argv[3]=='all' or sys.argv[3]=='run'
    upd_box = sys.argv[3]=='all' or sys.argv[3]=='box'
    upd_log = sys.argv[3]=='all' or sys.argv[3]=='log'
    es = elasticBandInjector(url,subsys,upd_run,upd_box,upd_log)

    print "Quit"
    os._exit(0)
