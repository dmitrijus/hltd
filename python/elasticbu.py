#!/bin/env python

import sys,traceback
import os
import datetime
import time

import logging
import _inotify as inotify
import threading
import Queue

from hltdconf import *
from aUtils import *
import mappings

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import *
import csv

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
import simplejson as json
import socket

#silence HTTP connection info from requests package
logging.getLogger("urllib3").setLevel(logging.WARNING)

GENERICJSON,SUMMARYJSON = range(2) #injected JSON range

def getURLwithIP(url,nsslock=None):
  try:
      prefix = ''
      if url.startswith('http://'):
          prefix='http://'
          url = url[7:]
      suffix=''
      port_pos=url.rfind(':')
      if port_pos!=-1:
          suffix=url[port_pos:]
          url = url[:port_pos]
  except Exception as ex:
      logging.error('could not parse URL ' +url)
      raise(ex)
  if url!='localhost':
      if nsslock is not None:
          try:
              nsslock.acquire()
              ip = socket.gethostbyname(url)
              nsslock.release()
          except Exception as ex:
              try:nsslock.release()
              except:pass
              raise ex
      else:
          ip = socket.gethostbyname(url)
  else: ip='127.0.0.1'

  return prefix+str(ip)+suffix


class elasticBandBU:

    def __init__(self,conf,runnumber,startTime,runMode=True,nsslock=None,box_version=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conf=conf
        self.es_server_url=conf.elastic_runindex_url
        self.runindex_write="runindex_"+conf.elastic_runindex_name+"_write"
        self.runindex_read="runindex_"+conf.elastic_runindex_name+"_read"
        self.runindex_name="runindex_"+conf.elastic_runindex_name
        self.boxinfo_write="boxinfo_"+conf.elastic_runindex_name+"_write"
        self.boxinfo_read="boxinfo_"+conf.elastic_runindex_name+"_read"
        self.boxinfo_name="boxinfo_"+conf.elastic_runindex_name
        self.boxdoc_version=box_version
        self.runnumber = str(runnumber)
        self.startTime = startTime
        self.host = os.uname()[1]
        self.stopping=False
        self.threadEvent = threading.Event()
        self.runMode=runMode
        self.boxinfoFUMap = {}
        self.ip_url=None
        self.nsslock=nsslock
        self.updateIndexMaybe(self.runindex_name,self.runindex_write,self.runindex_read,mappings.central_es_settings,mappings.central_runindex_mapping)
        self.updateIndexMaybe(self.boxinfo_name,self.boxinfo_write,self.boxinfo_read,mappings.central_es_settings,mappings.central_boxinfo_mapping)
        self.black_list=None
        if self.conf.instance=='main':
            self.hostinst = self.host
        else:
            self.hostinst = self.host+'_'+self.conf.instance

        #write run number document
        if runMode == True and self.stopping==False:
            document = {}
            document['runNumber'] = self.runnumber
            document['startTime'] = startTime
            documents = [document]
            self.index_documents('run',documents)
            #except ElasticHttpError as ex:
            #    self.logger.info(ex)
            #    pass


    def updateIndexMaybe(self,index_name,alias_write,alias_read,settings,mapping):
        connectionAttempts=0
        retry=False
        while True:
            if self.stopping:break
            connectionAttempts+=1
            try:
                if retry or self.ip_url==None:
                    self.ip_url=getURLwithIP(self.es_server_url,self.nsslock)
                    self.es = ElasticSearch(self.ip_url,timeout=20)

                #check if runindex alias exists
                if requests.get(self.ip_url+'/_alias/'+alias_write).status_code == 200: 
                    self.logger.info('writing to elastic index '+alias_write + ' on '+self.es_server_url+' - '+self.ip_url )
                    self.createDocMappingsMaybe(alias_write,mapping)
                    break
                else:
                    time.sleep(.5)
                    if (connectionAttempts%10)==0:
                        self.logger.error('unable to access to elasticsearch alias ' + alias_write + ' on '+self.es_server_url+' / '+self.ip_url)
                    continue
            except ElasticHttpError as ex:
                #es error, retry
                self.logger.error(ex)
                if self.runMode and connectionAttempts>100:
                    self.logger.error('elastic (BU): exiting after 100 ElasticHttpError reports from '+ self.es_server_url)
                    sys.exit(1)
                elif self.runMode==False and connectionAttempts>10:
                    self.threadEvent.wait(60)
                else:
                    self.threadEvent.wait(1)
                retry=True
                continue

            except (socket.gaierror,ConnectionError,Timeout,RequestsConnectionError,RequestsTimeout) as ex:
                #try to reconnect with different IP from DNS load balancing
                if self.runMode and connectionAttempts>100:
                   self.logger.error('elastic (BU): exiting after 100 connection attempts to '+ self.es_server_url)
                   sys.exit(1)
                elif self.runMode==False and connectionAttempts>10:
                   self.threadEvent.wait(60)
                else:
                   self.threadEvent.wait(1)
                retry=True
                continue

    def createDocMappingsMaybe(self,index_name,mapping):
        #update in case of new documents added to mapping definition
        for key in mapping:
            doc = {key:mapping[key]}
            res = requests.get(self.ip_url+'/'+index_name+'/'+key+'/_mapping')
            #only update if mapping is empty
            if res.status_code==200:
                if res.content.strip()=='{}':
                    self.logger.info('inserting new mapping for '+str(key))
                    requests.post(self.ip_url+'/'+index_name+'/'+key+'/_mapping',json.dumps(doc))
                else:
                    #still check if number of properties is identical in each type
                    inmapping = json.loads(res.content)
                    for indexname in inmapping:
                        properties = inmapping[indexname]['mappings'][key]['properties']

                        self.logger.info('checking mapping '+ indexname + '/' + key + ' which has '
                            + str(len(mapping[key]['properties'])) + '(index:' + str(len(properties)) + ') entries..')
                        for pdoc in mapping[key]['properties']:
                            if pdoc not in properties:
                                self.logger.info('inserting mapping for ' + str(key) + ' which is missing mapping property ' + str(pdoc))
                                requests.post(self.ip_url+'/'+index_name+'/'+key+'/_mapping',json.dumps(doc))
                                #self.logger.info(str(json.dumps(doc)))
                                break
            else:
                self.logger.warning('requests error code '+res.status_code+' in mapping request')

    def read_line(self,fullpath):
        with open(fullpath,'r') as fp:
            return fp.readline()
    
    def elasticize_modulelegend(self,fullpath):

        self.logger.info(os.path.basename(fullpath))
        stub = self.read_line(fullpath)
        document = {}
        document['_parent']= self.runnumber
        document['id']= "microstatelegend_"+self.runnumber
        document['names']= self.read_line(fullpath)
        documents = [document]
        return self.index_documents('microstatelegend',documents)


    def elasticize_pathlegend(self,fullpath):

        self.logger.info(os.path.basename(fullpath))
        stub = self.read_line(fullpath)
        document = {}
        document['_parent']= self.runnumber
        document['id']= "pathlegend_"+self.runnumber
        document['names']= self.read_line(fullpath)
        documents = [document]
        return self.index_documents('pathlegend',documents)

    def elasticize_stream_label(self,infile):
        #elasticize stream name information
        self.logger.info(infile.filepath)
        document = {}
        document['_parent']= self.runnumber
        document['stream']=infile.stream[6:]
        document['id']=infile.basename
        return self.index_documents('stream_label',[document])

    def elasticize_runend_time(self,endtime):

        self.logger.info(str(endtime)+" going into buffer")
        document = {}
        document['runNumber'] = self.runnumber
        document['startTime'] = self.startTime
        document['endTime'] = endtime
        documents = [document]
        self.index_documents('run',documents)

    def elasticize_resource_summary(self,jsondoc):
        self.logger.debug('injecting resource summary document')
        jsondoc['appliance']=self.host
        self.index_documents('resource_summary',[jsondoc],bulk=False)

    def elasticize_box(self,infile):

        basename = infile.basename
        self.logger.debug(basename)
        current_time = time.time()

        if infile.data=={}:return

        bu_doc=False
        if basename.startswith('bu') or basename.startswith('dvbu'):
            bu_doc=True

        #check box file against blacklist
        if bu_doc or self.black_list==None:
            self.black_list=[]

            try:
                with open(os.path.join(self.conf.watch_directory,'appliance','blacklist'),"r") as fi:
                    try:
                        self.black_list = json.load(fi)
                    except ValueError:
                        #file is being written or corrupted
                        return
            except:
                #blacklist file is not present, do not filter
                pass

        if basename in self.black_list:return

        if bu_doc==False:
            try:
                if self.boxdoc_version<infile.data['version']:
                    self.logger.info('skipping '+basename+' box file version '+str(infile.data['version'])+' which is newer than '+str(self.boxdoc_version))
                    return;
            except:
                self.logger.warning("didn't find version field in box file "+basename)
                return
            try:
                self.boxinfoFUMap[basename] = [infile.data,current_time]
            except Exception as ex:
                self.logger.warning('box info not injected: '+str(ex))
                return
        try:
            document = infile.data
            #unique id for separate instances
            if bu_doc:
                document['id']=self.hostinst
            else:
                document['id']=basename

            document['activeRuns'] = str(document['activeRuns']).strip('[]')
            document['appliance']=self.host
            document['instance']=self.conf.instance
            #only here
            document['host']=basename
            try:document.pop('version')
            except:pass
            self.index_documents('boxinfo',[document])
        except Exception as ex:
            self.logger.warning('box info not injected: '+str(ex))
            return

    def elasticize_eols(self,infile):
        basename = infile.basename
        self.logger.info(basename)
        data = infile.data['data']
        data.insert(0,infile.mtime)
        data.insert(0,infile.ls[2:])
        
        values = [int(f) if f.isdigit() else str(f) for f in data]
        keys = ["ls","fm_date","NEvents","NFiles","TotalEvents","NLostEvents"]
        document = dict(zip(keys, values))

        document['id'] = infile.name+"_"+os.uname()[1]
        document['_parent']= self.runnumber
        documents = [document]
        self.index_documents('eols',documents)

    def index_documents(self,name,documents,bulk=True):
        attempts=0
        destination_index = ""
        is_box=False
        if name.startswith("boxinfo") or name=='resource_summary':
          destination_index = self.boxinfo_write
          is_box=True
        else:
          destination_index = self.runindex_write
        while True:
            attempts+=1
            try:
                if bulk:
                    self.es.bulk_index(destination_index,name,documents)
                else:
                    self.es.index(destination_index,name,documents[0])
                return True
            except ElasticHttpError as ex:
                if attempts<=1:continue
                self.logger.error('elasticsearch HTTP error'+str(ex)+'. skipping document '+name)
                if is_box==True:break
                #self.logger.exception(ex)
                return False
            except (socket.gaierror,ConnectionError,Timeout) as ex:
                if attempts>100 and self.runMode:
                    raise(ex)
                self.logger.error('elasticsearch connection error' + str(ex)+'. retry.')
                if self.stopping:return False
                ip_url=getURLwithIP(self.es_server_url,self.nsslock)
                self.es = ElasticSearch(ip_url,timeout=20)
                time.sleep(0.1)
                if is_box==True:break
        return False
             

class elasticCollectorBU():
    
    def __init__(self, es, inRunDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        

        self.insertedModuleLegend = False
        self.insertedPathLegend = False
	self.inRunDir=inRunDir
        
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()
        self.source = False
        self.infile = False
	self.es=es

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
	self.logger.info("elasticCollectorBU: start main loop (monitoring:"+self.inRunDir+")")
	count = 0
	while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
	    if self.source:
		try:
		    event = self.source.get(True,1.0) #blocking with timeout
		    self.eventtype = event.mask
		    self.infile = fileHandler(event.fullpath)
		    self.emptyQueue.clear()
		    if self.infile.filetype==EOR:
                        if self.es:
                            try:
                                dt=os.path.getctime(event.fullpath)
                                endtime = datetime.datetime.utcfromtimestamp(dt).isoformat()
                                self.es.elasticize_runend_time(endtime)
                            except Exception as ex:
                                self.logger.warning(str(ex))
                                endtime = datetime.datetime.utcnow().isoformat()
                                self.es.elasticize_runend_time(endtime)
                        break
                    self.process()
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set()
                except (ValueError,IOError) as ex:
                    self.logger.exception(ex)
                except Exception as ex:
                    self.logger.exception(ex)
            else:
                time.sleep(1.0)
            #check for run directory file every 5 loops
            count+=1
            if (count%5) == 0:
                #if run dir deleted
                if os.path.exists(self.inRunDir)==False:
                    self.logger.info("Exiting because run directory in has disappeared")
                    if self.es:
                        #write end timestamp in case EoR file was not seen
                        endtime = datetime.datetime.utcnow().isoformat()
                        self.es.elasticize_runend_time(endtime)
                    break
	self.logger.info("Stop main loop (watching directory " + str(self.inRunDir) + ")")


    def setSource(self,source):
        self.source = source


    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if self.es and eventtype & (inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO):
            if filetype in [MODULELEGEND] and self.insertedModuleLegend == False:
                if self.es.elasticize_modulelegend(filepath):
                    self.insertedModuleLegend = True
            elif filetype in [PATHLEGEND] and self.insertedPathLegend == False:
                if self.es.elasticize_pathlegend(filepath):
                    self.insertedPathLegend = True
            elif filetype == INI:
                self.es.elasticize_stream_label(self.infile)
            elif filetype == EOLS:
                self.es.elasticize_eols(self.infile)
            elif filetype == EOR:
                self.es.elasticize_eor(self.infile)

class JsonEvent:
    def __init__(self,json,eventtype):
        self.eventtype = eventtype
        self.json = json

class elasticBoxCollectorBU():

    def __init__(self,esbox):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()
        self.source = False
        self.infile = False
        self.es = esbox
        self.dropThreshold=0

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
	self.logger.info("elasticBoxCollectorBU: start main loop")
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
            if self.source:
                try:
                    event = self.source.get(True,1.0) #blocking with timeout
                    self.emptyQueue.clear()
                    if self.dropThreshold>0 and self.source.qsize()>self.dropThreshold:
                        self.logger.info('box queue size reached: '+str(self.source.qsize())+' - dropping event from queue.')
                        continue
                    if isinstance(event,JsonEvent):
                        self.processInjected(event) 
                    else:
                        self.eventtype = event.mask
                        self.infile = fileHandler(event.fullpath)
                        self.process() 

                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set()
                except ValueError as ex:
                    self.logger.exception(ex)
                except IOError as ex:
                    self.logger.warning("IOError on reading "+event.fullpath)
            else:
                time.sleep(1.0)
        self.logger.info("elasticBoxCollectorBU: stop main loop")

    def setSource(self,source,dropThreshold=0):
        self.source = source
        #threshold is used to control queue size for boxinfo injection in case main server is down
        self.dropThreshold=dropThreshold

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if filetype == BOX:
            #self.logger.info(self.infile.basename)
            self.es.elasticize_box(self.infile)

    def injectJson(self,json,eventtype):
        self.source.put(JsonEvent(json,eventtype))

    def injectSummaryJson(self,json):
        self.injectJson(json,SUMMARYJSON)

    def processInjected(self,event):
        if event.eventtype==SUMMARYJSON:
            self.es.elasticize_resource_summary(event.json)

class BoxInfoUpdater(threading.Thread):

    def __init__(self,ramdisk,conf,nsslock,box_version):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stopping = False
        self.es=None
        self.conf=conf
        self.nsslock=nsslock
        self.boxdoc_version = box_version

        try:
            threading.Thread.__init__(self)
            self.threadEvent = threading.Event()

            boxesDir =  os.path.join(ramdisk,'appliance/boxes')
            boxesMask = inotify.IN_CLOSE_WRITE 
            self.logger.info("starting elastic for "+boxesDir)

            self.eventQueue = Queue.Queue()
            self.mr = MonitorRanger()
            self.mr.setEventQueue(self.eventQueue)
            self.mr.register_inotify_path(boxesDir,boxesMask)

        except Exception,ex:
            self.logger.exception(ex)

    def run(self):
        try:
            self.es = elasticBandBU(self.conf,0,'',False,self.nsslock,self.boxdoc_version)
            if self.stopping:return

            self.ec = elasticBoxCollectorBU(self.es)
            #keep up to 200 box file updates in queue
            self.ec.setSource(self.eventQueue,dropThreshold=200)

            self.mr.start_inotify()
            self.ec.start()
        except Exception,ex:
            self.logger.exception(ex)

    def stop(self):
        try:
            self.stopping=True
            self.threadEvent.set()
            if self.es is not None:
                self.es.stopping=True
                self.es.threadEvent.set()
            if self.mr is not None:
                self.mr.stop_inotify()
            if self.ec is not None:
                self.ec.stop()
            self.join()
        except RuntimeError,ex:
            pass
        except Exception,ex:
            self.logger.exception(ex)

class RunCompletedChecker(threading.Thread):

    def __init__(self,conf,run):
        self.logger = logging.getLogger(self.__class__.__name__)
        threading.Thread.__init__(self)
        self.conf = conf
        self.run = run
        rundirstr = 'run'+ str(run.runnumber).zfill(conf.run_number_padding)
        self.indexPrefix = rundirstr + '_' + conf.elastic_cluster
        self.url =       'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/fu-complete/_count'
        self.urlclose =  'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/_close'
        self.urlsearch = 'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/fu-complete/_search?size=1'
        self.url_query = '{  "query": { "filtered": {"query": {"match_all": {}}}}, "sort": { "fm_date": { "order": "desc" }}}'
        self.stop = False
        self.threadEvent = threading.Event()

    def run(self):

        check_es_complete=True
        total_es_elapsed=0

        while self.stop==False:

            if check_es_complete:
                try:
                    resp = requests.post(self.url, '',timeout=5)
                    data = json.loads(resp.content)
                    if int(data['count']) >= len(self.run.online_resource_list):
                        try:
                            respq = requests.post(self.urlsearch,self.url_query,timeout=5)
                            dataq = json.loads(respq.content)
                            fm_time = str(dataq['hits']['hits'][0]['_source']['fm_date'])
                            #fill in central index completition time
                            postq = "{runNumber\":\"" + str(self.nr) + "\",\"completedTime\" : \"" + fm_time + "\"}"
                            requests.post(self.conf.elastic_runindex_url+'/'+"runindex_"+self.conf.elastic_runindex_name+'_write/run',postq,timeout=5)
                            self.logger.info("filled in completition time for run "+str(self.nr))
                        except IndexError:
                            # 0 FU resources present in this run, skip writing completition time
                            pass 
                        except Exception as ex:
                            self.logger.exception(ex)
                        check_es_complete=False
                        continue
                    else:
                        time.sleep(5)
                        total_es_elapsed+=5
                        if total_es_elapsed>600:
                            self.logger.warning('run index complete flag was not written by all FUs, giving up checks after 10 minutes.')
                            check_es_complete=False
                            continue
                except Exception,ex:
                    self.logger.error('Error in run completition check')
                    self.logger.exception(ex)
                    check_es_complete=False

            #exit if both checks are complete
            if check_es_complete==False:
                try:
                    if self.conf.close_es_index==True:
                        #wait a bit for queries to complete
                        time.sleep(10)
                        resp = requests.post(self.urlclose,timeout=5)
                        self.logger.info('closed appliance ES index for run '+str(self.nr))
                except Exception as exc:
                    self.logger.error('Error in closing run index')
                    self.logger.exception(exc)
                break
            #check every 10 seconds
            self.threadEvent.wait(10)

    def stop(self):
        self.stop = True
        self.threadEvent.set() 


if __name__ == "__main__":

    import procname
    procname.setprocname('elasticbu')

    conf=initConf(sys.argv[1])

    logging.basicConfig(filename=os.path.join(conf.log_dir,"elasticbu.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()

    runnumber = sys.argv[2]
    watchdir = conf.watch_directory
    mainDir = os.path.join(watchdir,'run'+ runnumber.zfill(conf.run_number_padding))
    dt=os.path.getctime(mainDir)
    startTime = datetime.datetime.utcfromtimestamp(dt).isoformat()
    #EoR file path to watch for

    mainMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
    monDir = os.path.join(mainDir,"mon")
    monMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO

    logger.info("starting elastic for "+mainDir)
    logger.info("starting elastic for "+monDir)

    try:
        logger.info("try create input mon dir " + monDir)
        os.mkdir(monDir)
    except OSError,ex:
        logger.info(ex)
        pass

    mr = None
    try:
        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        mr.register_inotify_path(monDir,monMask)
        mr.register_inotify_path(mainDir,mainMask)

        mr.start_inotify()

        es = elasticBandBU(conf,runnumber,startTime)

        #starting elasticCollector thread
        ec = elasticCollectorBU(es,mainDir)
        ec.setSource(eventQueue)
        ec.start()

    except Exception as e:
        logger.exception(e)
        print traceback.format_exc()
        logger.error("when processing files from directory "+mainDir)

    logging.info("Closing notifier")
    if mr is not None:
      mr.stop_inotifyTimeout(1)

    logging.info("Quit")
    os._exit(0)

