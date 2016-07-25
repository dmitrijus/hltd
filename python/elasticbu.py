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
from daemon2 import stdOutLog,stdErrorLog
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
        raise ex
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

    def __init__(self,conf,runnumber,startTime,runMode=True,nsslock=None,box_version=None,update_run_mapping=True,update_box_mapping=True):
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
        if update_run_mapping:
            self.updateIndexMaybe(self.runindex_name,self.runindex_write,self.runindex_read,mappings.central_es_settings,mappings.central_runindex_mapping)
        if update_box_mapping:
            self.updateIndexMaybe(self.boxinfo_name,self.boxinfo_write,self.boxinfo_read,mappings.central_es_settings,mappings.central_boxinfo_mapping)
        #silence
        eslib_logger = logging.getLogger('elasticsearch')
        eslib_logger.setLevel(logging.ERROR)

        self.black_list=None
        if self.conf.instance=='main':
            self.hostinst = self.host
        else:
            self.hostinst = self.host+'_'+self.conf.instance

        #this naturally fits with the 'run' document
        try:
            version = None
            arch = None
            with open(os.path.join(mainDir,'hlt',conf.paramfile_name),'r') as fp:
                fffparams = json.load(fp)
                version = fffparams['CMSSW_VERSION']
                arch = fffparams['SCRAM_ARCH']
        except:
          pass

        #write run number document
        if runMode == True and self.stopping==False:
            document = {}
            doc_id = self.runnumber
            document['runNumber'] = doc_id
            document['startTime'] = startTime
            document['activeBUs'] = 1
            document['totalBUs'] = 1
            document['rawDataSeenByHLT']=False
            if version: document['CMSSW_version']=version
            if arch: document['CMSSW_arch']=arch
            documents = [document]
            ret = self.index_documents('run',documents,doc_id,bulk=False,overwrite=False)
            if isinstance(ret,tuple) and ret[1]==409:
                #run document was already created by another BU. In that case increase atomically active BU counter
                self.index_documents('run',[{"script":"ctx._source.activeBUs+=1;ctx._source.totalBUs+=1"}],doc_id,bulk=False,update_only=True,script=True,retry_on_conflict=300)


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

                #check if index alias exists
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
                                if res.status_code!=200: self.logger.warning('insert mapping reply status code '+str(res.status_code)+': '+res.content)
                                break
            else:
                self.logger.warning('requests error code '+res.status_code+' in mapping request')

    def read_line(self,fullpath):
        with open(fullpath,'r') as fp:
            return fp.readline()

    def elasticize_modulelegend(self,fullpath):

        self.logger.info(os.path.basename(fullpath))
        document = {}
        #document['_parent']= self.runnumber
        doc_id="microstatelegend_"+self.runnumber
        if fullpath.endswith('.jsn'):
            try:
                with open(fullpath,'r') as fp:
                    doc = json.load(fp)
                    document['stateNames'] = doc['names']
                    try:document['reserved'] = doc['reserved']
                    except:document['reserved'] = 33
                    try:document['special'] = doc['special']
                    except:document['special'] = 7
                    nstring = ""
                    cnt = 0
                    outputcnt = 0
                    #fill in also old format for now
                    for sname in doc['names']:
                        nstring+= str(cnt) + "=" + sname + " "
                        cnt+=1
                        if sname.startswith('hltOutput'):outputcnt+=1
                    try:document['output'] = doc['output']
                    except:document['output']=outputcnt
                    #document['names'] = nstring
            except Exception as ex:
                self.logger.warning("can not parse "+fullpath + ' ' + str(ex))
        else:
            #old format
            stub = self.read_line(fullpath)
            docnames= self.read_line(fullpath)
            document['reserved'] = 33
            document['special'] = 7
            outputcnt=0
            for sname in docnames.split():
                if "=hltOutput" in sname: outputcnt+=1
            document['output'] = outputcnt
            document['stateNames']=[]
            nameTokens = docnames.split()
            for nameToken in nameTokens:
                if '=' in nameToken:
                    idx,sn = nameToken.split('=')
                    document["stateNames"].append( sn )

        documents = [document]
        doc_pars = {"parent":str(self.runnumber)}
        return self.index_documents('microstatelegend',documents,doc_id,doc_params=doc_pars,bulk=False)


    def elasticize_pathlegend(self,fullpath):
        self.logger.info(os.path.basename(fullpath))
        document = {}
        #document['_parent']= self.runnumber
        doc_id="pathlegend_"+self.runnumber
        if fullpath.endswith('.jsn'):
            try:
                with open(fullpath,'r') as fp:
                    doc = json.load(fp)
                    document['stateNames'] = doc['names']
                    document['reserved'] = doc['reserved']
                    #put old name format value
                    nstring=""
                    cnt=0
                    for sname in doc['names']:
                        nstring+= str(cnt) + "=" + sname + " "
                        cnt+=1
                    document['names'] = nstring
            except Exception as ex:
                self.logger.warning("can not parse "+fullpath)
        else:
            stub = self.read_line(fullpath)
            document['names']= self.read_line(fullpath)
        documents = [document]
        doc_pars = {"parent":str(self.runnumber)}
        return self.index_documents('pathlegend',documents,doc_id,doc_params=doc_pars,bulk=False)

    def elasticize_stream_label(self,infile):
        #elasticize stream name information
        self.logger.info(infile.filepath)
        document = {}
        #document['_parent']= self.runnumber
        document['stream']=infile.stream[6:]
        doc_id=infile.basename
        doc_pars = {"parent":str(self.runnumber)}
        return self.index_documents('stream_label',[document],doc_id,doc_params=doc_pars,bulk=False)

    def elasticize_runend_time(self,endtime):
        self.logger.info(str(endtime)+" going into buffer")
        doc_id = self.runnumber
        #first update: endtime field
        self.index_documents('run',[{"endTime":endtime}],doc_id,bulk=False,update_only=True)
        #second update:decrease atomically active BU counter
        self.index_documents('run',[{"script":"ctx._source.activeBUs-=1"}],doc_id,bulk=False,update_only=True,script=True,retry_on_conflict=300)

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
                doc_id=self.hostinst
            else:
                doc_id=basename

            document['id']=doc_id
            try:
              document['activeRunList'] = map(int,document['activeRuns'])
            except:
              pass
            try:
              document['activeRuns'] = map(str,document['activeRuns'])
            except:
              pass
            document['appliance']=self.host
            document['instance']=self.conf.instance
            if bu_doc==True:
                document['blacklist']=self.black_list
            #only here
            document['host']=basename
            try:document.pop('version')
            except:pass
            try:document.pop('ip')
            except:pass
            try:document.pop('boot_id')
            except:pass
            self.index_documents('boxinfo',[document],doc_id,bulk=False)
        except Exception as ex:
            self.logger.warning('box info not injected: '+str(ex))
            return

    def elasticize_fubox(self,doc):
        try:
            doc_id = self.host
            doc['host']=doc_id
            self.index_documents('fu-box-status',[doc],doc_id,bulk=False)
        except Exception as ex:
            self.logger.warning('fu box status not injected: '+str(ex))

    def elasticize_eols(self,infile):
        basename = infile.basename
        self.logger.info(basename)
        data = infile.data['data']
        data.insert(0,infile.mtime)
        data.insert(0,infile.ls[2:])

        values = [int(f) if f.isdigit() else str(f) for f in data]
        try:
            keys = ["ls","fm_date","NEvents","NFiles","TotalEvents","NLostEvents","NBytes"]
            document = dict(zip(keys, values))
        except:
            #try without NBytes
            keys = ["ls","fm_date","NEvents","NFiles","TotalEvents","NLostEvents"]
            document = dict(zip(keys, values))

        doc_id = infile.name+"_"+self.host
        document['id'] = doc_id
        #document['_parent']= self.runnumber
        document['appliance']=self.host
        documents = [document]
        doc_pars = {"parent":str(self.runnumber)}
        self.index_documents('eols',documents,doc_id,doc_params=doc_pars,bulk=False)

    def index_documents(self,name,documents,doc_id=None,doc_params=None,bulk=True,overwrite=True,update_only=False,retry_on_conflict=0,script=False):

        if name=='fu-box-status' or name.startswith("boxinfo") or name=='resource_summary':
            destination_index = self.boxinfo_write
            is_box=True
        else:
            destination_index = self.runindex_write
            is_box=False
        attempts=0
        while True:
            attempts+=1
            try:
                if bulk:
                    self.es.bulk_index(destination_index,name,documents)
                else:
                    if doc_id:
                      if update_only:
                        if script:
                          self.es.update(index=destination_index,doc_type=name,id=doc_id,script=documents[0],upsert=False,retry_on_conflict=retry_on_conflict)
                        else:
                          self.es.update(index=destination_index,doc_type=name,id=doc_id,doc=documents[0],upsert=False,retry_on_conflict=retry_on_conflict)
                      else:
                        #overwrite existing can be used with id specified
                        if doc_params:
                          self.es.index(destination_index,name,documents[0],doc_id,parent=doc_params['parent'],overwrite_existing=overwrite)
                        else:
                          self.es.index(destination_index,name,documents[0],doc_id,overwrite_existing=overwrite)
                    else:
                        self.es.index(destination_index,name,documents[0])
                return True

            except ElasticHttpError as ex:
                if name=='run' and ex[0]==409: #create failed because overwrite was forbidden
                    return (False,ex[0])

                if ex[0]==429:
                  if attempts<10 and not is_box:
                    self.logger.warning('elasticsearch HTTP error 429'+str(ex)+'. retrying..')
                    time.sleep(.1)
                    continue
                else:
                  if attempts<=1 and not is_box:continue

                if is_box:
                    self.logger.warning('elasticsearch HTTP error '+str(ex)+'. skipping document '+name)
                else:
                    self.logger.error('elasticsearch HTTP error '+str(ex)+'. skipping document '+name)
                return False
            except (socket.gaierror,ConnectionError,Timeout) as ex:
                if attempts>100 and self.runMode:
                    raise(ex)
                if is_box or attempts<=1:
                    self.logger.warning('elasticsearch connection error' + str(ex)+'. retry.')
                elif (attempts-2)%10==0:
                    self.logger.error('elasticsearch connection error' + str(ex)+'. retry.')
                if self.stopping:return False
                ip_url=getURLwithIP(self.es_server_url,self.nsslock)
                self.es = ElasticSearch(ip_url,timeout=20)
                time.sleep(0.1)
                if is_box==True:#give up on too many box retries as they are indexed again every 5 seconds
                  break
        return False


class elasticCollectorBU():

    def __init__(self, es, inRunDir, outRunDir):
        self.logger = logging.getLogger(self.__class__.__name__)


        self.insertedModuleLegend = False
        self.insertedPathLegend = False
        self.inRunDir=inRunDir
        self.outRunDir=outRunDir

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
                        #check if event content is 0 and create output dir (triggers deletion in case of no FUs available)
                        try:
                            with open(event.fullpath,'r') as feor:
                              if int(json.load(feor)['data'][0])==0:
                                  self.makeOutputDir()
                        except Exception as ex:
                            self.logger.warning('unable to parse EoR file content '+str(ex))
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

    def makeOutputDir(self):
        try:
            os.mkdir(self.outRunDir)
            os.chmod(self.outRunDir,0777)
        except:
            #this is opportunistic, will fail in normal conditions fail
            pass

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

class InjectedEvent:
    def __init__(self,fullpath,eventtype):
        self.fullpath = fullpath
        self.mask = eventtype


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
                    if isinstance(event,JsonEvent):
                        self.logger.warning("IOError on reading JSON " + str(event.json) + ' ' + str(ex))
                    else:
                        self.logger.warning("IOError on reading "+event.fullpath +' '+ str(ex))
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

    def __init__(self,conf,nsslock,box_version):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stopping = False
        self.es=None
        self.conf=conf
        self.nsslock=nsslock
        self.boxdoc_version = box_version

        try:
            threading.Thread.__init__(self)
            self.threadEvent = threading.Event()

            boxesDir =  os.path.join(conf.watch_directory,'appliance/boxes')
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

    def __init__(self,conf,runObj):
        self.logger = logging.getLogger(self.__class__.__name__)
        threading.Thread.__init__(self)
        self.conf = conf
        self.runObj = runObj
        rundirstr = 'run'+ str(runObj.runnumber).zfill(conf.run_number_padding)
        self.indexPrefix = rundirstr + '_' + conf.elastic_cluster
        self.url =       'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/fu-complete/_count'
        self.urlclose =  'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/_close'
        self.urlsearch = 'http://'+conf.es_local+':9200/' + self.indexPrefix + '*/fu-complete/_search?size=1'
        self.url_query = '{  "query": { "filtered": {"query": {"match_all": {}}}}, "sort": { "fm_date": { "order": "desc" }}}'
        self.stopping = False
        self.threadEvent = threading.Event()

    def run(self):

        check_es_complete=True
        total_es_elapsed=0

        while self.stopping==False:

            if check_es_complete:
                try:
                    resp = requests.post(self.url, '',timeout=5)
                    data = json.loads(resp.content)
                    if int(data['count']) >= len(self.runObj.online_resource_list):
                        try:
                            respq = requests.post(self.urlsearch,self.url_query,timeout=5)
                            dataq = json.loads(respq.content)
                            fm_time = str(dataq['hits']['hits'][0]['_source']['fm_date'])
                            #fill in central index completition time
                            postq = "{runNumber\":\"" + str(self.runObj.runnumber) + "\",\"completedTime\" : \"" + fm_time + "\"}"
                            requests.post(self.conf.elastic_runindex_url+'/'+"runindex_"+self.conf.elastic_runindex_name+'_write/run',postq,timeout=5)
                            self.logger.info("filled in completition time for run "+str(self.runObj.runnumber))
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
                break
            #check every 10 seconds
            self.threadEvent.wait(10)

    def stop(self):
        self.stopping = True
        self.threadEvent.set()


if __name__ == "__main__":

    import procname
    procname.setprocname('elasticbu')

    conf=initConf(sys.argv[2])

    try:run_str = ' - RUN:'+sys.argv[1].zfill(conf.run_number_padding)
    except:run_str = ''

    logging.basicConfig(filename=os.path.join(conf.log_dir,"elasticbu.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(funcName)s'+run_str+' - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()

    runnumber = sys.argv[1]
    watchdir = conf.watch_directory
    mainDir = os.path.join(watchdir,'run'+ runnumber.zfill(conf.run_number_padding))
    mainOutDir = os.path.join('/fff/output','run'+ runnumber.zfill(conf.run_number_padding))
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

        def checkEoR():
            try:
                eor_path = os.path.join(mainDir,'run'+runnumber.zfill(conf.run_number_padding)+'_ls0000_EoR.jsn')
                os.stat(eor_path)
                time.sleep(5)
                #inject EoR file in case it was not caught by inotify
                eventQueue.put(InjectedEvent(eor_path,0))
            except OSError as ex:
                pass
            except Exception as ex:
                logger.warning(str(ex))
                pass
        checkThread = threading.Thread(target=checkEoR)
        #checkThread.daemon=True
        checkThread.start()

        #starting elasticCollector thread
        ec = elasticCollectorBU(es,mainDir,mainOutDir)
        ec.setSource(eventQueue)
        ec.start()

    except Exception as e:
        logger.exception(e)
        print traceback.format_exc()
        logger.error("when processing files from directory "+mainDir)

    try:checkThread.join()
    except:pass

    logging.info("Closing notifier")
    if mr is not None:
        mr.stop_inotifyTimeout(1)

    logging.info("Quit")
    os._exit(0)
