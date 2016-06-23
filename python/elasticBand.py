import os,socket,time
import sys
import threading
from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import *
import simplejson as json
import csv
import math
import logging

from aUtils import *

def getURLwithIP(url):
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
        ip = socket.gethostbyname(url)
    else: ip='127.0.0.1'

    return prefix+str(ip)+suffix


class IndexCreator(threading.Thread):

    def __init__(self,es_server_url, indexSuffix, forceReplicas, forceShards, numPreCreate=10):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.daemon=True
        if not self.isDaemon(): self.logger.error("Not daemon")
        self.body=None
        try:
            filepath = os.path.join(os.path.dirname((os.path.realpath(__file__))),'../json',"runapplianceTemplate.json")
            with open(filepath,'r') as fpi:
                self.body = json.load(fpi)

            if forceReplicas>=0:
                self.body['settings']['index']['number_of_replicas']=forceReplicas
            if forceShards>=0:
                self.body['settings']['index']['number_of_shards']=forceShards

            #body.pop('template')
        except Exception as e:
            self.logger.error("Exception opening elasticsearch template, "+str(e))

        self.indexSuffix = indexSuffix
        self.bufferedList = []
        self.runPendingDelete = []
        self.numPreCreate=10

        eslib_logger = logging.getLogger('elasticsearch')
        eslib_logger.setLevel(logging.ERROR)
        self.es=ElasticSearch(getURLwithIP(self.es_server_url),timeout=20)
        self.logger.info('done instantiation of indexCreator')
        self.masked=True
        self.lastActiveRun=None

    def run(self):
        while True:
            time.sleep(60*5)
            if self.masked:continue
            if not self.lastActiveRun:continue
            #try create
            self.createNextIndexMaybe(self.lastActiveRun)


    def setMasked(self,masked,lastActiveRun=None):
        self.masked=masked
        if lastActiveRun and (not self.lastActiveRun or self.lastActiveRun<lastActiveRun):
            self.lastActiveRun = lastActiveRun

    def createNextIndexMaybe(self,lastActiveRun):
        if len(self.bufferedList)>=self.numPreCreate:
            return False
        result = self.create(lastActiveRun+1)
        if result:
            self.bufferedList.append(lastActiveRun+1)

        #try to delete one empty index from skipped list
        if len(self.runPendingDelete):
            try:
                d_indexName = 'run'+str(self.runPendingDelete[0])+'_'+self.indexSuffix
                d_res = self.es.delete_index(index = d_indexName)
                if d_res!={'acknowledged':True}:
                #todo:handle index doesn't exist (remove
                    self.logger.warning("Failed to delete index "+d_indexName+" with status"+str(d_res))
                else:
                    self.runPendingDelete.pop(0)
            except Exception as ex:
                self.logger.warning("Failed to delete index "+d_indexName+" with exception"+str(ex))
        return result

    def runHasStarted(self,run):
        for rn in self.bufferedList[:]:
            if rn<=run:
                self.bufferedList.remove(rn)
                if rn<run:
                    self.runPendingDelete.append(rn)

    def create(self,run):
        runstring = 'run'+str(run)
        if not self.body:
            self.logger.error("Unable to create index, no local run index template")
            return False
        indexName = runstring + "_" + self.indexSuffix
        try:
            #c_res = self.es.create_index(index = self.indexName, body = self.body)
            c_res = self.es.send_request('PUT', [indexName], body = self.body)
            if c_res!={'acknowledged':True}:
                self.logger.error("Failed to create index "+indexName+" with status"+str(c_res))
                return False
            return True
        except Exception as ex:
            self.logger.warning("Unable to create index "+indexName+". "+str(ex))
            return False

class elasticBand():


    def __init__(self,es_server_url,runstring,indexSuffix,monBufferSize,fastUpdateModulo,forceReplicas,forceShards,nprocid=None,bu_name="unknown"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es_server_url = es_server_url
        self.istateBuffer = []
        self.prcinBuffer = {}
        self.prcoutBuffer = {}
        self.fuoutBuffer = {}
        self.prcsstateBuffer = {}

        self.es = ElasticSearch(self.es_server_url,timeout=20)
        eslib_logger = logging.getLogger('elasticsearch')
        eslib_logger.setLevel(logging.ERROR)

        #self.hostip = socket.gethostbyname_ex(self.hostname)[2][0]
        #self.number_of_data_nodes = self.es.health()['number_of_data_nodes']
        #self.settings = {     "index.routing.allocation.require._ip" : self.hostip }

        self.indexName = runstring + "_" + indexSuffix
        try:
            filepath = os.path.join(os.path.dirname((os.path.realpath(__file__))),'../json',"runapplianceTemplate.json")
            with open(filepath,'r') as fpi:
                body = json.load(fpi)
            if forceReplicas>=0:
                body['settings']['index']['number_of_replicas']=forceReplicas
            if forceShards>=0:
                body['settings']['index']['number_of_shards']=forceShards

            #body.pop('template')
            #c_res = self.es.create_index(index = self.indexName, body = body)
            c_res = self.es.send_request('PUT', [self.indexName], body = body)
            if c_res!={'acknowledged':True}:
                self.logger.info("Result of index create: " + str(c_res) )
        except Exception as ex:
            self.logger.info("Elastic Exception "+ str(ex))
        self.indexFailures=0
        self.monBufferSize = monBufferSize
        self.fastUpdateModulo = fastUpdateModulo
        self.hostname = os.uname()[1]
        self.sourceid = self.hostname + '_' + str(os.getpid())
        #construct id string (num total (logical) cores and num_utilized cores
        self.nprocid = nprocid
        self.bu_name = bu_name

    def imbue_jsn(self,infile,silent=False):
        with open(infile.filepath,'r') as fp:
            try:
                document = json.load(fp)
            except json.scanner.JSONDecodeError,ex:
                if silent==False:
                    self.logger.exception(ex)
                return None,-1
            return document,0

    def imbue_csv(self,infile):
        with open(infile.filepath,'r') as fp:
            fp.readline()
            row = fp.readline().split(',')
            return row

    def elasticize_prc_istate(self,infile):
        filepath = infile.filepath
        self.logger.debug("%r going into buffer" %filepath)
        #mtime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(os.path.getmtime(filepath)))
        mtime = infile.mtime
        stub = self.imbue_csv(infile)
        document = {}
        if len(stub) == 0 or stub[0]=='\n':
            return;
        try:
            document['macro'] = int(stub[0])
            document['mini']  = int(stub[1])
            document['micro'] = int(stub[2])
            document['tp']    = float(stub[4])
            document['lead']  = float(stub[5])
            document['nfiles']= int(stub[6])
            document['lockwaitUs']  = float(stub['data'][7])
            document['lockcount']  = float(stub['data'][8])
            try:document['nevents']= int(stub[9])
            except:pass
            document['fm_date'] = str(mtime)
            document['mclass'] = self.nprocid
            if infile.tid:
              document['source'] = self.hostname + '_' + infile.pid + '_' + infile.tid
            else:
              document['source'] = self.hostname + '_' + infile.pid
            self.istateBuffer.append(document)
        except Exception:
            pass
        #if len(self.istateBuffer) == MONBUFFERSIZE:
        if len(self.istateBuffer) == self.monBufferSize and (len(self.istateBuffer)%self.fastUpdateModulo)==0:
            self.flushMonBuffer()

    def elasticize_prc_sstate(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        datadict = {}
        document['host']=self.hostname
        document['pid']=int(infile.pid[3:])
        try:document['tid']=int(infile.tid[3:])
        except:pass
        datadict['ls'] = int(infile.ls[2:])
        if document['data'][0] != "N/A":
            datadict['macro']   = [int(f) for f in document['data'][0].strip('[]').split(',')]
            #datadict['macro_s']   = [int(f) for f in document['data'][0].strip('[]').split(',')]
        else:
            datadict['macro'] = -1
            #datadict['macro_s'] = -1
        if document['data'][1] != "N/A":
            miniVector = []
            for idx,f in enumerate(document['data'][1].strip('[]').split(',')):
                val = int(f)
                if val>0:miniVector.append({'key':idx,'value':val})
            datadict['miniv']   = miniVector
        else:
            datadict['miniv'] = []
        if document['data'][2] != "N/A":
            microVector = []
            for idx,f in enumerate(document['data'][2].strip('[]').split(',')):
                val = int(f)
                if val>0:microVector.append({'key':idx,'value':val})
            datadict['microv']   = microVector
        else:
            datadict['microv'] = []
        try:
            datadict['inputStats'] = {
              'tp' :   float(document['data'][4]) if not math.isnan(float(document['data'][4])) and not  math.isinf(float(document['data'][4])) else 0.,
              'lead' : float(document['data'][5]) if not math.isnan(float(document['data'][5])) and not  math.isinf(float(document['data'][5])) else 0.,
              'nfiles' :  int(document['data'][6]),
              'lockwaitUs' : float(document['data'][7]),
              'lockcount' : float(document['data'][8])
            }
            #new elements
            try:
              datadict['inputStats']['nevents']=document['data'][9]
            except:
              pass
        except:
            pass
        datadict['fm_date'] = str(infile.mtime)
        #per-thread source field if CMSSW is configured to provide per-thread json
        if infile.tid:
          datadict['source'] = self.hostname + '_' + infile.pid + '_' + infile.tid
        else:  
          datadict['source'] = self.hostname + '_' + infile.pid
        datadict['mclass'] = self.nprocid
        #datadict['fm_date_s'] = str(infile.mtime)
        #datadict['source_s'] = self.hostname + '_' + infile.pid
        #datadict['mclass_s'] = self.nprocid
        #self.tryIndex('prc-s-state',datadict)
        self.prcsstateBuffer.setdefault(infile.ls,[]).append(datadict)

    def elasticize_prc_out(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        run=infile.run
        ls=infile.ls
        stream=infile.stream
        #removing 'stream' prefix
        if stream.startswith("stream"): stream = stream[6:]
        try:
          document['din']=int(document["data"][0])
          document['dout']=int(document["data"][1])
        except Exception as ex:
          self.logger.warning(str(ex))
        try:document.pop('data')
        except:pass
        document['lsn']=int(ls[2:])
        document['streamn']=stream
        document['pidn']=int(infile.pid[3:])
        document['hostn']=self.hostname
        document['appn']=self.bu_name
        try:document.pop('definition')
        except:pass
        try:document.pop('source')
        except:pass
        self.prcoutBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'prc-out',document)
        #return int(ls[2:])

    def elasticize_fu_out(self,infile):

        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        run=infile.run
        ls=infile.ls
        stream=infile.stream
        #removing 'stream' prefix
        if stream.startswith("stream"): stream = stream[6:]
        #TODO:read output jsd file to decide on the variable format
        values = [int(f) if ((type(f) is str and f.isdigit()) or type(f) is int) else str(f) for f in document['data']]
        if len(values)>10:
            keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","fileAdler32","TransferDestination","MergeType","hltErrorEvents"]
        elif len(values)>9:
            keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","fileAdler32","TransferDestination","hltErrorEvents"]
        else:
            keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","fileAdler32","TransferDestination"]
        datadict = dict(zip(keys, values))
        try:datadict.pop('Filelist')
        except:pass
        document['data']=datadict
        document['ls']=int(infile.ls[2:])
        document['stream']=stream
        #document['source']=self.hostname
        document['host']=self.hostname
        document['appliance']=self.bu_name
        document['fm_date']=str(infile.mtime)
        try:document.pop('definition')
        except:pass
        try:document.pop('source')
        except:pass
        self.fuoutBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'fu-out',document)

    def elasticize_prc_in(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]
        try:
            data_size=document['data'][1]
        except:
            data_size=0
        datadict = {'out':document['data'][0],'size':data_size}
        document['data']=datadict
        document['ls']=int(infile.ls[2:])
        document['index']=int(infile.index[5:])
        document['pid']=int(infile.pid[3:])
        document['host']=self.hostname
        document['appliance']=self.bu_name
        document['fm_date']=str(infile.mtime)
        try:document.pop('definition')
        except:pass
        try:document.pop('source')
        except:pass
        #self.prcinBuffer.setdefault(ls,[]).append(document)
        self.tryBulkIndex('prc-in',[document],attempts=5)

    def elasticize_queue_status(self,infile):
        return True #disabling this message
        document,ret = self.imbue_jsn(infile,silent=True)
        if ret<0:return False
        document['fm_date']=str(infile.mtime)
        document['host']=self.hostname
        self.tryIndex('qstatus',document)
        return True

    def elasticize_fu_complete(self,timestamp):
        document = {}
        document['host']=self.hostname
        document['fm_date']=timestamp
        self.tryBulkIndex('fu-complete',[document],attempts=3)

    def flushMonBuffer(self):
        if self.istateBuffer:
            self.logger.info("flushing fast monitor buffer (len: %r) " %len(self.istateBuffer))
            self.tryBulkIndex('prc-i-state',self.istateBuffer,attempts=2)
            self.istateBuffer = []

    def flushLS(self,ls):
        self.logger.info("flushing %r" %ls)
        prcinDocs = self.prcinBuffer.pop(ls) if ls in self.prcinBuffer else None
        prcoutDocs = self.prcoutBuffer.pop(ls) if ls in self.prcoutBuffer else None
        fuoutDocs = self.fuoutBuffer.pop(ls) if ls in self.fuoutBuffer else None
        prcsstateDocs = self.prcsstateBuffer.pop(ls) if ls in self.prcsstateBuffer else None
        if prcinDocs: self.tryBulkIndex('prc-in',prcinDocs,attempts=5)
        if prcoutDocs: self.tryBulkIndex('prc-out',prcoutDocs,attempts=5)
        if fuoutDocs: self.tryBulkIndex('fu-out',fuoutDocs,attempts=10)
        if prcsstateDocs: self.tryBulkIndex('prc-s-state',prcsstateDocs,attempts=5)

    def flushAllLS(self):
        lslist = list(  set(self.prcinBuffer.keys()) |
                        set(self.prcoutBuffer.keys()) |
                        set(self.fuoutBuffer.keys()) |
                        set(self.prcsstateBuffer.keys()) )
        for ls in lslist:
            self.flushLS(ls)

    def flushAll(self):
        self.flushMonBuffer()
        self.flushAllLS()

    def tryIndex(self,docname,document):
        try:
            self.es.index(self.indexName,docname,document)
        except (ConnectionError,Timeout) as ex:
            self.logger.warning("Elasticsearch connection error:"+str(ex))
            self.indexFailures+=1
            if self.indexFailures<2:
                self.logger.exception(ex)
            #    self.logger.warning("Elasticsearch connection error.")
            time.sleep(3)
        except ElasticHttpError as ex:
            self.logger.warning("Elasticsearch http error:"+str(ex))
            self.indexFailures+=1
            if self.indexFailures<2:
                self.logger.exception(ex)
            time.sleep(.1)

    def tryBulkIndex(self,docname,documents,attempts=1):
        tried_ip_rotate = False
        while attempts>0:
            attempts-=1
            try:
                reply = self.es.bulk_index(self.indexName,docname,documents)
                try:
                    if reply['errors']==True:
                        self.logger.error("Error reply on bulk-index request:"+ str(reply))
                        time.sleep(.1)
                        continue
                except Exception as ex:
                    self.logger.error("unable to find error field in reply from elasticsearch: "+str(reply))
                    continue
                break
            except (ConnectionError,Timeout) as ex:
                self.logger.warning("Elasticsearch connection error:"+str(ex))
                if attempts==0:
                    if not tried_ip_rotate:
                        #try another host before giving up
                        self.es=ElasticSearch(getURLwithIP(self.es_server_url),timeout=20)
                        tried_ip_rotate=True
                        attempts=1
                        continue

                    self.indexFailures+=1
                    if self.indexFailures<2:
                        self.logger.exception(ex)
                    #    self.logger.warning("Elasticsearch connection error.")
                time.sleep(2)
            except ElasticHttpError as ex:
                self.logger.warning("Elasticsearch http error:"+str(ex))
                if attempts==0:
                    self.indexFailures+=1
                    if self.indexFailures<2:
                        self.logger.exception(ex)
                time.sleep(.1)
