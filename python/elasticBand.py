import os,socket,time
import sys
from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
from pyelasticsearch.client import ConnectionError
from pyelasticsearch.client import Timeout
import simplejson as json
import csv
import math
import logging

from aUtils import *


class elasticBand():


    def __init__(self,es_server_url,runstring,indexSuffix,monBufferSize,fastUpdateModulo):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.istateBuffer = []  
        self.prcinBuffer = {}
        self.prcoutBuffer = {}
        self.fuoutBuffer = {}
        self.es = ElasticSearch(es_server_url,timeout=20,revival_delay=60) 
        self.hostname = os.uname()[1]
        self.hostip = socket.gethostbyname_ex(self.hostname)[2][0]
        #self.number_of_data_nodes = self.es.health()['number_of_data_nodes']
        self.settings = {     "index.routing.allocation.require._ip" : self.hostip }
        self.indexCreated=False
        self.indexFailures=0
        self.monBufferSize = monBufferSize
        self.fastUpdateModulo = fastUpdateModulo
        aliasName = runstring + "_" + indexSuffix
        self.indexName = aliasName# + "_" + self.hostname 
 
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
            document['fm_date'] = str(mtime)
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
        datadict['ls'] = int(infile.ls[2:])
        datadict['process'] = infile.pid
        if document['data'][0] != "N/A":
          datadict['macro']   = [int(f) for f in document['data'][0].strip('[]').split(',')]
        else:
          datadict['macro'] = 0
        if document['data'][1] != "N/A":
          datadict['mini']    = [int(f) for f in document['data'][1].strip('[]').split(',')]
        else:
          datadict['mini'] = 0
        if document['data'][2] != "N/A":
          datadict['micro']   = [int(f) for f in document['data'][2].strip('[]').split(',')]
        else:
          datadict['micro'] = 0
        datadict['tp']      = float(document['data'][4]) if not math.isnan(float(document['data'][4])) and not  math.isinf(float(document['data'][4])) else 0.
        datadict['lead']    = float(document['data'][5]) if not math.isnan(float(document['data'][5])) and not  math.isinf(float(document['data'][5])) else 0.
        datadict['nfiles']  = int(document['data'][6])
        self.tryIndex('prc-s-state',datadict)
 
    def elasticize_prc_out(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        run=infile.run
        ls=infile.ls
        stream=infile.stream
        #removing 'stream' prefix
        if stream.startswith("stream"): stream = stream[6:]
        values = [int(f) if ((type(f) is str and f.isdigit()) or type(f) is int) else str(f) for f in document['data']]
        #values = [int(f) if f.isdigit() else str(f) for f in document['data']]
        keys = ["in","out"]
        #keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","fileAdler32"]
        datadict = dict(zip(keys, values))
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        try:document.pop('definition')
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

        values = [int(f) if f.isdigit() else str(f) for f in document['data']]
        keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","fileAdler32"]
        datadict = dict(zip(keys, values))
        try:datadict.pop('Filelist')
	except:pass
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        try:document.pop('definition')
	except:pass
        self.fuoutBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'fu-out',document)

    def elasticize_prc_in(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        ls=infile.ls
        index=infile.index
        prc=infile.pid

        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]
        datadict = {'out':document['data'][0]}
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['index']=int(index[5:])
        document['dest']=self.hostname
        document['process']=int(prc[3:])
        try:document.pop('definition')
	except:pass
        self.prcinBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'prc-in',document)

    def elasticize_queue_status(self,infile):
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
        self.tryIndex('fu-complete',document)
 
    def flushMonBuffer(self):
        if self.istateBuffer:
            self.logger.info("flushing fast monitor buffer (len: %r) " %len(self.istateBuffer))
            self.tryBulkIndex('prc-i-state',self.istateBuffer,attempts=1)
            self.istateBuffer = []

    def flushLS(self,ls):
        self.logger.info("flushing %r" %ls)
        prcinDocs = self.prcinBuffer.pop(ls) if ls in self.prcinBuffer else None
        prcoutDocs = self.prcoutBuffer.pop(ls) if ls in self.prcoutBuffer else None
        fuoutDocs = self.fuoutBuffer.pop(ls) if ls in self.fuoutBuffer else None
        if prcinDocs: self.tryBulkIndex('prc-in',prcinDocs,attempts=2)
        if prcoutDocs: self.tryBulkIndex('prc-out',prcoutDocs,attempts=2)
        if fuoutDocs: self.tryBulkIndex('fu-out',fuoutDocs,attempts=5)
 
    def flushAll(self):
        self.flushMonBuffer()
        lslist = list(  set(self.prcinBuffer.keys()) | 
                        set(self.prcoutBuffer.keys()) |
                        set(self.fuoutBuffer.keys()) )
        for ls in lslist:
            self.flushLS(ls)

    def updateIndexSettingsMaybe(self):
	return
        if self.indexCreated==False:
            self.es.update_settings(self.indexName,self.settings)
            self.indexCreated=True

    def tryIndex(self,docname,document): 
        try:
            self.es.index(self.indexName,docname,document)
            self.updateIndexSettingsMaybe()
        except (ConnectionError,Timeout) as ex:
            self.indexFailures+=1
            if self.indexFailures<2:
                self.logger.warning("Elasticsearch connection error.")
            time.sleep(5)
        except ElasticHttpError as ex:
            self.indexFailures+=1
            if self.indexFailures<2:
                self.logger.exception(ex)

    def tryBulkIndex(self,docname,documents,attempts=1):
        while attempts>0:
            attempts-=1
            try:
                self.es.bulk_index(self.indexName,docname,documents)
                self.updateIndexSettingsMaybe()
                break
            except (ConnectionError,Timeout) as ex:
                if attempts==0:
                    self.indexFailures+=1
                    if self.indexFailures<2:
                        self.logger.warning("Elasticsearch connection error.")
                time.sleep(5)
            except ElasticHttpError as ex:
                if attempts==0:
                    self.indexFailures+=1
                    if self.indexFailures<2:
                        self.logger.exception(ex)

