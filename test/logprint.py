#!/bin/env python

import requests
import simplejson as json
import sys
import datetime
import time




logThreshold=2 #WARNING
repeatsMax=100
termWhite=True
quit=True
printHelp=True
light=False

if len(sys.argv)>1:
    printHelp=False
    for arg in sys.argv:
        if arg=='--light':
            light=True

        elif arg=="--help":
            quit=True
            printHelp=True

        #elif arg.startswith('-c='):
        #    connurl=arg[3:]
        #    if not connurl.startswith('http://'): connurl = 'http://'+connurl

        elif arg.startswith('-l='):
            if   arg[3:]=="DEBUG":logThreshold=0
            elif arg[3:]=="WARNING":logThreshold=2
            elif arg[3:]=="ERROR":logThreshold=3
            elif arg[3:]=="FATAL":logThreshold=4

        elif arg.startswith('-r='):
            repeatsMax=int(arg[3:])

        elif arg=='--black':
            termWhite=False
        elif arg.startswith('--mode'):
            quit=False
            dmode = arg[arg.find('=')+1:].strip()
            if dmode=='cdaq':
                runDiscoveryUrl='http://es-cdaq.cms:9200/runindex_cdaq_read/run/_search?size=2'
                connurl='http://es-tribe.cms:9200'
            elif dmode=='daq2val':
                runDiscoveryUrl='http://es-cdaq.cms:9200/runindex_dv_read/run/_search?size=2'
                connurl='http://es-tribe.cms:9200'
            elif dmode=='minidaq':
                runDiscoveryUrl='http://es-cdaq.cms:9200/runindex_minidaq_read/run/_search?size=2'
                connurl='http://es-tribe.cms:9200'
            elif dmode=='vm2':
                runDiscoveryUrl='http://es-vm-cdaq:9200/runindex_cdaq_read/run/_search?size=2'
                connurl='http://es-vm-tribe.cern.ch:9200'

if printHelp==True:
    print "Usage: . logprint.py -c=%URL -l=%[DEBUG,INFO,WARNING,ERROR,FATAL] -r=%[-1,0,...] --black --light"
    print " --mode:cdaq,daq2val,minidaq,vm2. discovery of runs the central index (MANDATORY)"
    print " -l: log level threshold (default: INFO)"
    print " -r: DEBUG/INFO repeat suppression threshold (default: 100, disable: -1)"
    print " --black: color scheme for black background terminal (default: disabled)"
    print " --light: your terminal background color (default: white)"
    print " --help: print this info and quit"

if quit:
    sys.exit(0)
elif printHelp==True:
    print "\n Starting logger using default values..."

print "connecting through",connurl

#url =  'http://localhost:9200/run*/cmsswlog/_search'
urlend='/cmsswlog/_search'
urlbegin=connurl+'/'

url =  urlbegin+'run*'+urlend
urlcustom = url

#resp = requests.post(url, query)

addSpace=False
repeatsMax=100
suppressionMap = {}
alreadySuppressed = {}

filter1 =  { "range": {
               "_timestamp": {
                 "from": "",
                 "to": ""
               }
#               ,"severityVal" : { "gte" :  str(logThreshold+1)}
             }
           }
         


filter2 =  { "and" : [
               {"range": {
                 "_timestamp": {
                   "from": "",
                   "to": ""
                 }
               }},
               {"range": {
                 "severityVal" : { "gte" :  str(logThreshold)}
               }}
             ]
           }
               #,{"or": [{"term":{"severity":"INFO"}},{"term":{"severity":"WARNINIG"}},{"term":{"severity":"ERROR"}},{"term":{"severity":"FATAL"}}] }
               #,{"not":{"term":{"severity":"DEBUG"}}}



qdoc = { 
  "fields":["_timestamp", "_source"], 
  "size":100, 
  "query": { 
    "filtered": {"query": {"match_all": {}}
#      "filter": {
#        "range": {
#          "_timestamp": {
#            "from": "",
#            "to": ""
#          }
#        }
#      }
    }
  },
  "sort": { "_timestamp": { "order": "asc" }}
}

if logThreshold==0:
    useFilters2=False
    qdoc['query']['filtered']['filter'] = filter1
else:
    useFilters2=True
    qdoc['query']['filtered']['filter'] = filter2

runindex_query = '{  "query": { "filtered": {"query": {"match_all": {}}}}, "sort": { "startTime": { "order": "desc" }}}'


sleept = 0.5

init = True

#timezone difference
tzdeltaSec = int(time.time() -  time.mktime(time.gmtime()))

#1 seconds delay to allow indexing to be done for requested intervals
tnow =  datetime.datetime.utcnow().isoformat()
t_now = time.time()
#tfuture3 =  datetime.datetime.utcnow().isoformat()
timetest = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
tfuture3 =  datetime.datetime.fromtimestamp(t_now-tzdeltaSec).isoformat()
tfuture2 =  datetime.datetime.fromtimestamp(t_now-sleept-tzdeltaSec).isoformat()
tfuture =  datetime.datetime.fromtimestamp(t_now-2*sleept-tzdeltaSec).isoformat()
tnow =  datetime.datetime.fromtimestamp(t_now-3*sleept-tzdeltaSec).isoformat()
time.sleep(sleept)
lastEmpty=True

counter=0

print "current UTC time is",datetime.datetime.fromtimestamp(t_now-tzdeltaSec).strftime('%Y-%m-%d %H:%M:%S %Z')
print "loop start...."



while True:


    if runDiscoveryUrl!=None:
        cdaqr = requests.post(runDiscoveryUrl,runindex_query)

        runs_string = ''
        chits = json.loads(cdaqr.content)['hits']['hits']
	for i,c in enumerate(chits):
	    if i==0:
	        runs_string+='run' + str(c['_source']['runNumber']+'*')
	    else:
	        runs_string+=',run' + str(c['_source']['runNumber']+'*')
	
	if runs_string=='':runs_string='run*'
        urlcustom =  urlbegin+runs_string+urlend

    counter+=1
    tbefore = tnow
    tnow = tfuture
    tfuture = tfuture2
    tfuture2 = tfuture3
    tfuture3 = datetime.datetime.utcnow().isoformat()

    if lastEmpty==False:
        print "\n================\n"

    time.sleep(sleept)

    #hack
    #tbefore2 =  datetime.datetime.fromtimestamp(t_now-tzdeltaSec-3600).isoformat()
    if useFilters2:
        qdoc['query']['filtered']['filter']['and'][0]['range']['_timestamp']['from']=tbefore
        qdoc['query']['filtered']['filter']['and'][0]['range']['_timestamp']['to']=tnow
    else:
        qdoc['query']['filtered']['filter']['range']['_timestamp']['from']=tbefore
        qdoc['query']['filtered']['filter']['range']['_timestamp']['to']=tnow
    q = json.dumps(qdoc)

    resp = requests.post(urlcustom, q)

    if resp.status_code!=200:
        print "query",urlcustom,q,"returned with status code",resp.status_code
        sys.exit(2)

    data = json.loads(resp.content)

    lastEmpty==True
    maxhostlen=0
   
    #allow suppressed messages every 60 seconds
    if counter%(60/sleept)==0:
        for k in suppressionMap.keys():
            if suppressionMap[k]>100:
                suppressionMap[k]=99
            elif suppressionMap[k]>0:
                suppressionMap[k]-=1

    try:
      if data['hits']['total']!=0:
        lastEmpty==False

        arr = data['hits']['hits']

        for e in arr:


            runstring = ''
            try:
	        runidx = e['_index'].split('_')
                if runidx[0].startswith('log_'):
                    rn = runidx[1]
                else:
                    rn = runidx[0]
                if rn[3:].isdigit():
                    runstring = ' Run: '+str(int(rn[3:]))+' '
            except:
                pass


            ev = e['_source']
            severity = ev['severity']

            #suppression
            repeats=0
            if repeatsMax!=-1:
              try:
                rep = suppressionMap[ev['lexicalId']]
                rep+=1
                suppressionMap[ev['lexicalId']]=rep
                repeats=rep
                if repeats>repeatsMax and (severity=="INFO" or severity=="DEBUG"):
                    alreadySuppressed[ev['lexicalId']]=True
                    continue
              except:
                try:
                    suppressionMap[ev['lexicalId']]=1
                    repeats=1
                except:pass

            severity = ev['severity']
            sevorig=severity
            if severity=='INFO':
                #severity = '\x1b[32m INFO \x1b[0m'
                if light==False:
                    severity = '\x1b[37;1;42m INFO \x1b[0m   '
                else:
                    severity = ' ----    '
            elif severity=='WARNING':
                severity =     '\x1b[37;1;43m WARNING \x1b[0m'
            elif severity=='ERROR':
                severity =     '\x1b[37;1;41m ERROR \x1b[0m  '
            elif severity=='FATAL':
                severity =     '\x1b[31;1;40m FATAL \x1b[0m  '
            else:
                if light==False:
                    severity = 'DEBUG    '
                else:
                    severity = '  --     '

            #msg time/zone are not present in all messages...
            msgtime=''
            try:
                msgtime = '\x1b[1m'+ev['msgtime']
                try:
                    msgtime+=' '+ev['msgtimezone']
                except:
                    pass
                msgtime+='\x1b[0m'
            except:
                pass
            host = ev['host']
            hlen = len(host)
            if hlen<maxhostlen and maxhostlen<30:
                host = host.ljust(maxhostlen-hlen)
            else:
                maxhostlen=hlen
               
            pidstr=('(pid '+str(ev['pid'])+')').ljust(11)
            #while len(pidstr)<7:
            #    pidstr+=' '

            #parse optional entries
            cat=''
            str2='\x1b[1m'
            try:
                cat =ev['category']
                str2 +=cat.ljust(21)+' '
            except:
                pass

            mod=''
            try:
                mod = ev['module']
                if mod!=cat:
                    str2+=' '+mod
            except:pass
            try:
                str2+=' '+ev['moduleInstance']
            except:pass
            try:
                str2+=' '+ev['moduleCall']
            except:pass
            try:
                str2+=' '+ev['fwkState']
            except:pass
            try:
                str2+='   \x1b[0mLumi:'+ str(ev['lumi'])
            except:pass
            try:
                str2+='   \x1b[0mEvent:'+str( ev['eventInPrc'])
            except:pass

            #construct final log print:
            str2+='\x1b[0m'
            if sevorig=='INFO':
                if termWhite:
                    print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[47m'+ev['message']+'\x1b[0m'
                else:
                    print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[41m'+ev['message']+'\x1b[0m'
            elif sevorig=='WARNING':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[37;1;45m'+ev['message']+'\x1b[0m'
            elif sevorig=='ERROR':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[37;1;40m'+ev['message']+'\x1b[0m'
            elif sevorig=='FATAL':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[31;1;40m'+ev['message']+'\x1b[0m'
            else:#debug
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  '+ev['message']

            if repeats==repeatsMax and (sevorig=="INFO" or sevorig=="DEBUG"):
                try:
                    suppressionStatus = alreadySuppressed[ev['lexicalId']]
                except:
                    print "\x1b[1;36;46m[Printed message has reached maximum repeat threshold (",str(repeatsMax),") and will be suppressed for this session!]\x1b[0m"
            if addSpace:
                print ""
        #print json.dumps(data['hits']['hits'],indent=1)
    except Exception,ex:
      #print ex
      pass
