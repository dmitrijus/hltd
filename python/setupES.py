#!/bin/env python
import sys,os

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import ElasticHttpError

import simplejson as json
import socket
import logging

def delete_template(es,name):
    es.send_request('DELETE', ['_template', name])

def load_template(name):
    filepath = os.path.join(os.path.dirname((os.path.realpath(__file__))),'../json',name+"Template.json")
    try:
        with open(filepath) as json_file:
            doc = json.load(json_file)
    except IOError,ex:
        #print ex
        doc = None
    except Exception,ex:
        #print ex
        doc = None
    return doc

def send_template(es,name,doc):
    es.send_request('PUT', ['_template', name], doc, query_params=None)

def create_template(es,name,label,subsystem,forceReplicas,forceShards,send=True):
    doc = load_template(label)
    doc["template"]="run*"+subsystem
    doc["order"]=1
    if forceReplicas>=0:
        doc['settings']['index']['number_of_replicas']=forceReplicas
    if forceShards>=0:
        doc['settings']['index']['number_of_shards']=forceShards
    if send:send_template(es,name,doc)
    return doc

#get rid of unicode elements
def convert(inp):
    if isinstance(inp, dict):
        return dict((convert(key), convert(value)) for key, value in inp.iteritems())
    elif isinstance(inp, list):
        return [convert(element) for element in inp]
    elif isinstance(inp, unicode):
        return inp.encode('utf-8')
    else:
        return inp

def printout(msg,usePrint,haveLog):
    if usePrint:
        print msg
    elif haveLog:
        logging.info(msg)


def setupES(es_server_url='http://localhost:9200',deleteOld=1,doPrint=False,overrideTests=False, forceReplicas=-1, forceShards=-1, create_index_name=None,subsystem="cdaq"):

    #ip_url=getURLwithIP(es_server_url)
    es = ElasticSearch(es_server_url,timeout=5)

    #list_template
    templateList = es.send_request('GET', ['_template'])

    TEMPLATES = ["runappliance_"+subsystem]
    loaddoc = None
    for template_name in TEMPLATES:
        template_label = template_name.split('_')[0]
        if template_name not in templateList:
            printout(template_name + " template not present. It will be created. ",doPrint,False)
            loaddoc = create_template(es,template_name,template_label,subsystem,forceReplicas,forceShards)
        else:
            loaddoc = create_template(es,None,template_label,subsystem,forceReplicas,forceShards,send=False)
            norm_name = convert(templateList[template_name])
            if deleteOld==0:
                printout(template_name+" already exists. Add 'replace' parameter to update if different, or forceupdate to always  update.",doPrint,False)
            else:
                printout(template_name+" already exists.",doPrint,False)
                if loaddoc!=None:
                    mappingSame =  norm_name['mappings']==loaddoc['mappings']
                    #settingSame = norm_name['settings']==loaddoc['settings']
                    settingsSame=True
                    if int(norm_name['settings']['index']['number_of_replicas'])!=int(loaddoc['settings']['index']['number_of_replicas']):
                        settingsSame=False
                    if int(norm_name['settings']['index']['number_of_shards'])!=int(loaddoc['settings']['index']['number_of_shards']):
                        settingsSame=False
                    #currently analyzer settings are ot checked

                    if not (mappingSame and settingsSame) or deleteOld>1:
                        #test is override
                        if overrideTests==False:
                            try:
                                if norm_name['settings']['index,test']==True:
                                    printout("Template test setting found, skipping update...",doPrint,True)
                                    break
                            except:pass
                        printout("Updating "+template_name+" ES template",doPrint,True)
                        create_template(es,template_name,template_label,subsystem,forceReplicas,forceShards)
                    else:
                        printout('runappliance ES template is up to date',doPrint,True)

    #create index
    if create_index_name:
        if loaddoc:
            try:
                c_res = es.send_request('PUT', [create_index_name], body = loaddoc)
                if c_res!={'acknowledged':True}:
                    printout("Result of index " + create_index_name + " create request: " + str(c_res),doPrint,True )
            except ElasticHttpError, ex:
                if ex[1]['type']=='index_already_exists_exception':
                    #this is for index pre-creator
                    printout("Attempting to intialize already existing index "+create_index_name,doPrint,True)
                    try:
                        doc_resp = es.send_request('GET', [create_index_name,'_count'])['count']
                    except ElasticHttpError as ex:
                        try:
                            if ex[1]['type'] == "index_closed_exception":
                                printout("Index "+create_index_name+ " is already closed! Index will be reopened",doPrint,True)
                                c_res = es.send_request('POST', [create_index_name,'_open'])
                            else:
                                printout(str(ex),doPrint,True)
                        except:
                            printout("setupES exception: "+str(ex),doPrint,True)
                    except Exception as ex:
                        printout("Unable to get doc count in index. Possible cluster problem: "+str(ex),doPrint,True)
            except Exception as ex:
                #if type(ex)==RemoteTransportException: print "a",type(ex)
                printout("Index not created: "+str(ex),doPrint,True)
        else:
            printout("Not creating index without a template",doPrint,True)

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print "Please provide an elasticsearch server url (e.g. http://es-local:9200) and subsystem (e.g. cdaq,dv)"
        sys.exit(1)

    replaceOption=0
    if len(sys.argv)>3:
        if "replace" in sys.argv[3]:
            replaceOption=1
        if "forcereplace" in sys.argv[3]:
            replaceOption=2

    setupES(es_server_url=sys.argv[1],deleteOld=replaceOption,doPrint=True,overrideTests=True,subsystem=sys.argv[2])
