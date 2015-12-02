import sys,os

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import *

import simplejson as json
import socket
import logging

def delete_template(es,name):
    es.send_request('DELETE', ['_template', name])

def load_template(es,name):
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

def create_template(es,name):
    doc = load_template(es,name)
    send_template(es,name,doc)
    return doc

def convert(input):
	if isinstance(input, dict):
		return dict((convert(key), convert(value)) for key, value in input.iteritems())
	elif isinstance(input, list):
		return [convert(element) for element in input]
	elif isinstance(input, unicode):
		return input.encode('utf-8')
	else:
		return input

def printout(msg,usePrint,haveLog):
    if usePrint:
        print msg
    elif haveLog:
	logging.info(msg)


def setupES(es_server_url='http://localhost:9200',deleteOld=1,doPrint=False,overrideTests=False, forceReplicas=-1, create_index_name=None):

    #ip_url=getURLwithIP(es_server_url)
    es = ElasticSearch(es_server_url,timeout=5)

    #get_template
    #es.send_request('GET', ['_template', name],query_params=query_params)

    #list_template
    #res = es.cluster_state(metric='metadata')
    templateList = es.send_request('GET', ['_template'])
    #templateList = res['metadata']['templates']



    TEMPLATES = ["runappliance"]
    loaddoc = None
    for template_name in TEMPLATES:
        if template_name not in templateList:
            printout(template_name+"template not present. It will be created. ",doPrint,False)
            loaddoc = create_template(es,template_name)
            if forceReplicas>=0:
              loaddoc['settings']['index']['number_of_replicas']=forceReplicas
        else:
            loaddoc = load_template(es,template_name)
            if forceReplicas>=0:
              loaddoc['settings']['index']['number_of_replicas']=forceReplicas
            norm_name = convert(templateList[template_name])
            if deleteOld==0:
                printout(template_name+" already exists. Add 'replace' parameter to update if different, or forceupdate to always  update.",doPrint,False)
            else:
                printout(template_name+" already exists.",doPrint,False)
                if loaddoc!=None:
                    mappingSame =  norm_name['mappings']==loaddoc['mappings']
                    #settingSame = norm_name['settings']==loaddoc['settings']
                    settingsSame=True
                    if int(norm_name['settings']['index.number_of_replicas'])!=int(loaddoc['settings']['index']['number_of_replicas']):
                        settingsSame=False
                    if int(norm_name['settings']['index.number_of_shards'])!=int(loaddoc['settings']['index']['number_of_shards']):
                        settingsSame=False
	            #currently analyzer settings are ot checked
                    #if norm_name['settings']['index']['analysis']!=loaddoc['settings']['analysis']:
                    #    settingsSame=False
                    if not (mappingSame and settingsSame) or deleteOld>1:
                        #test is override
			if overrideTests==False:
			    try:
		                if norm_name['settings']['index,test']==True:
                                    printout("Template test setting found, skipping update...",doPrint,True)
                                    break
			    except:pass
                        #delete_template(es,template_name)
			printout("Updating "+template_name+" ES template",doPrint,True)
                        create_template(es,template_name)
		    else:
                      printout('runappliance ES template is up to date',doPrint,True)

    #create index name
    if create_index_name:
        if loaddoc:
            try:
                #c_res = es.create_index(index = create_index_name, body = loaddoc)
                c_res = es.send_request('PUT', [create_index_name], body = loaddoc)
                if c_res!={'acknowledged':True}:
                    logging.info("Result of index " + create_index_name + " create request: " + str(c_res) )
            except Exception as ex:
                logging.info("Index not created: "+str(ex))
        else:
            logging.info("Not creating index without template")

if __name__ == '__main__':

    if len(sys.argv) > 3:
        print "Invalid argument number"
        sys.exit(1)
    if len(sys.argv) < 2:
        print "Please provide an elasticsearch server url (e.g. http://localhost:9200)"
        sys.exit(1)

    replaceOption=0
    if len(sys.argv)>2:
        if "replace" in sys.argv[2]:
            replaceOption=1
        if "forcereplace" in sys.argv[2]:
            replaceOption=2

    setupES(es_server_url=sys.argv[1],deleteOld=replaceOption,doPrint=True,overrideTests=True)

