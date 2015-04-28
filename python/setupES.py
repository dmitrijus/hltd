import sys,os

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.exceptions import *

import simplejson as json
import socket

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

def convert(input):
	if isinstance(input, dict):
		return dict((convert(key), convert(value)) for key, value in input.iteritems())
	elif isinstance(input, list):
		return [convert(element) for element in input]
	elif isinstance(input, unicode):
		return input.encode('utf-8')
	else:
		return input

def setupES(es_server_url='http://localhost:9200',deleteOld=1,doLog=False):

    #ip_url=getURLwithIP(es_server_url)
    es = ElasticSearch(es_server_url,timeout=3)

    #get_template
    #es.send_request('GET', ['_template', name],query_params=query_params)

    #list_template
    res = es.cluster_state(metric='metadata')
    templateList = res['metadata']['templates']


    TEMPLATES = ["runappliance"]
    for template_name in TEMPLATES:
        if template_name not in templateList:
            if doLog:
                print "{0} template not present. It will be created. ".format(template_name)
            create_template(es,template_name)
        else:
            norm_name = convert(templateList[template_name])
            if deleteOld==0:
                if doLog:
                    print "{0} already exists. Add 'replace' parameter to force update.".format(template_name)
            else:
                if doLog:
                    print "{0} already exists.".format(template_name)
                loaddoc = load_template(es,template_name)
                if loaddoc!=None:
                    mappingSame =  norm_name['mappings']==loaddoc['mappings']
                    #settingSame = norm_name['settings']==loaddoc['settings']
                    if not (mappingSame) or deleteOld>1: #and settingSame):
                        delete_template(es,template_name)
                        if doLog:
                            print "deleted old template and will recreate {0}".format(template_name)
                        create_template(es,template_name)



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

    setupES(es_server_url=sys.argv[1],deleteOld=replaceOption,doLog=True)


