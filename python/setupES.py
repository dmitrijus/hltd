import sys,os

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
from pyelasticsearch.client import ConnectionError
from pyelasticsearch.client import Timeout

import simplejson as json
import socket

TEMPLATES = ["runappliance"]

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
      raise(ex)
  if url!='localhost':
      ip = socket.gethostbyname(url)
  else: ip='127.0.0.1'

  return prefix+str(ip)+suffix

def delete_template(es,name):
    es.send_request('DELETE', ['_template', name])

def create_template(es,name):
    filepath = os.path.join(os.path.dirname((os.path.realpath(__file__))),'../json',name+"Template.json")
    try:
        with open(filepath) as json_file:    
            doc = json.load(json_file)
    except IOError,ex:                
        print ex
        sys.exit(1)

    #create_template
    es.send_request('PUT', ['_template', name], doc, query_params=None)

def main():
    if len(sys.argv) > 3:
        print "Invalid argument number"
        sys.exit(1)
    if len(sys.argv) < 2:
        print "Please provide an elasticsearch server url (e.g. http://localhost:9200)"
        sys.exit(1)

    deleteOld=False
    if len(sys.argv)>2:
        if "replace" in sys.argv[2]:
            deleteOld=True

    es_server_url = sys.argv[1]
    ip_url=getURLwithIP(es_server_url)
    es = ElasticSearch(es_server_url)

    #get_template
    #es.send_request('GET', ['_template', name],query_params=query_params)

    #list_template
    res = es.cluster_state(filter_routing_table=True,filter_nodes=True, filter_blocks=True)
    templateList = res['metadata']['templates']

    for template_name in TEMPLATES:
        if template_name not in templateList: 
            print "{0} template not present. It will be created. ".format(template_name)
            create_template(es,template_name)
        else:
            if deleteOld==False:
                print "{0} already exists. Add 'replace' parameter to force update.".format(template_name)
            else:
                print "{0} already exists.".format(template_name)
                delete_template(es,template_name)
                print "Deleted old template and will recreate {0}".format(template_name)
                create_template(es,template_name)



if __name__ == '__main__':
    main()

