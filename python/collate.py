from pyelasticsearch.client import ElasticSearch
from pprint import pprint
from ordereddict import OrderedDict

import sys
import os

class Aggregator:

    def __init__(self,action):
        self.result = None
        self.work = None
        self.actdict = None
        if isinstance(action,dict):
            self.actdict = action
            self.action = Aggregator.__dict__['iterate']
        else:
            self.action = Aggregator.__dict__[action]

    def add(self,input):
        if not self.result: self.result = 0
        self.result += input

    def histoadd(self,input):
        if not self.result: self.result = [0] * len(input)
        print len(input),len(self.result)
        assert len(input) == len(self.result)
        self.result = [x + self.result[ind] for ind,x in enumerate(input)]

    def cat(self,input):
        if not self.result: self.result = []
        self.result.append(input)

    def check(self,input):
        if not self.result: self.result = input
        assert self.result == input

    def ignore(self,input):
        self.result = input

    def match(self,input):
#matches prefix (anything before "_")
        if not self.result: self.result = input[:input.rfind('_')]
        assert self.result == input[:input.rfind('_')]

    def avg(self,input):
        if not self.work:
            self.work = []
            self.result = 0
        self.work.append(input)
        for x in self.work: self.result += x
        self.result /= len(self.work)

    def drop(self,input):
        return self.result

    def iterate(self,input):
        for k,v in input.items():
            self.actdict[k](v)

    def __call__(self,input):
        self.action(self,input)

    def reset(self):
        self.result = None
        self.work = None
        if self.actdict:
            for v in self.actdict.values(): v.reset()

    def value(self):
        if self.actdict:
            return dict((k,v.value()) for k,v in self.actdict.items())
        else:
            return self.result

class Query:
    def __init__(self,doctype,filtered=None):
        self.doctype = doctype
        self.generic_query= OrderedDict()
        self.generic_query["size"]= 10000
        self.generic_query["query"] = {
            "query_string" : {
            "query" : ""
            }
            }

        if filtered:
            self.generic_query["query"]["constant_score"] = {
                "filter" : {
                    "prefix" : { filtered : os.uname()[1] }
                    }
                }

    def __call__(self,ls,stream=None):
        query = self.generic_query
        query['query']['query_string']['query'] = "_type:"+self.doctype+" AND ls:"+str(ls)
        query['query']['query_string']['query'] += (" AND stream:"+stream) if stream else ""
        return query

class Collation:
    def __init__(self,es_server_url):
        self.server = ElasticSearch(es_server_url)
        self.datadict = {
            'prc-out' : {
                "lookup" : Query('prc-out','source'),
                "action" : {
                    'definition' : Aggregator('drop'),
                    'data': Aggregator({'in': Aggregator('add'),
                                        'out': Aggregator('add'),
                                        'file':Aggregator('cat')
                                        }),
                    'ls' : Aggregator('check'),
                    'stream' : Aggregator('check'),
                    'source' : Aggregator('match')
                    }
                },
            'prc-in' : {
                "lookup" : Query('prc-in','dest'),
                "action" : {
                    'definition' : Aggregator('drop'),
                    'data': Aggregator({
                            'out'    : Aggregator('add'),
                            }),
                    'ls'     : Aggregator('check'),
                    'index'  : Aggregator('cat'),
                    'source' : Aggregator('check'),
                    'dest'   : Aggregator('check'),
                    'process': Aggregator('cat')
                    }
                },
            'prc-s-state' : {
                "lookup" : Query('prc-s-state'),
                "action" : {
                    'macro'  : Aggregator('histoadd'),
                    'mini'   : Aggregator('histoadd'),
                    'micro'  : Aggregator('histoadd'),
                    'tp'     : Aggregator('add'),
                    'lead'   : Aggregator('avg'),
                    'nfiles' : Aggregator('add'),
                    'ls'     : Aggregator('check'),
                    'process': Aggregator('cat')
                    }
                }
            }
    def lookup(self,doctype):
        return self.datadict[doctype]['lookup']
    def action(self,doctype):
        return self.datadict[doctype]['action']

#print datadict[type]['lookup']
    def search(self,ind,doctype,ls,stream=None):
        if stream:
            result=self.server.search(self.lookup(doctype)(ls,stream),
                             index=ind)
        else:
            result=self.server.search(self.lookup(doctype)(ls),
                             index=ind)
        return result

    def collate(self,ind,doctype,ls,stream=None):
        result = self.search(ind,doctype,ls,stream)
        for element in  result['hits']['hits']:
            for k,v in element['_source'].items():
                self.action(doctype)[k](v)
        retval = dict((k,v.value()) for k,v in self.action(doctype).items())
        for v in self.action(doctype).values(): v.reset()
        return retval

    def refresh(self,ind):
        self.server.refresh(ind)

    def stash(self,ind,doctype,doc):
        result=self.server.index(ind,doctype,doc)
        return result
