#!/bin/env python

central_es_settings = {
            "analysis":{
                "analyzer": {
                    "prefix-test-analyzer": {
                        "type": "custom",
                        "tokenizer": "prefix-test-tokenizer"
                    }
                },
                "tokenizer": {
                    "prefix-test-tokenizer": {
                        "type": "path_hierarchy",
                        "delimiter": " "
                    }
                }
             },
            "index":{
                'number_of_shards' : 20,
                'number_of_replicas' : 1
            },
        }


central_es_settings_hltlogs = {
            "analysis":{
                "analyzer": {
                    "prefix-test-analyzer": {
                        "type": "custom",
                        "tokenizer": "prefix-test-tokenizer"
                    }
                },
                "tokenizer": {
                    "prefix-test-tokenizer": {
                        "type": "path_hierarchy",
                        "delimiter": "_"
                    }
                }
            },
            "index":{
                'number_of_shards' : 20,
                'number_of_replicas' : 1
            }
        }
 

central_runindex_mapping = {
            'run' : {
#                '_routing' :{
#                    'required' : True,
#                    'path'     : 'runNumber'
#                },
                '_id' : {
                    'path' : 'runNumber'
                },
                'properties' : {
                    'runNumber':{
                        'type':'integer'
                        },
                    'startTimeRC':{
                        'type':'date'
                            },
                    'stopTimeRC':{
                        'type':'date'
                            },
                    'startTime':{
                        'type':'date'
                            },
                    'endTime':{
                        'type':'date'
                            },
                    'completedTime' : {
                        'type':'date'
                            }
                },
                '_timestamp' : {
                    'enabled' : True,
                    'store'   : 'yes'
                    }
            },
            'microstatelegend' : {

                '_id' : {
                    'path' : 'id'
                },
                '_parent':{'type':'run'},
                'properties' : {
                    'names':{
                        'type':'string'
                        },
                    'id':{
                        'type':'string'
                        }
                    }
            },
            'pathlegend' : {

                '_id' : {
                    'path' : 'id'
                },
                '_parent':{'type':'run'},
                'properties' : {
                    'names':{
                        'type':'string'
                        },
                    'id':{
                        'type':'string'
                        }

                    }
                },

            'eols' : {
                '_id'        :{'path':'id'},
                '_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'},
                    'ls'            :{'type':'integer'},
                    'NEvents'       :{'type':'integer'},
                    'NFiles'        :{'type':'integer'},
                    'TotalEvents'   :{'type':'integer'}
                    },
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "fm_date"
                    },
                },
            'minimerge' : {
                '_id'        :{'path':'id'},
                '_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'}, #run+appliance+stream+ls
                    'appliance'     :{'type':'string'},
                    'stream'        :{'type':'string','index' : 'not_analyzed'},
                    'ls'            :{'type':'integer'},
                    'processed'     :{'type':'integer'},
                    'accepted'      :{'type':'integer'},
                    'errorEvents'   :{'type':'integer'},
                    'size'          :{'type':'long'},
                    }
                },
            'macromerge' : {
                '_id'        :{'path':'id'},
                '_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'}, #run+appliance+stream+ls
                    'appliance'     :{'type':'string'},
                    'stream'        :{'type':'string','index' : 'not_analyzed'},
                    'ls'            :{'type':'integer'},
                    'processed'     :{'type':'integer'},
                    'accepted'      :{'type':'integer'},
                    'errorEvents'   :{'type':'integer'},
                    'size'          :{'type':'long'},
                    }
                }

            }
central_boxinfo_mapping = {
          'boxinfo' : {
            '_id'        :{'path':'id'},
            'properties' : {
              'fm_date'       :{'type':'date'},
              'id'            :{'type':'string'},
              'host'          :{'type':'string',"index":"not_analyzed"},
              'appliance'     :{'type':'string',"index":"not_analyzed"},
              'instance'      :{'type':'string',"index":"not_analyzed"},
              'broken'        :{'type':'integer'},
              'used'          :{'type':'integer'},
              'idles'         :{'type':'integer'},
              'quarantined'   :{'type':'integer'},
              'cloud'         :{'type':'integer'},
              'usedDataDir'   :{'type':'integer'},
              'totalDataDir'  :{'type':'integer'},
              'usedRamdisk'   :{'type':'integer'},
              'totalRamdisk'  :{'type':'integer'},
              'usedOutput'    :{'type':'integer'},
              'totalOutput'   :{'type':'integer'},
              'activeRuns'    :{'type':'string'},
              'activeRunsErrors':{'type':'string',"index":"not_analyzed"},
              },
            '_timestamp' : { 
              'enabled'   : True,
              'store'     : "yes",
              "path"      : "fm_date"
              },
            '_ttl'       : { 'enabled' : True,
                             'default' :  '30d'
                             }
          },
          'boxinfo_appliance' : {
            'properties' : {
              'fm_date'       :{'type':'date'},
              'broken'        :{'type':'integer'},
              'used'          :{'type':'integer'},
              'idles'         :{'type':'integer'},
              'quarantined'   :{'type':'integer'},
              'cloud'         :{'type':'integer'},
              'usedDataDir'   :{'type':'integer'},
              'totalDataDir'  :{'type':'integer'},
              'usedRamdisk'   :{'type':'integer'},
              'totalRamdisk'  :{'type':'integer'},
              'usedOutput'    :{'type':'integer'},
              'totalOutput'   :{'type':'integer'},
              'activeRuns'    :{'type':'string'},
              'hosts'           :{'type':'string',"index":"not_analyzed"},
              'blacklistedHosts':{'type':'string',"index":"not_analyzed"},
              'host'            :{'type':'string',"index":"not_analyzed"},
              'instance'        :{'type':'string',"index":"not_analyzed"}
              },
            '_timestamp' : { 
              'enabled'   : True,
              'store'     : "yes",
              "path"      : "fm_date"
              }
            },
          }


central_hltdlogs_mapping = {
            'hltdlog' : {
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes"
                },
                #'_ttl'       : { 'enabled' : True,
                #              'default' :  '30d'}
                #,
                'properties' : {
                    'host'      : {'type' : 'string'},
                    'type'      : {'type' : 'string',"index" : "not_analyzed"},
                    'severity'  : {'type' : 'string',"index" : "not_analyzed"},
                    'severityVal'  : {'type' : 'integer'},
                    'message'   : {'type' : 'string'},
                    'lexicalId' : {'type' : 'string',"index" : "not_analyzed"},
                    'msgtime' : {'type' : 'date','format':'YYYY-mm-dd HH:mm:ss'},
                 }
            }
        }


