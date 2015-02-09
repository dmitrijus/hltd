import ConfigParser
import logging
import os
import datetime

class hltdConf:
    def __init__(self, conffile):
        #        logging.debug( 'opening config file '+conffile)
        #        print 'opening config file '+conffile
        #        f = file(conffile)
        #        print f
        cfg = ConfigParser.SafeConfigParser()
        cfg.read(conffile)

        self.enabled=False
        self.role = None
        self.elastic_bu_test = None
        self.elastic_runindex_url = None
        self.elastic_runindex_name = 'runindex'
        self.watch_directory = None
        self.ramdisk_subdirectory = 'ramdisk'
        self.output_subdirectory = 'output'
        self.fastmon_insert_modulo = 1
        self.elastic_cluster = None
 
        for sec in cfg.sections():
            for item,value in cfg.items(sec):
                self.__dict__[item] = value

        self.enabled = bool(self.enabled=="True")
        self.run_number_padding = int(self.run_number_padding)
        self.delete_run_dir = bool(self.delete_run_dir=="True")
        self.use_elasticsearch = bool(self.use_elasticsearch=="True")
        self.close_es_index = bool(self.close_es_index=="True")
        self.cgi_port = int(self.cgi_port)
        self.soap2file_port = int(self.soap2file_port)

        self.dqm_machine = bool(self.dqm_machine=="True")
        if self.dqm_machine:
            self.resource_base = self.dqm_resource_base
        self.dqm_globalrunfile_mandatory = bool(self.dqm_globalrunfile_mandatory=="True")

        self.process_restart_delay_sec = float(self.process_restart_delay_sec)
        self.process_restart_limit = int(self.process_restart_limit)
        self.cmssw_threads_autosplit = int(self.cmssw_threads_autosplit)
        self.cmssw_threads = int(self.cmssw_threads)
        self.cmssw_streams = int(self.cmssw_streams)
        self.resource_use_fraction = float(self.resource_use_fraction)
        self.service_log_level = getattr(logging,self.service_log_level)
        self.autodetect_parameters()

        #read cluster name from elastic search configuration file (used to specify index name)
        if not self.elastic_cluster and self.use_elasticsearch == True:
            f = None
            try:
                f=open('/etc/elasticsearch/elasticsearch.yml')
            except:
                pass
            if f is not None:
                lines = f.readlines()
                for line in lines:
                    sline = line.strip()
                    if line.startswith("cluster.name"):
                        self.elastic_cluster = line.split(':')[1].strip()
      
    def dump(self):
        logging.info( '<CONFIGURATION time='+str(datetime.datetime.now())+'>')
        logging.info( 'conf.user            '+self.user)
        logging.info( 'conf.role            '+ self.role)
        logging.info( 'conf.cmssw_base      '+ self.cmssw_base)
        logging.info( '</CONFIGURATION>')

    def autodetect_parameters(self):
        if not self.role and 'bu' in os.uname()[1]:
            self.role = 'bu'
        elif not self.role:
            self.role = 'fu'
        if not self.watch_directory:
            if self.role == 'bu': self.watch_directory='/fff/ramdisk'
            if self.role == 'fu': self.watch_directory='/fff/data'


conf = hltdConf('/etc/hltd.conf')
