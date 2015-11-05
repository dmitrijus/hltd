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
        self.mount_control_path = bool(self.mount_control_path=="True")
        self.run_number_padding = int(self.run_number_padding)
        self.delete_run_dir = bool(self.delete_run_dir=="True")
        self.use_elasticsearch = bool(self.use_elasticsearch=="True")
        self.force_replicas = int(self.force_replicas)
        self.cgi_port = int(self.cgi_port)
        self.cgi_instance_port_offset = int(self.cgi_instance_port_offset)
        self.soap2file_port = int(self.soap2file_port)

        try:
          self.instance_same_destination=bool(self.instance_same_destination=="True")
        except:
          self.instance_same_destination = True

        self.dqm_machine = bool(self.dqm_machine=="True")
        if self.dqm_machine:
            self.resource_base = self.dqm_resource_base
        self.dqm_globallock = bool(self.dqm_globallock=="True")

        self.process_restart_delay_sec = float(self.process_restart_delay_sec)
        self.process_restart_limit = int(self.process_restart_limit)
        self.cmssw_threads_autosplit = int(self.cmssw_threads_autosplit)
        self.cmssw_threads = int(self.cmssw_threads)
        self.cmssw_streams = int(self.cmssw_streams)
        self.resource_use_fraction = float(self.resource_use_fraction)
        self.auto_clear_quarantined = bool(self.auto_clear_quarantined=="True")
        self.max_local_disk_usage = int(self.max_local_disk_usage)
        self.service_log_level = getattr(logging,self.service_log_level)
        self.autodetect_parameters()

        #read cluster name from elastic search configuration file (if not set up directly)
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
        logging.info( '<hltd STATUS time="' + str(datetime.datetime.now()).split('.')[0] + '" user:' + self.user + ' role:' + self.role + '>')

    def autodetect_parameters(self):
        if not self.role and (os.uname()[1].startswith('bu-') or os.uname()[1].startswith('dvbu-')):
            self.role = 'bu'
        elif not self.role:
            self.role = 'fu'
        if not self.watch_directory:
            if self.role == 'bu': self.watch_directory='/fff/ramdisk'
            if self.role == 'fu': self.watch_directory='/fff/data'

def initConf(instance='main'):
    conf=None
    try:
        if instance!='main':
            conf = hltdConf('/etc/hltd-'+instance+'.conf')
    except:pass
    if conf==None and instance=='main': conf = hltdConf('/etc/hltd.conf')
    return conf

