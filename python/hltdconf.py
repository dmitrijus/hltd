import ConfigParser
import logging
import os
import datetime

class hltdConf:
    def __init__(self, conffile):
        cfg = ConfigParser.SafeConfigParser()
        cfg.read(conffile)

        self.role = None
        self.elastic_bu_test = None
        self.elastic_runindex_url = None
        self.elastic_runindex_name = 'cdaq'
        self.watch_directory = None
        self.ramdisk_subdirectory = 'ramdisk'
        self.output_subdirectory = 'output'
        self.fastmon_insert_modulo = 1
        self.elastic_cluster = None
        self.log_dir = "/var/log/hltd"
        self.es_local = ""

        for sec in cfg.sections():
            for item,value in cfg.items(sec):
                self.__dict__[item] = value

        #override default values into imposed types
        self.enabled = cfg.getboolean('General','enabled')
        self.mount_control_path = cfg.getboolean('General','mount_control_path')

        #default
        try:
            self.run_number_padding = cfg.getint('General','run_number_padding')
        except:
            self.run_number_padding = 6

        self.delete_run_dir = cfg.getboolean('General','delete_run_dir')
        self.output_adler32 = cfg.getboolean('General','output_adler32')

        self.use_elasticsearch = cfg.getboolean('Monitoring','use_elasticsearch')
        self.force_replicas = cfg.getint('Monitoring','force_replicas')
        self.force_shards = cfg.getint('Monitoring','force_shards')
        self.update_es_template = cfg.getboolean('Monitoring','update_es_template')
        self.cgi_port = cfg.getint('Web','cgi_port')
        self.cgi_instance_port_offset = cfg.getint('Web','cgi_instance_port_offset')
        self.soap2file_port = cfg.getint('Web','soap2file_port')

        #try:
        #    self.instance_same_destination=bool(self.instance_same_destination=="True")
        #except:
        #    self.instance_same_destination = True

        self.dqm_machine = cfg.getboolean('DQM','dqm_machine')
        if self.dqm_machine:
            self.resource_base = self.dqm_resource_base
        self.dqm_globallock = cfg.getboolean('DQM','dqm_globallock')

        self.process_restart_delay_sec = cfg.getfloat('Recovery','process_restart_delay_sec')
        self.process_restart_limit = cfg.getint('Recovery','process_restart_limit')
        self.cmssw_threads_autosplit = cfg.getint('CMSSW','cmssw_threads_autosplit')
        self.cmssw_threads = cfg.getint('CMSSW','cmssw_threads')
        self.cmssw_streams = cfg.getint('CMSSW','cmssw_streams')
        self.resource_use_fraction = cfg.getfloat('Resources','resource_use_fraction')
        self.auto_clear_quarantined = cfg.getboolean('Recovery','auto_clear_quarantined')
        self.max_local_disk_usage = cfg.getint('Resources','max_local_disk_usage')
        self.service_log_level = getattr(logging,self.service_log_level)
        self.autodetect_parameters()

        if self.es_local!='localhost':
          self.elastic_cluster=self.elastic_runindex_name

        #read cluster name from elastic search configuration file (if not set up directly)
        if not self.elastic_cluster and self.use_elasticsearch:
            try:
                with open('/etc/elasticsearch/elasticsearch.yml') as fi:
                    lines = fi.readlines()
                    for line in lines:
                        sline = line.strip()
                        if line.startswith("cluster.name"):
                            self.elastic_cluster = line.split(':')[1].strip()
            except:
                pass

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
