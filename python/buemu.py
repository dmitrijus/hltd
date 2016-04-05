import os
import subprocess
import demote
import prctl
from signal import SIGINT,SIGKILL
import logging

conf = None

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
 
class BUEmu:
    def __init__(self,config,disklist):
        self.process=None
        self.runnumber = None
        self.conf = config
        global conf
        conf = config

        self.disklist = disklist

    def startNewRun(self,nr):
        if self.runnumber:
            logging.error("Another BU emulator run "+str(self.runnumber)+" is already ongoing")
            return
        self.runnumber = nr
        configtouse = conf.test_bu_config
        destination_base = None
        if conf.role == 'fu':
            startindex = 0
            destination_base = self.disklist[startindex%len(self.disklist)]
        else:
            destination_base = conf.watch_directory


        if "_patch" in conf.cmssw_default_version:
            full_release="cmssw-patch"
        else:
            full_release="cmssw"


        new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                        conf.cmssw_base,
                        conf.cmssw_arch,
                        conf.cmssw_default_version,
                        conf.exec_directory,
                        full_release,
                        'null',
                        configtouse,
                        str(nr),
                        '/tmp', #input dir is not needed
                        destination_base,
                        '1',
                        '1']
        try:
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
        except Exception as ex:
            logging.error("Error in forking BU emulator process")
            logging.exception(ex)

    def stop(self):
        os.kill(self.process.pid,SIGINT)
        self.process.wait()
        self.runnumber=None


