#import os
#import json
#import subprocess
#import logging

import subprocess
import demote
import prctl
from signal import SIGKILL

dqm_globalrun_filepattern = '.run{0}.global'

def restartLogCollector(conf,logger,logCollector,instanceParam):
    if logCollector!=None:
        logger.info("terminating logCollector")
        logCollector.terminate()
        logCollector = None
    logger.info("starting logcollector.py")
    logcollector_args = ['/opt/hltd/python/logcollector.py']
    logcollector_args.append(instanceParam)
    global user
    user = conf.user
    logCollector = subprocess.Popen(logcollector_args,preexec_fn=preexec_function,close_fds=True)

def preexec_function():
    dem = demote.demote(user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

def updateBlacklist(conf,logger,blfile):
    black_list=[]
    active_black_list=[]
    if conf.role=='bu':
        try:
            if os.stat(blfile).st_size>0:
                with open(blfile,'r') as fi:
                    try:
                        static_black_list = json.load(fi)
                        for item in static_black_list:
                            black_list.append(item)
                        logger.info("found these resources in " + blfile + " : " + str(black_list))
                    except ValueError:
                        logger.error("error parsing" + blfile)
        except:
                #no blacklist file, this is ok
            pass
        black_list=list(set(black_list))
        try:
            forceUpdate=False
            with open(os.path.join(conf.watch_directory,'appliance','blacklist'),'r') as fi:
                active_black_list = json.load(fi)
        except:
            forceUpdate=True
        if forceUpdate==True or active_black_list != black_list:
            try:
                with open(os.path.join(conf.watch_directory,'appliance','blacklist'),'w') as fi:
                    json.dump(black_list,fi)
            except:
                return False,black_list
    #TODO:check on FU if blacklisted
    return True,black_list

