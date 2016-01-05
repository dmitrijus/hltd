#!/bin/env python
import os,sys
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import time
import datetime
import dateutil.parser
import logging
import subprocess
from signal import SIGKILL
from signal import SIGINT
import simplejson as json
import threading
#import CGIHTTPServer
import BaseHTTPServer
import cgitb
import httplib
import demote
import re
import shutil
import socket
#import random

#modules distributed with hltd
import prctl
import _inotify as inotify

#modules which are part of hltd
from daemon2 import Daemon2
from hltdconf import initConf
from inotifywrapper import InotifyWrapper
from HLTDCommon import restartLogCollector,preexec_function
from RunRanger import RunRanger
from mountmanager import MountManager
from elasticbu import BoxInfoUpdater
from aUtils import fileHandler,ES_DIR_NAME
from setupES import setupES
from elasticBand import IndexCreator
from buemu import BUEmu
from webctrl import WebCtrl

dqm_globalrun_filepattern = '.run{0}.global'

class BoxInfo:
    def __init__(self):
        self.machine_blacklist=[]
        self.FUMap = {}
        self.boxdoc_version = 1
        self.updater = None

class ResInfo:

    def __init__(self):
       self.q_list = []
       self.num_excluded = 0
       self.idles = None
       self.used = None
       self.broken = None
       self.quarantined = None
       self.cloud = None
       self.nthreads = None
       self.nstreams = None
       self.expected_processes = None

    def cleanup_resources(self):
        try:
            dirlist = os.listdir(self.cloud)
            for cpu in dirlist:
                self.resmove(self.cloud,self.idles,cpu)
            dirlist = os.listdir(self.broken)
            for cpu in dirlist:
                self.resmove(self.broken,self.idles,cpu)
            dirlist = os.listdir(self.used)
            for cpu in dirlist:
                self.resmove(self.used,self.idles,cpu)
            dirlist = os.listdir(self.quarantined)
            for cpu in dirlist:
                self.resmove(self.quarantined,self.idles,cpu)
            dirlist = os.listdir(self.idles)
            #quarantine files beyond use fraction limit (rounded to closest integer)
            self.num_excluded = int(round(len(dirlist)*(1.-conf.resource_use_fraction)))
            for i in range(0,int(self.num_excluded)):
                self.resmove(self.idles,self.quarantined,dirlist[i])
            return True
        except Exception as ex:
            logger.warning(str(ex))
            return False

    def move_resources_to_cloud(self):
        dirlist = os.listdir(self.broken)
        for cpu in dirlist:
            self.resmove(self.broken,self.cloud,cpu)
        dirlist = os.listdir(self.used)
        for cpu in dirlist:
            self.resmove(self.used,self.cloud,cpu)
        dirlist = os.listdir(self.quarantined)
        for cpu in dirlist:
            self.resmove(self.quarantined,self.cloud,cpu)
        self.q_list=[]
        dirlist = os.listdir(self.idles)
        for cpu in dirlist:
            self.resmove(self.idles,self.cloud,cpu)
        dirlist = os.listdir(self.idles)
        for cpu in dirlist:
            self.resmove(self.idles,self.cloud,cpu)

    def has_active_resources(self):
        return len(os.listdir(self.broken))+len(os.listdir(self.used))+len(os.listdir(self.idles)) > 0

    def calculate_threadnumber(self):
        idlecount = len(os.listdir(self.idles))
        if conf.cmssw_threads_autosplit>0:
            self.nthreads = idlecount/conf.cmssw_threads_autosplit
            self.nstreams = idlecount/conf.cmssw_threads_autosplit
            if self.nthreads*conf.cmssw_threads_autosplit != self.nthreads:
                logger.error("idle cores can not be evenly split to cmssw threads")
        else:
            self.nthreads = conf.cmssw_threads
            self.nstreams = conf.cmssw_streams
        self.expected_processes = idlecount/self.nstreams

    def resmove(self,sourcedir,destdir,res):
        os.rename(os.path.join(sourcedir,res),os.path.join(destdir,res))

class StateInfo:
    def __init__(self):
        #states (suspended <-> normal <-> cloud)
        self.suspended = False
        self.cloud_mode = False
        #transitional
        self.entering_cloud_mode = False
        self.exiting_cloud_mode = False
        #imperative 
        self.abort_cloud_mode = False
        #flags
        self.resources_blocked_flag = False
        self.disabled_resource_allocation = False
        self.masked_resources = False

    #interfaces to the cloud igniter script
    def ignite_cloud():
        try:
            proc = subprocess.Popen([conf.cloud_igniter_path,'start'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out = proc.communicate()[0]
            if proc.returncode==0:
                return True
            else:
                logger.error("cloud igniter start returned code "+str(proc.returncode))
            if proc.returncode>1:
                logger.error(out)

        except OSError as ex:
            if ex.errno==2:
                logger.warning(conf.cloud_igniter_path + ' is missing')
            else:
                logger.error("Failed to run cloud igniter start")
                logger.exception(ex)
        return False

    def extinguish_cloud():
        try:
            proc = subprocess.Popen([conf.cloud_igniter_path,'stop'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out = proc.communicate()[0]
            if proc.returncode in [0,1]:
                return True
            else:
                logger.error("cloud igniter stop returned "+str(proc.returncode))
                if len(out):logger.error(out)

        except OSError as ex:
            if ex.errno==2:
                logger.warning(conf.cloud_igniter_path + ' is missing')
            else:
                logger.error("Failed to run cloud igniter start")
                logger.exception(ex)
        return False

    def cloud_status():
        try:
            proc = subprocess.Popen([conf.cloud_igniter_path,'status'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out = proc.communicate()[0]
            if proc.returncode >1:
                logger.error("cloud igniter status returned error code "+str(proc.returncode))
                logger.error(out)
        except OSError as ex:
            if ex.errno==2:
                logger.warning(conf.cloud_igniter_path + ' is missing')
            else:
                logger.error("Failed to run cloud igniter start")
                logger.exception(ex)
            return 100
        #script returns 0/1 if cloud is running/not running. invert this
        if proc.returncode == 0:return 1
        elif proc.returncode == 1: return 0
        else: return proc.returncode

def setFromConf(myinstance,resInfo):

    global conf
    global logger

    conf=initConf(myinstance)

    #for running from symbolic links
    watch_directory = os.readlink(conf.watch_directory) if os.path.islink(conf.watch_directory) else conf.watch_directory
    conf.watch_directory = watch_directory

    """
    the line below is a VERY DIRTY trick to address the fact that
    BU resources are dynamic hence they should not be under /etc
    """
    if conf.role == 'bu':
        resource_base = os.path.join(conf.watch_directory,'appliance')
    else:
        resource_base = conf.resource_base
    resource_base = os.readlink(resource_base) if os.path.islink(resource_base) else resource_base
    conf.resource_base = resInfo.resource_base

    if conf.role == 'fu':
        resInfo.idles = os.path.join(resource_base,'idle/')
        resInfo.used = os.path.join(resource_base,'online/')
        resInfo.broken = os.path.join(resource_base,'except/')
        resInfo.quarantined = os.path.join(resource_base,'quarantined/')
        resInfo.cloud = os.path.join(resource_base,'cloud/')

    #prepare log directory
    if myinstance!='main':
        if not os.path.exists(conf.log_dir):
            os.makedirs(conf.log_dir)
        if not os.path.exists(os.path.join(conf.log_dir,'pid')):
            os.makedirs(os.path.join(conf.log_dir,'pid'))
        os.chmod(conf.log_dir,0777)
        os.chmod(os.path.join(conf.log_dir,'pid'),0777)

    logging.basicConfig(filename=os.path.join(conf.log_dir,"hltd.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))
    conf.dump()

def updateBlacklist(blfile):
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

class system_monitor(threading.Thread):

    def __init__(self,stateInfo,resInfo,runList,mountMgr,boxInfo,indexCreator):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.create_file=True
        self.threadEvent = threading.Event()
        self.threadEventStat = threading.Event()
        self.statThread = None
        self.stale_flag=False
        self.highest_run_number = None
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.mm = mountMgr
        self.boxInfo = boxInfo
        self.indexCreator = indexCreator
        self.rehash()
        if conf.mount_control_path:
            self.startStatNFS()

    def rehash(self):
        if conf.role == 'fu':
            self.check_directory = [os.path.join(x,'appliance','dn') for x in self.mm.bu_disk_list_ramdisk_instance]
            #write only in one location
            if conf.mount_control_path:
                logger.info('Updating box info via control interface')
                self.directory = [os.path.join(self.mm.bu_disk_ramdisk_CI_instance,'appliance','boxes')]
            else:
                logger.info('Updating box info via data interface')
                if len(self.mm.bu_disk_list_ramdisk_instance):
                    self.directory = [os.path.join(self.mm.bu_disk_list_ramdisk_instance[0],'appliance','boxes')]
            self.check_file = [os.path.join(x,self.hostname) for x in self.check_directory]
        else:
            self.directory = [os.path.join(conf.watch_directory,'appliance/boxes/')]
            try:
                #if directory does not exist: check if it is renamed to specific name (non-main instance)
                if not os.path.exists(self.directory[0]) and conf.instance=="main":
                    os.makedirs(self.directory[0])
            except OSError:
                pass

        self.file = [os.path.join(x,self.hostname) for x in self.directory]

        logger.info("system_monitor: rehash found the following BU disk(s):"+str(self.file))
        for disk in self.file:
            logger.info(disk)

    def startStatNFS(self):
        if conf.role == "fu":
            self.statThread = threading.Thread(target=self.runStatNFS)
            self.statThread.start()

    def runStatNFS(self):
        fu_stale_counter=0
        fu_stale_counter2=0
        while self.running:
            if conf.mount_control_path:
                self.threadEventStat.wait(2)
            time_start = time.time()
            err_detected = False
            try:
                #check for NFS stale file handle
                for disk in  self.mm.bu_disk_list_ramdisk:
                    mpstat = os.stat(disk)
                for disk in  self.mm.bu_disk_list_output:
                    mpstat = os.stat(disk)
                if self.mm.bu_disk_ramdisk_CI:
                    disk = self.mm.bu_disk_ramdisk_CI
                    mpstat = os.stat(disk)
                #no issue if we reached this point
                fu_stale_counter = 0
            except (IOError,OSError) as ex:
                err_detected=True
                if ex.errno == 116:
                    if fu_stale_counter==0 or fu_stale_counter%500==0:
                        logger.fatal('detected stale file handle: '+str(disk))
                else:
                    logger.warning('stat mountpoint ' + str(disk) + ' caught Error: '+str(ex))
                fu_stale_counter+=1
                err_detected=True
            except Exception as ex:
                err_detected=True
                logger.warning('stat mountpoint ' + str(disk) + ' caught exception: '+str(ex))

            #if stale handle checks passed, check if write access and timing are normal
            #for all data network ramdisk mountpoints
            if conf.mount_control_path and not err_detected:
                try:
                    for mfile in self.check_file:
                        with open(mfile,'w') as fp:
                            fp.write('{}')
                        fu_stale_counter2 = 0
                        #os.stat(mfile)
                except IOError as ex:
                    err_detected = True
                    fu_stale_counter2+=1
                    if ex.errno==2:
                        #still an error if htld on BU did not create 'appliance/dn' dir
                        if fu_stale_counter2==0 or fu_stale_counter2%20==0:
                            logger.warning('unable to update '+mfile+ ' : '+str(ex))
                    else:
                        logger.error('update file ' + mfile + ' caught Error:'+str(ex))
                except Exception as ex:
                    err_detected = True
                    logger.error('update file ' + mfile + ' caught exception:'+str(ex))

            #measure time needed to do these actions. stale flag is set if it takes more than 10 seconds
            stat_time_delta = time.time()-time_start
            if stat_time_delta>5:
                if conf.mount_control_path:
                    logger.warning("unusually long time ("+str(stat_time_delta)+"s) was needed to perform file handle and boxinfo stat check")
                else:
                    logger.warning("unusually long time ("+str(stat_time_delta)+"s) was needed to perform stale file handle check")
            if stat_time_delta>5 or err_detected:
                self.stale_flag=True
            else:
                #clear stale flag if successful
                self.stale_flag=False

            #no loop if called inside main loop
            if not conf.mount_control_path:
                return

    def run(self):
        try:
            logger.debug('entered system monitor thread ')
            res_path_temp = os.path.join(conf.watch_directory,'appliance','resource_summary_temp')
            res_path = os.path.join(conf.watch_directory,'appliance','resource_summary')
            selfhost = os.uname()[1]
            boxinfo_update_attempts=0
            counter=0
            fu_watchdir_is_mountpoint = os.path.ismount(conf.watch_directory)
            while self.running:
                self.threadEvent.wait(5 if counter>0 else 1)
                counter+=1
                counter=counter%5
                if stateInfo.suspended: continue
                tstring = datetime.datetime.utcfromtimestamp(time.time()).isoformat()

                ramdisk = None
                if conf.role == 'bu':
                    ramdisk = os.statvfs(conf.watch_directory)
                    ramdisk_occ=1
                    try:
                      ramdisk_occ_num = float((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize - self.mm.ramdisk_submount_size)
                      ramdisk_occ_den = float(ramdisk.f_blocks*ramdisk.f_bsize - self.mm.ramdisk_submount_size)
                      ramdisk_occ = ramdisk_occ_num/ramdisk_occ_den
                    except:pass
                    if ramdisk_occ<0:
                        ramdisk_occ=0
                        logger.info('incorrect ramdisk occupancy:' + str(ramdisk_occ))
                    if ramdisk_occ>1:
                        ramdisk_occ=1
                        logger.info('incorrect ramdisk occupancy:' + str(ramdisk_occ))

                    #init
                    resource_count_idle = 0
                    resource_count_used = 0
                    resource_count_broken = 0
                    resource_count_quarantined = 0
                    resource_count_stale = 0
                    resource_count_pending = 0
                    resource_count_activeRun = 0
                    cloud_count = 0
                    lastFURuns = []
                    lastFUrun=-1
                    activeRunQueuedLumisNum = -1
                    activeRunCMSSWMaxLumi = -1
                    active_res = 0

                    fu_data_alarm=False

                    current_time = time.time()
                    stale_machines = []
                    try:
                        current_runnumber = self.runList.getLastRun().runnumber
                    except:
                        current_runnumber=0
                    for key in self.boxInfo.FUMap:
                        if key==selfhost:continue
                        try:
                            edata,etime,lastStatus = self.boxInfo.FUMap[key]
                        except:continue #deleted?
                        if current_time - etime > 10 or edata == None: continue
                        try:
                            try:
                                if edata['version']!=self.boxInfo.boxdoc_version:
                                    logger.warning('box file version mismatch from '+str(key)+' got:'+str(edata['version'])+' required:'+str(self.boxInfo.boxdoc_version))
                                    continue
                            except:
                                logger.warning('box file version for '+str(key)+' not found')
                                continue
                            if edata['detectedStaleHandle']:
                                stale_machines.append(str(key))
                                resource_count_stale+=edata['idles']+edata['used']+edata['broken']
                            else:
                                if current_runnumber in  edata['activeRuns']:
                                    resource_count_activeRun += edata['used_activeRun']+edata['broken_activeRun']
                                active_addition =0

                                if edata['cloudState'] == "resourcesReleased":
                                    resource_count_pending += edata['idles']
                                else:
                                    resource_count_idle+=edata['idles']
                                    active_addition+=edata['idles']

                                active_addition+=edata['used']
                                resource_count_used+=edata['used']
                                resource_count_broken+=edata['broken']
                                resource_count_quarantined+=edata['quarantined']

                                #active resources reported to BU if cloud state is off
                                if edata['cloudState'] == "off":
                                    active_res+=active_addition

                            cloud_count+=edata['cloud']
                            fu_data_alarm = edata['fuDataAlarm'] or fu_data_alarm
                        except Exception as ex:
                            logger.warning('problem updating boxinfo summary: '+str(ex))
                        try:
                            lastFURuns.append(edata['activeRuns'][-1])
                        except:pass
                    if len(stale_machines) and counter==1:
                        logger.warning("detected stale box resources: "+str(stale_machines))
                    fuRuns = sorted(list(set(lastFURuns)))
                    if len(fuRuns)>0:
                        lastFUrun = fuRuns[-1]
                        #second pass
                        for key in self.boxInfo.FUMap:
                            if key==selfhost:continue
                            try:
                                edata,etime,lastStatus = self.boxInfo.FUMap[key]
                            except:continue #deleted?
                            if current_time - etime > 10 or edata == None: continue
                            try:
                                try:
                                    if edata['version']!=self.boxInfo.boxdoc_version: continue
                                except: continue
                                lastrun = edata['activeRuns'][-1]
                                if lastrun==lastFUrun:
                                    qlumis = int(edata['activeRunNumQueuedLS'])
                                    if qlumis>activeRunQueuedLumisNum:activeRunQueuedLumisNum=qlumis
                                    maxcmsswls = int(edata['activeRunCMSSWMaxLS'])
                                    if maxcmsswls>activeRunCMSSWMaxLumi:activeRunCMSSWMaxLumi=maxcmsswls
                            except:pass
                    res_doc = {
                                "active_resources":active_res,
                                "active_resources_activeRun":resource_count_activeRun,
                                #"active_resources":resource_count_activeRun,
                                "idle":resource_count_idle,
                                "used":resource_count_used,
                                "broken":resource_count_broken,
                                "quarantined":resource_count_quarantined,
                                "stale_resources":resource_count_stale,
                                "cloud":cloud_count,
                                "pending_resources":resource_count_pending,
                                "activeFURun":lastFUrun,
                                "activeRunNumQueuedLS":activeRunQueuedLumisNum,
                                "activeRunCMSSWMaxLS":activeRunCMSSWMaxLumi,
                                "ramdisk_occupancy":ramdisk_occ,
                                "fuDiskspaceAlarm":fu_data_alarm
                              }
                    with open(res_path_temp,'w') as fp:
                        json.dump(res_doc,fp,indent=True)
                    os.rename(res_path_temp,res_path)
                    res_doc['fm_date']=tstring
                    try:self.boxInfo.updater.ec.injectSummaryJson(res_doc)
                    except:pass
                    try:
                        if lastFUrun>0:
                            if not self.highest_run_number or self.highest_run_number<lastFUrun:
                                self.highest_run_number=lastFUrun
                                if self.indexCreator:
                                    self.indexCreator.setMasked(lastFUrun>0,self.highest_run_number)
                        elif self.indexCreator:
                                self.indexCreator.setMasked(False,self.highest_run_number)

                    except:pass

                for mfile in self.file:
                    if conf.role == 'fu':

                            #check if stale file handle (or slow access)
                        if not conf.mount_control_path:
                            self.runStatNFS()

                        if fu_watchdir_is_mountpoint:
                            dirstat = os.statvfs(conf.watch_directory)
                            d_used = ((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)>>20,
                            d_total =  (dirstat.f_blocks*dirstat.f_bsize)>>20,
                        else:
                            p = subprocess.Popen("du -s --exclude " + ES_DIR_NAME + " --exclude mon --exclude open " + str(conf.watch_directory), shell=True, stdout=subprocess.PIPE)
                            p.wait()
                            std_out=p.stdout.read()
                            out = std_out.split('\t')[0]
                            d_used = int(out)>>10
                            d_total = conf.max_local_disk_usage

                        lastrun = self.runList.getLastRun()
                        n_used_activeRun=0
                        n_broken_activeRun=0

                        try:
                            #if cloud_mode==True and entering_cloud_mode==True:
                            #  n_idles = 0
                            #  n_used = 0
                            #  n_broken = 0
                            #  n_cloud = len(os.listdir(cloud))+len(os.listdir(idles))+len(os.listdir(used))+len(os.listdir(broken))
                            #else:
                            usedlist = os.listdir(self.resInfo.used)
                            brokenlist = os.listdir(self.resInfo.broken)
                            if lastrun:
                                try:
                                    n_used_activeRun = lastrun.countOwnedResourcesFrom(usedlist)
                                    n_broken_activeRun = lastrun.countOwnedResourcesFrom(brokenlist)
                                except:pass
                            n_idles = len(os.listdir(self.resInfo.idles))
                            n_used = len(usedlist)
                            n_broken = len(brokenlist)
                            n_cloud = len(os.listdir(self.resInfo.cloud))
                            n_quarantined = len(os.listdir(self.resInfo.quarantined))-self.resInfo.num_excluded
                            if n_quarantined<0: n_quarantined=0
                            numQueuedLumis,maxCMSSWLumi=self.getLumiQueueStat()

                            cloud_state = "off"
                            if self.state.cloud_mode:
                                if self.state.entering_cloud_mode: cloud_state="starting"
                                elif self.state.exiting_cloud_mode:cloud_state="stopping"
                                else: cloud_state="on"
                            elif self.state.resources_blocked_flag:
                                cloud_state = "resourcesReleased"
                            elif self.state.masked_resources:
                                cloud_state = "resourcesMasked"
                            else:
                                cloud_state = "off"

                            boxdoc = {
                                'fm_date':tstring,
                                'idles' : n_idles,
                                'used' : n_used,
                                'broken' : n_broken,
                                'used_activeRun' : n_used_activeRun,
                                'broken_activeRun' : n_broken_activeRun,
                                'cloud' : n_cloud,
                                'quarantined' : n_quarantined,
                                'usedDataDir' : d_used,
                                'totalDataDir' : d_total,
                                'fuDataAlarm' : d_used > 0.9*d_total,
                                'activeRuns' :   self.runList.getActiveRunNumbers(),
                                'activeRunNumQueuedLS':numQueuedLumis,
                                'activeRunCMSSWMaxLS':maxCMSSWLumi,
                                'activeRunStats':self.runList.getStateDoc(),
                                'cloudState':cloud_state,
                                'detectedStaleHandle':self.stale_flag,
                                'version':self.boxInfo.boxdoc_version
                            }
                            with open(mfile,'w+') as fp:
                                json.dump(boxdoc,fp,indent=True)
                            boxinfo_update_attempts=0

                        except (IOError,OSError) as ex:
                            logger.warning('boxinfo file write failed :'+str(ex))
                            #detecting stale file handle on recreated loop fs and remount
                            if conf.instance!='main' and (ex.errno==116 or ex.errno==2) and boxinfo_update_attempts>=5:
                                boxinfo_update_attempts=0
                                try:os.unlink(os.path.join(conf.watch_directory,'suspend0'))
                                except:pass
                                with open(os.path.join(conf.watch_directory,'suspend0'),'w'):
                                    pass
                                time.sleep(1)
                            boxinfo_update_attempts+=1
                        except Exception as ex:
                            logger.warning('exception on boxinfo file write failed : +'+str(ex))

                    if conf.role == 'bu':
                        outdir = os.statvfs('/fff/output')
                        boxdoc = {
                            'fm_date':tstring,
                            'usedRamdisk':((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize - self.mm.ramdisk_submount_size)>>20,
                            'totalRamdisk':(ramdisk.f_blocks*ramdisk.f_bsize - self.mm.ramdisk_submount_size)>>20,
                            'usedOutput':((outdir.f_blocks - outdir.f_bavail)*outdir.f_bsize)>>20,
                            'totalOutput':(outdir.f_blocks*outdir.f_bsize)>>20,
                            'activeRuns':self.runList.getActiveRunNumbers(),
                            "version":self.boxInfo.boxdoc_version
                        }
                        with open(mfile,'w+') as fp:
                            json.dump(boxdoc,fp,indent=True)

        except Exception as ex:
            logger.exception(ex)

        for mfile in self.file:
            try:
                os.remove(mfile)
            except OSError:
                pass

        logger.debug('exiting system monitor thread ')

    def getLumiQueueStat(self):
        try:
            with open(os.path.join(conf.watch_directory,
                                   'run'+str(self.runList.getLastRun().runnumber).zfill(conf.run_number_padding),
                                   'open','queue_status.jsn'),'r') as fp:

                #fcntl.flock(fp, fcntl.LOCK_EX)
                statusDoc = json.load(fp)
                return str(statusDoc["numQueuedLS"]),str(statusDoc["CMSSWMaxLS"])
        except:
            return "-1","-1"

    def stop(self):
        logger.debug("system_monitor: request to stop")
        self.running = False
        self.threadEvent.set()
        self.threadEventStat.set()
        if self.statThread:
            self.statThread.join()

class RunList:
    def __init__(self):
        self.runs = []

    def add(self,runObj):
        runNumber = runObj.runnumber
        check = filter(lambda x: runNumber == x.runnumber,self.runs)
        if len(check):
            raise Exception("Run "+str(runNumber)+" already exists")
        #doc = {runNumber:runObj}
        #self.runs.append(doc)
        self.runs.append(runObj)

    def remove(self,runNumber):
        #runs =  map(lambda x: x.keys()[0]==runNumber)
        runs =  filter(lambda x: x.runnumber==runNumber,self.runs)
        if len(runs)>1:
            logger.error("Multiple runs entries for "+str(runNumber)+" were found while removing run")
        for run in runs[:]: self.runs.pop(self.runs.index(run))

    def getOngoingRuns(self):
        #return map(lambda x: x[x.keys()[0]], filter(lambda x: x.is_ongoing_run==True,self.runs))
        return filter(lambda x: x.is_ongoing_run==True,self.runs)

    def getQuarantinedRuns(self):
        return filter(lambda x: x.pending_shutdown==True,self.runs)

    def getActiveRuns(self):
        #return map(lambda x.runnumber: x, self.runs)
        return self.runs[:]

    def getActiveRunNumbers(self):
        return map(lambda x: x.runnumber, self.runs)

    def getLastRun(self):
        try:
            return self.runs[-1]
        except:
            return None

    def getLastOngoingRun(self):
        try:
            return self.getOngoingRuns()[-1]
        except:
            return None


    def getRun(self,runNumber):
        try:
            return filter(lambda x: x.runnumber==runNumber,self.runs)[0]
        except:
            return None

    def isLatestRun(self,runObj):
        return self.runs[-1] == runObj
        #return len(filter(lambda x: x.runnumber>runObj.runnumber,self.runs))==0

    def getStateDoc(self):
        docArray = []
        for runObj in self.runs:
            docArray.append({'run':runObj.runnumber,'totalRes':runObj.n_used,'qRes':runObj.n_quarantined,'ongoing':runObj.is_ongoing_run,'errors':runObj.num_errors})
        return docArray


class ResourceRanger:

    def __init__(self,stateInfo,resInfo,runList,mountMgr,boxInfo,indexCreator,resource_lock):
        self.inotifyWrapper = InotifyWrapper(self)
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.managed_monitor = system_monitor(stateInfo,resInfo,runList,mountMgr,boxInfo,indexCreator)
        self.managed_monitor.start()
        self.regpath = []
        self.mm = mountMgr
        self.boxInfo = boxInfo
        self.resource_lock = resource_lock
        self.hostname = os.uname()[1]

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)
        self.regpath.append(path)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_managed_monitor(self):
        self.managed_monitor.stop()
        self.managed_monitor.join()
        logger.info("ResourceRanger: managed monitor shutdown done")

    def stop_inotify(self):
        self.inotifyWrapper.stop()
        self.inotifyWrapper.join()
        logger.info("ResourceRanger: Inotify wrapper shutdown done")

    def process_IN_MOVED_TO(self, event):
        logger.debug('ResourceRanger-MOVEDTO: event '+event.fullpath)
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        try:
            resourcepath=event.fullpath[1:event.fullpath.rfind("/")]
            resourcestate=resourcepath[resourcepath.rfind("/")+1:]
            resourcename=event.fullpath[event.fullpath.rfind("/")+1:]
            self.resource_lock.acquire()

            if not (resourcestate == 'online' or resourcestate == 'cloud'
                    or resourcestate == 'quarantined'):
                logger.debug('ResourceNotifier: new resource '
                              +resourcename
                              +' in '
                              +resourcepath
                              +' state '
                              +resourcestate
                              )

                if self.state.cloud_mode and not \
                   self.state.entering_cloud_mode and not \
                   self.state.exiting_cloud_mode and not \
                   self.state.abort_cloud_mode and not \
                   self.state.disabled_resource_allocation:
                    time.sleep(1)
                    logging.info('detected resource moved to non-cloud resource dir while already switched to cloud mode. Deactivating cloud.')
                    with open(os.path.join(conf.watch_directory,'include'),'w+') as fobj:
                        pass
                    self.resource_lock.release()
                    time.sleep(1)
                    return

                run = self.runList.getLastOngoingRun()
                if run is not None:
                    logger.info("ResourceRanger: found active run "+str(run.runnumber)+ " when received inotify MOVED event for "+event.fullpath)
                    """grab resources that become available
                    #@@EM implement threaded acquisition of resources here
                    """
                    #find all ready cores in same dir where inotify was triggered
                    try:
                        reslist = os.listdir('/'+resourcepath)
                    except Exception as ex:
                        logger.error("exception encountered in looking for resources")
                        logger.exception(ex)
                    #put inotify-ed resource as the first item
                    fileFound=False
                    for resindex,resname in enumerate(reslist):
                        fileFound=False
                        if resname == resourcename:
                            fileFound=True
                            if resindex != 0:
                                firstitem = reslist[0]
                                reslist[0] = resourcename
                                reslist[resindex] = firstitem
                            break
                    if fileFound==False:
                        #inotified file was already moved earlier
                        self.resource_lock.release()
                        return
                    #acquire sufficient cores for a multithreaded process start

                    #returns whether it can be matched to existing online resource or not
                    matchedList = run.MatchResource(reslist)

                    if matchedList:
                        #matched with previous resource (restarting process)
                        acquired_sufficient = True
                        res = run.AcquireResource(matchedList,resourcestate)

                    else:
                        resourcenames = []
                        for resname in reslist:
                            if len(resourcenames) < self.resInfo.nstreams:
                                resourcenames.append(resname)
                            else:
                                break

                        acquired_sufficient = False
                        if len(resourcenames) == self.resInfo.nstreams:
                            acquired_sufficient = True
                            res = run.AcquireResource(resourcenames,resourcestate)

                    if acquired_sufficient:
                        logger.info("ResourceRanger: acquired resource(s) "+str(res.cpu))
                        run.StartOnResource(res)
                        logger.info("ResourceRanger: started process on resource "
                                     +str(res.cpu))
                else:
                    #if no run is active, move (x N threads) files from except to idle to be picked up for the next run
                    #todo: debug,write test for this...
                    if resourcestate == 'except':
                        try:
                            reslist = os.listdir('/'+resourcepath)
                            #put inotify-ed resource as the first item
                            fileFound=False
                            for resindex,resname in enumerate(reslist):
                                if resname == resourcename:
                                    fileFound=True
                                    if resindex != 0:
                                        firstitem = reslist[0]
                                        reslist[0] = resourcename
                                        reslist[resindex] = firstitem
                                    break
                            if fileFound==False:
                                #inotified file was already moved earlier
                                self.resource_lock.release()
                                return
                            resourcenames = []
                            for resname in reslist:
                                if len(resourcenames) < self.resInfo.nstreams:
                                    resourcenames.append(resname)
                                else:
                                    break
                            if len(resourcenames) == self.resInfo.nstreams:
                                for resname in resourcenames:
                                    resInfo.resmove(self.resInfo.broken,self.resInfo.idles,resname)
                        #move this except after listdir?
                        except Exception as ex:
                            logger.info("exception encountered in looking for resources in except")
                            logger.info(ex)
            elif resourcestate=="cloud":
                #check if cloud mode was initiated, activate if necessary
                if conf.role=='fu' and self.state.cloud_mode==False:
                    time.sleep(1)
                    logging.info('detected core moved to cloud resources. Triggering cloud activation sequence.')
                    with open(os.path.join(conf.watch_directory,'exclude'),'w+') as fobj:
                        pass
                    time.sleep(1)
        except Exception as ex:
            logger.error("exception in ResourceRanger")
            logger.error(ex)
        try:
            self.resource_lock.release()
        except:pass

    def process_IN_CREATE(self, event):
        logger.debug('ResourceRanger-CREATE: event '+event.fullpath)
        if conf.dqm_machine:return
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        if basename=='blacklist':return
        if basename.startswith('test'):return
        if conf.role!='bu' or basename.endswith(self.hostname):
            return
        try:
            resourceage = os.path.getmtime(event.fullpath)
            self.resource_lock.acquire()
            lrun = self.runList.getLastRun()
            newRes = None
            if lrun!=None:
                if lrun.checkStaleResourceFile(event.fullpath):
                    logger.error("Run "+str(lrun.runnumber)+" notification: skipping resource "+basename+" which is stale")
                    self.resource_lock.release()
                    return
                logger.info('Try attaching FU resource: last run is '+str(lrun.runnumber))
                newRes = lrun.maybeNotifyNewRun(basename,resourceage)
            self.resource_lock.release()
            if newRes:
                newRes.NotifyNewRun(lrun.runnumber)
        except Exception as ex:
            logger.exception(ex)
            try:self.resource_lock.release()
            except:pass

    def process_default(self, event):
        logger.debug('ResourceRanger: event '+event.fullpath +' type '+ str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

    def process_IN_CLOSE_WRITE(self, event):
        logger.debug('ResourceRanger-IN_CLOSE_WRITE: event '+event.fullpath)
        resourcepath=event.fullpath[0:event.fullpath.rfind("/")]
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        if conf.role=='fu':return
        if basename == os.uname()[1]:return
        if basename.startswith('test'):return
        if basename == 'blacklist':
            with open(os.path.join(conf.watch_directory,'appliance','blacklist'),'r') as fi:
                try:
                    self.boxInfo.machine_blacklist = json.load(fi)
                except:
                    pass
        if resourcepath.endswith('boxes'):
            if basename in self.boxInfo.machine_blacklist:
                try:self.boxInfo.FUMap.pop(basename)
                except:pass
            else:
                current_time = time.time()
                current_datetime = datetime.datetime.utcfromtimestamp(current_time)
                emptyBox=False
                try:
                    infile = fileHandler(event.fullpath)
                    if infile.data=={}:emptyBox=True
                    #check which time is later (in case of small clock skew and small difference)
                    if current_datetime >  dateutil.parser.parse(infile.data['fm_date']):
                        dt = (current_datetime - dateutil.parser.parse(infile.data['fm_date'])).seconds
                    else:
                        dt = -(dateutil.parser.parse(infile.data['fm_date'])-current_datetime).seconds

                    if dt > 5:
                        logger.warning('setting stale flag for resource '+basename + ' which is '+str(dt)+' seconds behind')
                        #should be << 1s if NFS is responsive, set stale handle flag
                        infile.data['detectedStaleHandle']=True
                    elif dt < -5:
                        logger.error('setting stale flag for resource '+basename + ' which is '+str(dt)+' seconds ahead (clock skew)')
                        infile.data['detectedStaleHandle']=True

                    self.boxInfo.FUMap[basename] = [infile.data,current_time,True]
                except Exception as ex:
                    if not emptyBox:
                        logger.error("Unable to read of parse boxinfo file "+basename)
                        logger.exception(ex)
                    else:
                        logger.warning("got empty box file "+basename)
                    try:
                        self.boxInfo.FUMap[basename][2]=False
                    except:
                        #boxinfo entry doesn't exist yet
                        self.boxInfo.FUMap[basename]=[None,current_time,False]

    def checkNotifiedBoxes(self,runNumber):
        keys = self.boxInfo.FUMap.keys()
        c_time = time.time()
        for key in keys:
            #if key==self.hostname:continue #checked in inotify thread
            try:
                edata,etime,lastStatus = self.boxInfo.FUMap[key]
            except:
                #key deleted
                return False,False
            if c_time - etime > 20:continue
            #parsing or file access, check failed
            if lastStatus==False: return False,False
            try:
                #run is found in at least one box
                if runNumber in edata['activeRuns']:return True,True
            except:
                #invalid boxinfo data
                return False,False
        #all box data are valid, run not found
        return True,False

    def checkBoxes(self,runNumber):
        checkSuccessful=True
        runFound=False
        ioErrCount=0
        valErrCount=0
        files = os.listdir(self.regpath[-1])
        c_time = time.time()
        for file in files:
            if file == self.hostname:continue
            #ignore file if it is too old (FU with a problem)
            filename = os.path.join(dir,file)
            if c_time - os.path.getmtime(filename) > 20:continue
            try:
                with open(filename,'r') as fp:
                    doc = json.load(fp)
            except IOError as ex:
                checkSuccessful=False
                break
            except ValueError as ex:
                checkSuccessful=False
                break
            except Exception as ex:
                logger.exception(ex)
                checkSuccessful=False
                break;
            try:
                if runNumber in doc['activeRuns']:
                    runFound=True
                    break;
            except Exception as ex:
                logger.exception(ex)
                checkSuccessful=False
                break
        return checkSuccessful,runFound


class hltd(Daemon2,object):
    def __init__(self, instance):
        self.instance=instance
        Daemon2.__init__(self,'hltd',instance,'hltd')

    def run(self):
        """
        if role is not defined in the configuration (which it shouldn't)
        infer it from the name of the machine
        """

        #read configuration file
        state = StateInfo()
        resInfo = ResInfo()
        setFromConf(self.instance,resInfo)

        logger.info(" ")
        logger.info(" ")
        logger.info("[[[[ ---- hltd start : instance " + self.instance + " ---- ]]]]")
        logger.info(" ")

        if conf.enabled==False:
            logger.warning("Service is currently disabled.")
            sys.exit(1)

        nsslock = threading.Lock()
        resource_lock = threading.Lock()
        mm = MountManager(conf,preexec_function)

        if conf.role == 'fu':
            """
            cleanup resources
            """
            res_in_cloud = len(os.listdir(resInfo.cloud))>0
            while True:
                #switch to cloud mode if cloud files are found (e.g. machine rebooted while in cloud)
                if res_in_cloud:
                    logger.warning('found cores in cloud. this session will start in the cloud mode')
                    try:
                        resInfo.move_resources_to_cloud()
                    except:
                        pass
                    state.cloud_mode=True
                    #TODO:what if cloud mode switch fails?
                    if not stateInfo.cloud_status():
                        result = stateInfo.ignite_cloud()
                    break
                if resInfo.cleanup_resources()==True:break
                time.sleep(0.1)
                logger.warning("retrying cleanup_resources")

            resInfo.calculate_threadnumber()

            """
            recheck mount points
            this is done at start and whenever the file /etc/appliance/bus.config is modified
            mount points depend on configuration which may be updated (by runcontrol)
            (notice that hltd does not NEED to be restarted since it is watching the file all the time)
            """
            if not mm.cleanup_mountpoints(nsslock):
                logger.fatal("error mounting - terminating service")
                os._exit(10)

            #?
            try:
                os.makedirs(conf.watch_directory)
            except:
                pass

            #recursively remove any stale run data and other commands in the FU watch directory
            #if conf.watch_directory.strip()!='/':
            #    p = subprocess.Popen("rm -rf " + conf.watch_directory.strip()+'/{run*,end*,quarantined*,exclude,include,suspend*,populationcontrol,herod,logrestart,emu*}',shell=True)
            #    p.wait()

            if conf.watch_directory.startswith('/fff/'):
                p = subprocess.Popen("rm -rf " + conf.watch_directory+'/*',shell=True)
                p.wait()

            #switch to cloud mode if active, but hltd did not have cores in cloud directory in the last session
            if not is_in_cloud and cloud_status() == 1:
                    logger.warning("cloud is on on this host at hltd startup, switching to cloud mode")
                    self.resInfo.move_resources_to_cloud()
                    state.cloud_mode=True

        #startup es log collector
        logCollector = None
        if conf.use_elasticsearch == True:
            time.sleep(.2)
            restartLogCollector(logger,logCollector,self.instance)

        #BU mode threads
        boxInfo = BoxInfo()
        indexCreator = None
        if conf.role == 'bu':
            #update_success,machine_blacklist=updateBlacklist()
            boxInfo.machine_blacklist=[]
            mm.ramdisk_submount_size=0
            if self.instance == 'main':
                #if there are other instance mountpoints in ramdisk, they will be subtracted from size estimate
                mm.submount_size(conf.watch_directory)

            #start boxinfo elasticsearch updater
            try:os.makedirs(os.path.join(conf.resource_base,'dn'))
            except:pass
            try:os.makedirs(os.path.join(conf.resource_base,'boxes'))
            except:pass
            if conf.use_elasticsearch == True:
                boxInfo.updater = BoxInfoUpdater(conf,nsslock,boxInfo.boxdoc_version)
                boxInfo.updater.start()

            if conf.use_elasticsearch:
                indexCreator = IndexCreator('http://'+conf.es_local+':9200',conf.elastic_cluster,conf.force_replicas)
                #disabled until tested
                #indexCreator.start()

        #run class
        runList = RunList()

        #start monitoring resources
        rr = ResourceRanger(stateInfo,resInfo,runList,mm,boxInfo,indexCreator,resource_lock)

        #init resource ranger
        try:
            if conf.role == 'bu':
                imask  = inotify.IN_CLOSE_WRITE | inotify.IN_DELETE | inotify.IN_CREATE | inotify.IN_MOVED_TO
                rr.register_inotify_path(conf.resource_base, imask)
                rr.register_inotify_path(os.path.join(conf.resource_base,'boxes'), imask)
            else:
                imask  = inotify.IN_MOVED_TO
                rr.register_inotify_path(os.path.join(conf.resource_base,'idle'), imask)
                rr.register_inotify_path(os.path.join(conf.resource_base,'cloud'), imask)
                rr.register_inotify_path(os.path.join(conf.resource_base,'except'), imask)
            rr.start_inotify()
            logger.info("started ResourceRanger - watch_directory "+conf.resource_base)
        except Exception as ex:
            logger.error("Exception caught in starting ResourceRanger notifier")
            logger.error(ex)
            os._exit(1)

        #start monitoring new runs
        runRanger = RunRanger(self.instance,conf,state,resInfo,runList,rr,mm,logCollector,nsslock,resource_lock)
        runRanger.register_inotify_path(conf.watch_directory,inotify.IN_CREATE)
        runRanger.start_inotify()
        logger.info("started RunRanger  - watch_directory " + conf.watch_directory)


        try:
            cgitb.enable(display=0, logdir="/tmp")
            #handler = CGIHTTPServer.CGIHTTPRequestHandler
            handler = WebCtrl(self)
            # the following allows the base directory of the http
            # server to be 'conf.watch_directory, which is writeable
            # to everybody
            if os.path.exists(conf.watch_directory+'/cgi-bin'):
                os.remove(conf.watch_directory+'/cgi-bin')
            os.symlink('/opt/hltd/cgi',conf.watch_directory+'/cgi-bin')

            handler.cgi_directories = ['/cgi-bin']
            logger.info("starting http server on port "+str(conf.cgi_port))
            httpd = BaseHTTPServer.HTTPServer(("", conf.cgi_port), handler)

            logger.info("hltd serving at port "+str(conf.cgi_port)+" with role "+conf.role)
            os.chdir(conf.watch_directory)
            logger.info("[[[[ ---- hltd instance " + self.instance + ": init complete, starting httpd ---- ]]]]")
            logger.info("")
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("stop signal detected")
            aRuns =  runList.getActiveRuns()
            if len(aRuns)>0:
                logger.info("terminating all ongoing runs")
                for run in aRuns:
                    if conf.role=='fu':
                        run.Shutdown(True,True)
                    elif conf.role=='bu':
                        run.ShutdownBU()
                logger.info("terminated all ongoing runs")
            runRanger.stop_inotify()
            rr.stop_inotify()
            if boxInfo.updater is not None:
                logger.info("stopping boxinfo updater")
                boxInfo.updater.stop()
            if logCollector is not None:
                logger.info("terminating logCollector")
                logCollector.terminate()
            logger.info("stopping system monitor")
            rr.stop_managed_monitor()
            logger.info("closing httpd socket")
            httpd.socket.close()
            logger.info(threading.enumerate())
            logger.info("unmounting mount points")
            if not mm.cleanup_mountpoints(nsslock,remount=False):
                time.sleep(1)
                mm.cleanup_mountpoints(nsslock,remount=False)

            logger.info("shutdown of service (main thread) completed")
        except Exception as ex:
            logger.info("exception encountered in operating hltd")
            logger.info(ex)
            runRanger.stop_inotify()
            rr.stop_inotify()
            rr.stop_managed_monitor()
            raise

    def webHandler(self,option):
        logger.info('TEST:received option ' + option)
        pass

if __name__ == "__main__":
    import procname
    procname.setprocname('hltd')
    daemon = hltd(sys.argv[1])
    daemon.start()
