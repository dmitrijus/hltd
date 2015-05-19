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
#import SOAPpy
import threading
import CGIHTTPServer
import BaseHTTPServer
import cgitb
import httplib
import demote
import re
import shutil
import socket
#import fcntl
#import random

#modules distributed with hltd
import prctl

#modules which are part of hltd
from daemon2 import Daemon2
from hltdconf import *
from inotifywrapper import InotifyWrapper
import _inotify as inotify

from elasticbu import BoxInfoUpdater
from aUtils import fileHandler
from setupES import setupES 

thishost = os.uname()[1]
nthreads = None
nstreams = None
expected_processes = None

runList = None

bu_disk_list_ramdisk=[]
bu_disk_list_output=[]
bu_disk_list_ramdisk_instance=[]
bu_disk_list_output_instance=[]
bu_disk_ramdisk_CI = None
bu_disk_ramdisk_CI_instance = None

resource_lock = threading.Lock()
nsslock = threading.Lock()
suspended=False
entering_cloud_mode=False
exiting_cloud_mode=False
cloud_mode=False
abort_cloud_mode=False
cached_pending_run = None
resources_blocked_flag=False

fu_watchdir_is_mountpoint=False
ramdisk_submount_size=0
machine_blacklist=[]
boxinfoFUMap = {}
boxdoc_version = 1

logCollector = None

q_list = []

dqm_globalrun_filepattern = '.run{0}.global'

def setFromConf(myinstance):

    global conf
    global logger
    global idles
    global used
    global broken
    global quarantined
    global cloud

    conf=initConf(myinstance)


    idles = conf.resource_base+'/idle/'
    used = conf.resource_base+'/online/'
    broken = conf.resource_base+'/except/'
    quarantined = conf.resource_base+'/quarantined/'
    cloud = conf.resource_base+'/cloud/'

    #prepare log directory
    if myinstance!='main':
        if not os.path.exists(conf.log_dir): os.makedirs(conf.log_dir)
        if not os.path.exists(os.path.join(conf.log_dir,'pid')): os.makedirs(os.path.join(conf.log_dir,'pid'))
        os.chmod(conf.log_dir,0777)
        os.chmod(os.path.join(conf.log_dir,'pid'),0777)

    logging.basicConfig(filename=os.path.join(conf.log_dir,"hltd.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))
    conf.dump()


def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

def cleanup_resources():
    try:
        dirlist = os.listdir(cloud)
        for cpu in dirlist:
            os.rename(cloud+cpu,idles+cpu)
        dirlist = os.listdir(broken)
        for cpu in dirlist:
            os.rename(broken+cpu,idles+cpu)
        dirlist = os.listdir(used)
        for cpu in dirlist:
            os.rename(used+cpu,idles+cpu)
        dirlist = os.listdir(quarantined)
        for cpu in dirlist:
            os.rename(quarantined+cpu,idles+cpu)
        dirlist = os.listdir(idles)
        #quarantine files beyond use fraction limit (rounded to closest integer)
        num_excluded = round(len(dirlist)*(1.-conf.resource_use_fraction))
        for i in range(0,int(num_excluded)):
            os.rename(idles+dirlist[i],quarantined+dirlist[i])
        return True
    except Exception as ex:
        logger.warning(str(ex))
        return False

def move_resources_to_cloud():
    global q_list
    dirlist = os.listdir(broken)
    for cpu in dirlist:
        os.rename(broken+cpu,cloud+cpu)
    dirlist = os.listdir(used)
    for cpu in dirlist:
        os.rename(used+cpu,cloud+cpu)
    dirlist = os.listdir(quarantined)
    for cpu in dirlist:
        os.rename(quarantined+cpu,cloud+cpu)
    q_list=[]
    dirlist = os.listdir(idles)
    for cpu in dirlist:
        os.rename(idles+cpu,cloud+cpu)
    dirlist = os.listdir(idles)
    for cpu in dirlist:
        os.rename(idles+cpu,cloud+cpu)

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

def is_cloud_inactive():
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
    return proc.returncode

def umount_helper(point,attemptsLeft=3,initial=True):

    if initial:
        try:
            logger.info('calling umount of '+point)
            subprocess.check_call(['umount',point])
        except subprocess.CalledProcessError, err1:
            if err1.returncode<2:return True
            if attemptsLeft<=0:
                logger.error('Failed to perform umount of '+point+'. returncode:'+str(err1.returncode))
                return False
            logger.warning("umount failed, trying to kill users of mountpoint "+point)
            try:
                nsslock.acquire()
                f_user = subprocess.Popen(['fuser','-km',os.path.join('/'+point,conf.ramdisk_subdirectory)],shell=False,preexec_fn=preexec_function,close_fds=True)
                nsslock.release()
                f_user.wait()
            except:
                try:nsslock.release()
                except:pass
            return umount_helper(point,attemptsLeft-1,initial=False)
    else:
        attemptsLeft-=1
        time.sleep(.5)
        try:
            logger.info("trying umount -f of "+point)
            subprocess.check_call(['umount','-f',point])
        except subprocess.CalledProcessError, err2:
            if err2.returncode<2:return True
            if attemptsLeft<=0:
                logger.error('Failed to perform umount -f of '+point+'. returncode:'+str(err2.returncode))
                return False
            return umount_helper(point,attemptsLeft,initial=False)
    return True
 
def cleanup_mountpoints(remount=True):

    global bu_disk_list_ramdisk
    global bu_disk_list_ramdisk_instance
    global bu_disk_list_output
    global bu_disk_list_output_instance
    global bu_disk_ramdisk_CI
    global bu_disk_ramdisk_CI_instance
 
    bu_disk_list_ramdisk = []
    bu_disk_list_output = []
    bu_disk_list_ramdisk_instance = []
    bu_disk_list_output_instance = []
    bu_disk_ramdisk_CI=None
    bu_disk_ramdisk_CI_instance=None
 
    if conf.bu_base_dir[0] == '/':
        bu_disk_list_ramdisk = [os.path.join(conf.bu_base_dir,conf.ramdisk_subdirectory)]
        bu_disk_list_output = [os.path.join(conf.bu_base_dir,conf.output_subdirectory)]
        if conf.instance=="main":
            bu_disk_list_ramdisk_instance = bu_disk_list_ramdisk
            bu_disk_list_output_instance = bu_disk_list_output
        else:
            bu_disk_list_ramdisk_instance = [os.path.join(bu_disk_list_ramdisk[0],conf.instance)]
            bu_disk_list_output_instance = [os.path.join(bu_disk_list_output[0],conf.instance)]
 
        #make subdirectories if necessary and return
        if remount==True:
            try:
                os.makedirs(os.path.join(conf.bu_base_dir,conf.ramdisk_subdirectory))
            except OSError:
                pass
            try:
                os.makedirs(os.path.join(conf.bu_base_dir,conf.output_subdirectory))
            except OSError:
                pass
            return True
    try:
        process = subprocess.Popen(['mount'],stdout=subprocess.PIPE)
        out = process.communicate()[0]
        mounts = re.findall('/'+conf.bu_base_dir+'[0-9]+',out) + re.findall('/'+conf.bu_base_dir+'-CI/',out)

        mounts = sorted(list(set(mounts)))
        logger.info("cleanup_mountpoints: found following mount points: ")
        logger.info(mounts)
        umount_failure=False
        for mpoint in mounts:
            point = mpoint.rstrip('/')
            umount_failure = umount_helper(os.path.join('/'+point,conf.ramdisk_subdirectory))==False

            #only attempt this if first umount was successful
            if umount_failure==False and not point.rstrip('/').endswith("-CI"):
                    umount_failure = umount_helper(os.path.join('/'+point,conf.output_subdirectory))==False
 
            #this will remove directories only if they are empty (as unmounted mount point should be)
            try:
                if os.path.join('/'+point,conf.ramdisk_subdirectory)!='/':
	            os.rmdir(os.path.join('/'+point,conf.ramdisk_subdirectory))
            except Exception as ex:
                logger.exception(ex)
            try:
                if os.path.join('/'+point,conf.output_subdirectory)!='/':
                    if not point.rstrip('/').endswith("-CI"):
                        os.rmdir(os.path.join('/'+point,conf.output_subdirectory))
            except Exception as ex:
                logger.exception(ex)
        if remount==False:
            if umount_failure:return False
            return True

        i = 0
        bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
        if os.path.exists(bus_config):
            lines = []
            with open(bus_config) as fp:
                lines = fp.readlines()

            if conf.mount_control_path and len(lines):

                try:
                    os.makedirs(os.path.join('/'+conf.bu_base_dir+'-CI',conf.ramdisk_subdirectory))
                except OSError:
                    pass
                try:
                    mountaddr = lines[0].split('.')[0]+'.cms'
                    #VM fallback
                    if lines[0].endswith('.cern.ch'): mountaddr = lines[0]
                    logger.info("found BU to mount (CI) at " + mountaddr)
                except Exception as ex:
                    logger.fatal('Unable to parse bus.config file')
                    logger.exception(ex)
                    sys.exit(1)
                attemptsLeft = 8
                while attemptsLeft>0:
                    #by default ping waits 10 seconds
                    p_begin = datetime.datetime.now()
                    if os.system("ping -c 1 "+mountaddr)==0:
                        break
                    else:
                        p_end = datetime.datetime.now()
                        logger.warn('unable to ping '+mountaddr)
                        dt = p_end - p_begin
                        if dt.seconds < 10:
                            time.sleep(10-dt.seconds)
                    attemptsLeft-=1
                    if attemptsLeft==0:
                        logger.fatal('hltd was unable to ping BU '+mountaddr)
                        #check if bus.config has been updated
                        if (os.path.getmtime(bus_config) - busconfig_age)>1:
                            return cleanup_mountpoints(remount)
                        attemptsLeft=8
                        #sys.exit(1)
                if True:
                    logger.info("trying to mount (CI) "+mountaddr+':/fff/'+conf.ramdisk_subdirectory+' '+os.path.join('/'+conf.bu_base_dir+'-CI',conf.ramdisk_subdirectory))
                    try:
                        subprocess.check_call(
                            [conf.mount_command,
                             '-t',
                             conf.mount_type,
                             '-o',
                             conf.mount_options_ramdisk,
                             mountaddr+':/fff/'+conf.ramdisk_subdirectory,
                             os.path.join('/'+conf.bu_base_dir+'-CI',conf.ramdisk_subdirectory)]
                            )
                        toappend = os.path.join('/'+conf.bu_base_dir+'-CI',conf.ramdisk_subdirectory)
                        bu_disk_ramdisk_CI=toappend
                        if conf.instance=="main":
                            bu_disk_ramdisk_CI_instance = toappend
                        else:
                            bu_disk_ramdisk_CI_instance = os.path.join(toappend,conf.instance)
                    except subprocess.CalledProcessError, err2:
                        logger.exception(err2)
                        logger.fatal("Unable to mount ramdisk - exiting.")
                        sys.exit(1)




            busconfig_age = os.path.getmtime(bus_config)
            for line in lines:
                logger.info("found BU to mount at "+line.strip())
                try:
                    os.makedirs(os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory))
                except OSError:
                    pass
                try:
                    os.makedirs(os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory))
                except OSError:
                    pass

                attemptsLeft = 8
                while attemptsLeft>0:
                    #by default ping waits 10 seconds
                    p_begin = datetime.datetime.now()
                    if os.system("ping -c 1 "+line.strip())==0:
                        break
                    else:
                        p_end = datetime.datetime.now()
                        logger.warn('unable to ping '+line.strip())
                        dt = p_end - p_begin
                        if dt.seconds < 10:
                            time.sleep(10-dt.seconds)
                    attemptsLeft-=1
                    if attemptsLeft==0:
                        logger.fatal('hltd was unable to ping BU '+line.strip())
                        #check if bus.config has been updated
                        if (os.path.getmtime(bus_config) - busconfig_age)>1:
                            return cleanup_mountpoints(remount)
                        attemptsLeft=8
                        #sys.exit(1)
                if True:
                    logger.info("trying to mount "+line.strip()+':/fff/'+conf.ramdisk_subdirectory+' '+os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory))
                    try:
                        subprocess.check_call(
                            [conf.mount_command,
                             '-t',
                             conf.mount_type,
                             '-o',
                             conf.mount_options_ramdisk,
                             line.strip()+':/fff/'+conf.ramdisk_subdirectory,
                             os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory)]
                            )
                        toappend = os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory)
                        bu_disk_list_ramdisk.append(toappend)
                        if conf.instance=="main":
                            bu_disk_list_ramdisk_instance.append(toappend)
                        else:
                            bu_disk_list_ramdisk_instance.append(os.path.join(toappend,conf.instance))
                    except subprocess.CalledProcessError, err2:
                        logger.exception(err2)
                        logger.fatal("Unable to mount ramdisk - exiting.")
                        sys.exit(1)

                    logger.info("trying to mount "+line.strip()+':/fff/'+conf.output_subdirectory+' '+os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory))
                    try:
                        subprocess.check_call(
                            [conf.mount_command,
                             '-t',
                             conf.mount_type,
                             '-o',
                             conf.mount_options_output,
                             line.strip()+':/fff/'+conf.output_subdirectory,
                             os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory)]
                            )
                        toappend = os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory)
                        bu_disk_list_output.append(toappend)
                        if conf.instance=="main" or conf.instance_same_destination==True:
                            bu_disk_list_output_instance.append(toappend)
                        else:
                            bu_disk_list_output_instance.append(os.path.join(toappend,conf.instance))
                    except subprocess.CalledProcessError, err2:
                        logger.exception(err2)
                        logger.fatal("Unable to mount output - exiting.")
                        sys.exit(1)

                i+=1
        #clean up suspended state
        try:
            if remount==True:os.popen('rm -rf '+conf.watch_directory+'/suspend*')
        except:pass
    except Exception as ex:
        logger.error("Exception in cleanup_mountpoints")
        logger.exception(ex)
        if remount==True:
            logger.fatal("Unable to handle (un)mounting")
            return False
        else:return False

def submount_size(basedir):
    loop_size=0
    try:
        p = subprocess.Popen("mount", shell=False, stdout=subprocess.PIPE)
        p.wait()
        std_out=p.stdout.read().split("\n")
        for l in std_out:
            try:
                ls = l.strip()
                toks = l.split()
                if toks[0].startswith(basedir) and toks[2].startswith(basedir) and 'loop' in toks[5]:
                    imgstat = os.stat(toks[0])
                    imgsize = imgstat.st_size
                    loop_size+=imgsize
            except:pass
    except:pass
    return loop_size

def cleanup_bu_disks():
    outdirPath = conf.watch_directory[:conf.watch_directory.find(conf.ramdisk_subdirectory)]+conf.output_subdirectory
    if conf.watch_directory.startswith('/fff') and conf.ramdisk_subdirectory in conf.watch_directory:
        logger.info('cleanup BU disks: deleting runs in ramdisk ...')
        tries = 10
        while tries > 0:
            tries-=1
            p = subprocess.Popen("rm -rf " + conf.watch_directory+'/run*',shell=True)
            p.wait()
            if p.returncode==0:
                logger.info('Ramdisk cleanup performed')
                break
            else:
                logger.info('Failed ramdisk cleanup (return code:'+str(p.returncode)+') in attempt'+str(10-tries))

    logger.info('outdirPath:'+ outdirPath + ' '+conf.output_subdirectory)
    if outdirPath.startswith('/fff') and conf.output_subdirectory in outdirPath:

        logger.info('cleanup BU disks: deleting runs in output disk ...')
        tries = 10
        while tries > 0:
            tries-=1
            p = subprocess.Popen("rm -rf " + outdirPath+'/run*',shell=True)
            p.wait()
            if p.returncode==0:
                logger.info('Output cleanup performed')
                break
            else:
                logger.info('Failed output disk cleanup (return code:'+str(p.returncode)+') in attempt '+str(10-tries))
 

def calculate_threadnumber():
    global nthreads
    global nstreams
    global expected_processes
    idlecount = len(os.listdir(idles))
    if conf.cmssw_threads_autosplit>0:
        nthreads = idlecount/conf.cmssw_threads_autosplit
        nstreams = idlecount/conf.cmssw_threads_autosplit
        if nthreads*conf.cmssw_threads_autosplit != nthreads:
            logger.error("idle cores can not be evenly split to cmssw threads")
    else:
        nthreads = conf.cmssw_threads
        nstreams = conf.cmssw_streams
    expected_processes = idlecount/nstreams


def updateBlacklist():
    black_list=[]
    active_black_list=[]
    #TODO:this will be updated to read blacklist from database
    if conf.role=='bu':
        try:
            if os.stat('/etc/appliance/blacklist').st_size>0:
                with open('/etc/appliance/blacklist','r') as fi:
                    try:
                        static_black_list = json.load(fi)
                        for item in static_black_list:
                            black_list.append(item)
                        logger.info("found these resources in /etc/appliance/blacklist: "+str(black_list))
                    except ValueError:
                        logger.error("error parsing /etc/appliance/blacklist")
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

def restartLogCollector(instanceParam):
        global logCollector
        if logCollector!=None:
            logger.info("terminating logCollector")
            logCollector.terminate()
            logCollector = None
        logger.info("starting logcollector.py")
        logcollector_args = ['/opt/hltd/python/logcollector.py']
        logcollector_args.append(instanceParam)
        logCollector = subprocess.Popen(logcollector_args,preexec_fn=preexec_function,close_fds=True)

class system_monitor(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.rehash()
        self.create_file=True
        self.threadEvent = threading.Event()
        self.threadEventStat = threading.Event()
        self.statThread = None
        self.stale_flag=False
        self.boxdoc_version = boxdoc_version
        if conf.mount_control_path:
            self.startStatNFS()

    def rehash(self):
        if conf.role == 'fu':
            self.check_directory = [os.path.join(x,'appliance','dn') for x in bu_disk_list_ramdisk_instance]
            #write only in one location
            if conf.mount_control_path:
                logger.info('Updating box info via control interface')
                self.directory = [os.path.join(bu_disk_ramdisk_CI_instance,'appliance','boxes')]
            else:
                logger.info('Updating box info via data interface')
                self.directory = [os.path.join(bu_disk_list_ramdisk_instance[0],'appliance','boxes')]
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
            self.statThread = threading.Thread(target = self.runStatNFS)
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
                for disk in  bu_disk_list_ramdisk:
                    mpstat = os.stat(disk)
                for disk in  bu_disk_list_output:
                    mpstat = os.stat(disk)
                if bu_disk_ramdisk_CI:
                    disk = bu_disk_ramdisk_CI
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
            global suspended
            global ramdisk_submount_size
            res_path_temp = os.path.join(conf.watch_directory,'appliance','resource_summary_temp')
            res_path = os.path.join(conf.watch_directory,'appliance','resource_summary')
            selfhost = os.uname()[1]
            boxinfo_update_attempts=0
            counter=0
            while self.running:
                self.threadEvent.wait(5 if counter>0 else 1)
                counter+=1
                counter=counter%5
                if suspended:continue
                tstring = datetime.datetime.utcfromtimestamp(time.time()).isoformat()

                ramdisk = None
                if conf.role == 'bu':
                    ramdisk = os.statvfs(conf.watch_directory)
                    ramdisk_occ=1
                    try:ramdisk_occ = float((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize - ramdisk_submount_size)/float(ramdisk.f_blocks*ramdisk.f_bsize - ramdisk_submount_size)
                    except:pass
                    if ramdisk_occ<0:
                        ramdisk_occ=0
                        logger.info('incorrect ramdisk occupancy',ramdisk_occ)
                    if ramdisk_occ>1:
                        ramdisk_occ=1
                        logger.info('incorrect ramdisk occupancy',ramdisk_occ)

                    #init
                    resource_count_idle = 0
                    resource_count_used = 0
                    resource_count_broken = 0
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
                        current_runnumber = runList.getLastRun().runnumber
                    except:
                        current_runnumber=0
                    for key in boxinfoFUMap:
                        if key==selfhost:continue
                        try:
                            edata,etime,lastStatus = boxinfoFUMap[key]
                        except:continue #deleted?
                        if current_time - etime > 10 or edata == None: continue
                        try:
                            try:
                                if edata['version']!=self.boxdoc_version:
                                    logger.warning('box file version mismatch from '+str(key)+' got:'+str(edata['version'])+' required:'+str(self.boxdoc_version))
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
                    if len(stale_machines):
                        logger.warning("detected stale box resources: "+str(stale_machines))
                    fuRuns = sorted(list(set(lastFURuns)))
                    if len(fuRuns)>0:
                        lastFUrun = fuRuns[-1]
                        #second pass
                        for key in boxinfoFUMap:
                            if key==selfhost:continue
                            try:
                                edata,etime,lastStatus = boxinfoFUMap[key]
                            except:continue #deleted?
                            if current_time - etime > 10 or edata == None: continue
                            try:
                                try:
                                    if edata['version']!=self.boxdoc_version: continue
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
                    try:boxInfo.ec.injectSummaryJson(res_doc)
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
                            p = subprocess.Popen("du -s --exclude TEMP_ES_DIRECTORY --exclude mon " + str(conf.watch_directory), shell=True, stdout=subprocess.PIPE)
                            p.wait()
                            std_out=p.stdout.read()
                            out = std_out.split('\t')[0]
                            d_used = int(out)>>10
                            d_total = conf.max_local_disk_usage

                        lRun = runList.getLastRun()
                        n_used_activeRun=0
                        n_broken_activeRun=0

                        try:
                            #if cloud_mode==True and entering_cloud_mode==True:
                            #  n_idles = 0
                            #  n_used = 0
                            #  n_broken = 0
                            #  n_cloud = len(os.listdir(cloud))+len(os.listdir(idles))+len(os.listdir(used))+len(os.listdir(broken))
                            #else:
                            usedlist = os.listdir(used)
                            brokenlist = os.listdir(broken)
                            if lRun:
                                try:
                                    n_used_activeRun = lRun.countOwnedResourcesFrom(usedlist)
                                    n_broken_activeRun = lRun.countOwnedResourcesFrom(brokenlist)
                                except:pass
                            n_idles = len(os.listdir(idles))
                            n_used = len(usedlist)
                            n_broken = len(brokenlist)
                            n_cloud = len(os.listdir(cloud))
                            n_quarantined = len(os.listdir(quarantined))
                            numQueuedLumis,maxCMSSWLumi=self.getLumiQueueStat()

                            cloud_state = "off"
                            if cloud_mode:
                                if entering_cloud_mode: cloud_state="starting"
                                elif exiting_cloud_mode:cloud_state="stopping"
                                else: cloud_state="on"
                            elif resources_blocked_flag:
                              cloud_state = "resourcesReleased"
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
                                'activeRuns' :   runList.getActiveRunNumbers(),
                                'activeRunNumQueuedLS':numQueuedLumis,
                                'activeRunCMSSWMaxLS':maxCMSSWLumi,
                                'activeRunStats':runList.getStateDoc(),
                                'cloudState':cloud_state,
                                'detectedStaleHandle':self.stale_flag,
                                'version':self.boxdoc_version
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
                            'usedRamdisk':((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize - ramdisk_submount_size)>>20,
                            'totalRamdisk':(ramdisk.f_blocks*ramdisk.f_bsize - ramdisk_submount_size)>>20,
                            'usedOutput':((outdir.f_blocks - outdir.f_bavail)*outdir.f_bsize)>>20,
                            'totalOutput':(outdir.f_blocks*outdir.f_bsize)>>20,
                            'activeRuns':runList.getActiveRunNumbers(),
                            "version":self.boxdoc_version
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
            with open(os.path.join(conf.watch_directory,'run'+str(runList.getLastRun().runnumber).zfill(conf.run_number_padding),
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

class BUEmu:
    def __init__(self):
        self.process=None
        self.runnumber = None

    def startNewRun(self,nr):
        if self.runnumber:
            logger.error("Another BU emulator run "+str(self.runnumber)+" is already ongoing")
            return
        self.runnumber = nr
        configtouse = conf.test_bu_config
        destination_base = None
        if role == 'fu':
            destination_base = bu_disk_list_ramdisk_instance[startindex%len(bu_disk_list_ramdisk_instance)]
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
            logger.error("Error in forking BU emulator process")
            logger.error(ex)

    def stop(self):
        os.kill(self.process.pid,SIGINT)
        self.process.wait()
        self.runnumber=None

bu_emulator=BUEmu()

class OnlineResource:

    def __init__(self,parent,resourcenames,lock):
        self.parent = parent
        self.hoststate = 0 #@@MO what is this used for?
        self.cpu = resourcenames
        self.process = None
        self.processstate = None
        self.watchdog = None
        self.runnumber = None
        self.associateddir = None
        self.statefiledir = None
        self.lock = lock
        self.retry_attempts = 0
        self.quarantined = []

    def ping(self):
        if conf.role == 'bu':
            if not os.system("ping -c 1 "+self.cpu[0])==0: pass #self.hoststate = 0

    def NotifyNewRun(self,runnumber):
        self.runnumber = runnumber
        logger.info("calling start of run on "+self.cpu[0])
        try:
            connection = httplib.HTTPConnection(self.cpu[0], conf.cgi_port - conf.cgi_instance_port_offset)
            connection.request("GET",'cgi-bin/start_cgi.py?run='+str(runnumber))
            response = connection.getresponse()
            #do something intelligent with the response code
            logger.error("response was "+str(response.status))
            if response.status > 300: self.hoststate = 1
            else:
                logger.info(response.read())
        except Exception as ex:
            logger.exception(ex)

    def NotifyShutdown(self):
        try:
            connection = httplib.HTTPConnection(self.cpu[0], conf.cgi_port - self.cgi_instance_port_offset)
            connection.request("GET",'cgi-bin/stop_cgi.py?run='+str(self.runnumber))
            time.sleep(0.05)
            response = connection.getresponse()
            time.sleep(0.05)
            #do something intelligent with the response code
            #if response.status > 300: self.hoststate = 0
        except Exception as ex:
            logger.exception(ex)

    def StartNewProcess(self ,runnumber, startindex, arch, version, menu,transfermode,num_threads,num_streams):
        logger.debug("OnlineResource: StartNewProcess called")
        self.runnumber = runnumber

        """
        this is just a trick to be able to use two
        independent mounts of the BU - it should not be necessary in due course
        IFF it is necessary, it should address "any" number of mounts, not just 2
        """
        input_disk = bu_disk_list_ramdisk_instance[startindex%len(bu_disk_list_ramdisk_instance)]
        #run_dir = input_disk + '/run' + str(self.runnumber).zfill(conf.run_number_padding)
        logger.info("starting process with "+version+" and run number "+str(runnumber)+ ' threads:'+str(num_threads)+' streams:'+str(num_streams))

        if "_patch" in version:
            full_release="cmssw-patch"
        else:
            full_release="cmssw"

        if not conf.dqm_machine:
            new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                            conf.cmssw_base,
                            arch,
                            version,
                            conf.exec_directory,
                            full_release,
                            menu,
                            transfermode,
                            str(runnumber),
                            input_disk,
                            conf.watch_directory,
                            str(num_threads),
                            str(num_streams)]
        else: # a dqm machine
            dqm_globalrun_file = input_disk + '/' + dqm_globalrun_filepattern.format(str(runnumber).zfill(conf.run_number_padding))
            runkey = ''
            try:
                with open(dqm_globalrun_file, 'r') as f:
                    for line in f:
                        runkey = re.search(r'\s*run_key\s*=\s*([0-9A-Za-z_]*)', line, re.I)
                        if runkey:
                            runkey = runkey.group(1).lower()
                            break
            except IOError,ex:
                logging.exception(ex)
                logging.info("the default run key will be used for the dqm jobs")
            new_run_args = [conf.cmssw_script_location+'/startDqmRun.sh',
                            conf.cmssw_base,
                            arch,
                            conf.exec_directory,
                            str(runnumber),
                            input_disk,
                            used+self.cpu[0]]
            if self.watchdog:
                new_run_args.append('skipFirstLumis=True')
            if runkey:
                new_run_args.append('runkey={0}'.format(runkey))
            else:
                logging.info('Not able to determine the DQM run key from the "global" file. Default value from the input source will be used.')

        try:
#            dem = demote.demote(conf.user)
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
            self.processstate = 100
            logger.info("arg array "+str(new_run_args).translate(None, "'")+' started with pid '+str(self.process.pid))
#            time.sleep(1.)
            if self.watchdog==None:
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logger.debug("watchdog thread for "+str(self.process.pid)+" is alive "
                             + str(self.watchdog.is_alive()))
            else:
                self.watchdog.join()
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logger.debug("watchdog thread restarted for "+str(self.process.pid)+" is alive "
                              + str(self.watchdog.is_alive()))
        except Exception as ex:
            logger.info("OnlineResource: exception encountered in forking hlt slave")
            logger.info(ex)

    def join(self):
        logger.debug('calling join on thread ' +self.watchdog.name)
        self.watchdog.join()

    def disableRestart(self):
        logger.debug("OnlineResource "+str(self.cpu)+" restart is now disabled")
        if self.watchdog:
            self.watchdog.disableRestart()

    def clearQuarantined(self,doLock=True,restore=True):
        global q_list
        retq=[]
        if not restore:
            q_list+=self.quarantined
            return self.quarantined
        if doLock:resource_lock.acquire()
        try:
            for cpu in self.quarantined:
                logger.info('Clearing quarantined resource '+cpu)
                os.rename(quarantined+cpu,idles+cpu)
                retq.append(cpu)
            self.quarantined = []
            self.parent.n_used=0
            self.parent.n_quarantined=0
        except Exception as ex:
            logger.exception(ex)
        if doLock:resource_lock.release()
        return retq

class ProcessWatchdog(threading.Thread):
    def __init__(self,resource,lock):
        threading.Thread.__init__(self)
        self.resource = resource
        self.lock = lock
        self.retry_limit = conf.process_restart_limit
        self.retry_delay = conf.process_restart_delay_sec
        self.retry_enabled = True
        self.quarantined = False
    def run(self):
        try:
            monfile = self.resource.associateddir+'/hltd.jsn'
            logger.info('watchdog for process '+str(self.resource.process.pid))
            self.resource.process.wait()
            returncode = self.resource.process.returncode
            pid = self.resource.process.pid

            #update json process monitoring file
            self.resource.processstate=returncode
            logger.debug('ProcessWatchdog: acquire lock thread '+str(pid))
            self.lock.acquire()
            logger.debug('ProcessWatchdog: acquired lock thread '+str(pid))

            try:
                with open(monfile,"r+") as fp:

                    stat=json.load(fp)

                    stat=[[x[0],x[1],returncode]
                          if x[0]==self.resource.cpu else [x[0],x[1],x[2]] for x in stat]
                    fp.seek(0)
                    fp.truncate()
                    json.dump(stat,fp)

                    fp.flush()
            except IOError,ex:
                logger.exception(ex)
            except ValueError:
                pass

            logger.debug('ProcessWatchdog: release lock thread '+str(pid))
            self.lock.release()
            logger.debug('ProcessWatchdog: released lock thread '+str(pid))


            abortedmarker = self.resource.statefiledir+'/'+Run.ABORTED
            if os.path.exists(abortedmarker):
                resource_lock.acquire()
                #release resources
                try:
                    for cpu in self.resource.cpu:
                        try:
                            os.rename(used+cpu,idles+cpu)
                            self.resource.parent.n_used-=1
                        except Exception as ex:
                            logger.exception(ex)
                except:pass
                resource_lock.release()
                return

            #cleanup actions- remove process from list and attempt restart on same resource
            if returncode != 0 and returncode!=None:

                #bump error count in active_runs_errors which is logged in the box file
                self.resource.parent.num_errors+=1

                if returncode < 0:
                    logger.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with signal "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )
                else:
                    logger.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with code "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )
                #quit codes (configuration errors):
                quit_codes = [127,90,73]

                #removed 65 because it is not only configuration error
                #quit_codes = [127,90,65,73]

                #dqm mode will treat configuration error as a crash and eventually move to quarantined
                if conf.dqm_machine==False and returncode in quit_codes:
                    if self.resource.retry_attempts < self.retry_limit:
                        logger.warning('for this type of error, restarting this process is disabled')
                        self.resource.retry_attempts=self.retry_limit
                    if returncode==127:
                        logger.fatal('Exit code indicates that CMSSW environment might not be available (cmsRun executable not in path).')
                    elif returncode==90:
                        logger.fatal('Exit code indicates that there might be a python error in the CMSSW configuration.')
                    else:
                        logger.fatal('Exit code indicates that there might be a C/C++ error in the CMSSW configuration.')

                #generate crashed pid json file like: run000001_ls0000_crash_pid12345.jsn
                oldpid = "pid"+str(pid).zfill(5)
                outdir = self.resource.statefiledir
                runnumber = "run"+str(self.resource.runnumber).zfill(conf.run_number_padding)
                ls = "ls0000"
                filename = "_".join([runnumber,ls,"crash",oldpid])+".jsn"
                filepath = os.path.join(outdir,filename)
                document = {"errorCode":returncode}
                try:
                    with open(filepath,"w+") as fi:
                        json.dump(document,fi)
                except: logger.exception("unable to create %r" %filename)
                logger.info("pid crash file: %r" %filename)


                if self.resource.retry_attempts < self.retry_limit:
                    """
                    sleep a configurable amount of seconds before
                    trying a restart. This is to avoid 'crash storms'
                    """
                    time.sleep(self.retry_delay)

                    self.resource.process = None
                    self.resource.retry_attempts += 1

                    logger.info("try to restart process for resource(s) "
                                 +str(self.resource.cpu)
                                 +" attempt "
                                 + str(self.resource.retry_attempts))
                    resource_lock.acquire()
                    for cpu in self.resource.cpu:
                      os.rename(used+cpu,broken+cpu)
                      self.resource.parent.n_used-=1
                    resource_lock.release()
                    logger.debug("resource(s) " +str(self.resource.cpu)+
                                  " successfully moved to except")
                elif self.resource.retry_attempts >= self.retry_limit:
                    logger.error("process for run "
                                  +str(self.resource.runnumber)
                                  +" on resources " + str(self.resource.cpu)
                                  +" reached max retry limit "
                                  )
                    resource_lock.acquire()
                    for cpu in self.resource.cpu:
                        os.rename(used+cpu,quarantined+cpu)
                        self.resource.quarantined.append(cpu)
                        self.resource.parent.n_quarantined+=1
                    resource_lock.release()
                    self.quarantined=True

                    #write quarantined marker for RunRanger
                    try:
                        os.remove(conf.watch_directory+'/quarantined'+str(self.resource.runnumber).zfill(conf.run_number_padding))
                    except:pass
                    try:
                        fp = open(conf.watch_directory+'/quarantined'+str(self.resource.runnumber).zfill(conf.run_number_padding),'w+')
                        fp.close()
                    except Exception as ex:
                        logger.exception(ex)

            #successful end= release resource (TODO:maybe should mark aborted for non-0 error codes)
            elif returncode == 0 or returncode == None:
                logger.info('releasing resource, exit 0 meaning end of run '+str(self.resource.cpu))

                # generate an end-of-run marker if it isn't already there - it will be picked up by the RunRanger
                endmarker = conf.watch_directory+'/end'+str(self.resource.runnumber).zfill(conf.run_number_padding)
                stoppingmarker = self.resource.statefiledir+'/'+Run.STOPPING
                completemarker = self.resource.statefiledir+'/'+Run.COMPLETE
                if not os.path.exists(endmarker):
                    fp = open(endmarker,'w+')
                    fp.close()
                # wait until the request to end has been handled
                while not os.path.exists(stoppingmarker):
                    if os.path.exists(completemarker): break
                    time.sleep(.1)
                # move back the resource now that it's safe since the run is marked as ended
                resource_lock.acquire()
                for cpu in self.resource.cpu:
                  try:
                      os.rename(used+cpu,idles+cpu)
                  except Exception as ex:
                      logger.warning('problem moving core from used to idle:'+str(ex))
                resource_lock.release()

                #self.resource.process=None

            #        logger.info('exiting thread '+str(self.resource.process.pid))

        except Exception as ex:
            logger.info("OnlineResource watchdog: exception")
            logger.exception(ex)
            try:resource_lock.release()
            except:pass
        return

    def disableRestart(self):
        self.retry_enabled = False

class Run:

    STARTING = 'starting'
    ACTIVE = 'active'
    STOPPING = 'stopping'
    ABORTED = 'aborted'
    COMPLETE = 'complete'
    ABORTCOMPLETE = 'abortcomplete'

    VALID_MARKERS = [STARTING,ACTIVE,STOPPING,COMPLETE,ABORTED]

    def __init__(self,nr,dirname,bu_dir,instance):

        self.pending_shutdown=False
        self.is_ongoing_run=True
        self.num_errors = 0

        self.instance = instance
        self.runnumber = nr
        self.dirname = dirname
        self.online_resource_list = []
        self.anelastic_monitor = None
        self.elastic_monitor = None
        self.elastic_test = None

        self.arch = None
        self.version = None
        self.transfermode = None
        self.waitForEndThread = None
        self.beginTime = datetime.datetime.now()
        self.anelasticWatchdog = None
        self.elasticBUWatchdog = None
        self.completedChecker = None
        self.threadEvent = threading.Event()
        self.stopThreads = False

        #stats on usage of resources
        self.n_used = 0
        self.n_quarantined = 0

        self.inputdir_exists = False

        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STARTING)
        #TODO:raise from runList
        #            if int(self.runnumber) in active_runs:
        #                raise Exception("Run "+str(self.runnumber)+ "already active")

        self.hlt_directory = os.path.join(bu_dir,conf.menu_directory)
        self.menu_path = os.path.join(self.hlt_directory,conf.menu_name)
        self.paramfile_path = os.path.join(self.hlt_directory,conf.paramfile_name)

        readMenuAttempts=0
        #polling for HLT menu directory
        def paramsPresent():
            return os.path.exists(self.hlt_directory) and os.path.exists(self.menu_path) and os.path.exists(self.paramfile_path)

        paramsDetected = False
        while conf.dqm_machine==False and conf.role=='fu':
            if paramsPresent():
                try:
                    with open(self.paramfile_path,'r') as fp:
                           fffparams = json.load(fp)

                           self.arch = fffparams['SCRAM_ARCH']
                           self.version = fffparams['CMSSW_VERSION']
                           self.transfermode = fffparams['TRANSFER_MODE']
                           paramsDetected = True
                           logger.info("Run " + str(self.runnumber) + " uses " + self.version + " ("+self.arch + ") with " + str(conf.menu_name) + ' transferDest:'+self.transfermode)
                    break

                except ValueError as ex:
                    if readMenuAttempts>50:
                        logger.exception(ex)
                        break
                except Exception as ex:
                    if readMenuAttempts>50:
                        logger.exception(ex)
                        break

            else:
                if readMenuAttempts>50:
                    logger.error("FFF parameter or HLT menu files not found in ramdisk")
                    break
            readMenuAttempts+=1
            time.sleep(.1)
            continue

        if not paramsDetected:
            self.arch = conf.cmssw_arch
            self.version = conf.cmssw_default_version
            self.menu_path = conf.test_hlt_config1
            self.transfermode = 'null'
            if conf.role=='fu':
                logger.warn("Using default values for run " + str(self.runnumber) + ": " + self.version + " (" + self.arch + ") with " + self.menu_path)

        #give this command line parameter quoted in case it is empty
        if len(self.transfermode)==0:
            self.transfermode='null'

        #backup HLT menu and parameters
        if conf.role=='fu':
            try:
                hltTargetName = 'HltConfig.py_run'+str(self.runnumber)+'_'+self.arch+'_'+self.version+'_'+self.transfermode
                shutil.copy(self.menu_path,os.path.join(conf.log_dir,'pid',hltTargetName))
            except:
                logger.warn('Unable to backup HLT menu')

        self.rawinputdir = None
        #
        if conf.role == "bu":
            try:
                self.rawinputdir = conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
                os.stat(self.rawinputdir)
                self.inputdir_exists = True
            except Exception, ex:
                logger.error("failed to stat "+self.rawinputdir)
            try:
                os.mkdir(self.rawinputdir+'/mon')
            except Exception, ex:
                logger.error("could not create mon dir inside the run input directory")
        else:
            self.rawinputdir= os.path.join(bu_disk_list_ramdisk_instance[0],'run' + str(self.runnumber).zfill(conf.run_number_padding))

        #verify existence of the input directory
        if conf.role=='fu':
            if not paramsDetected and conf.dqm_machine==False:
                try:
                    os.stat(self.rawinputdir)
                    self.inputdir_exists = True
                except:
                    logger.error("unable to stat raw input directory for run "+str(self.runnumber))
                    return
            else:
                self.inputdir_exists = True

        self.lock = threading.Lock()

        if conf.use_elasticsearch == True:
            global nsslock
            try:
                if conf.role == "bu":
                    nsslock.acquire()
                    logger.info("starting elasticbu.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elasticbu.py',self.instance,str(self.runnumber)]
                else:
                    logger.info("starting elastic.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elastic.py',self.dirname,self.rawinputdir+'/mon',str(expected_processes)]

                self.elastic_monitor = subprocess.Popen(elastic_args,
                                                        preexec_fn=preexec_function,
                                                        close_fds=True
                                                        )
            except OSError as ex:
                logger.error("failed to start elasticsearch client")
                logger.error(ex)
            try:nsslock.release()
            except:pass
        if conf.role == "fu" and conf.dqm_machine==False:
            try:
                logger.info("starting anelastic.py with arguments:"+self.dirname)
                elastic_args = ['/opt/hltd/python/anelastic.py',self.dirname,str(self.runnumber), self.rawinputdir,bu_disk_list_output_instance[0]]
                self.anelastic_monitor = subprocess.Popen(elastic_args,
                                                    preexec_fn=preexec_function,
                                                    close_fds=True
                                                    )
            except OSError as ex:
                logger.fatal("failed to start anelastic.py client:")
                logger.exception(ex)
                sys.exit(1)

    def __del__(self):
        self.stopThreads=True
        self.threadEvent.set()
        if self.completedChecker:
            try:
                self.completedChecker.join()
            except RuntimeError:
                pass
        if self.elasticBUWatchdog:
            try:
                self.elasticBUWatchdog.join()
            except RuntimeError:
                pass
        logger.info('Run '+ str(self.runnumber) +' object __del__ has completed')

    def countOwnedResourcesFrom(self,resourcelist):
        ret = 0
        for resourcename in resourcelist:
            try:
                if resourcename in resourcenames:
                    ret+=1
            except:pass
        return ret

    def AcquireResource(self,resourcenames,fromstate):
        idles = conf.resource_base+'/'+fromstate+'/'
        try:
            logger.debug("Trying to acquire resource "
                          +str(resourcenames)
                          +" from "+fromstate)

            for resourcename in resourcenames:
              os.rename(idles+resourcename,used+resourcename)
              self.n_used+=1
            if not filter(lambda x: x.cpu==resourcenames,self.online_resource_list):
                logger.debug("resource(s) "+str(resourcenames)
                              +" not found in online_resource_list, creating new")
                self.online_resource_list.append(OnlineResource(self,resourcenames,self.lock))
                return self.online_resource_list[-1]
            logger.debug("resource(s) "+str(resourcenames)
                          +" found in online_resource_list")
            return filter(lambda x: x.cpu==resourcenames,self.online_resource_list)[0]
        except Exception as ex:
            logger.info("exception encountered in looking for resources")
            logger.info(ex)

    def ContactResource(self,resourcename):
        self.online_resource_list.append(OnlineResource(self,resourcename,self.lock))
        self.online_resource_list[-1].ping() #@@MO this is not doing anything useful, afaikt

    def ReleaseResource(self,res):
        self.online_resource_list.remove(res)

    def AcquireResources(self,mode):
        logger.info("acquiring resources from "+conf.resource_base)
        idles = conf.resource_base
        idles += '/idle/' if conf.role == 'fu' else '/boxes/'
        try:
            dirlist = os.listdir(idles)
        except Exception as ex:
            logger.info("exception encountered in looking for resources")
            logger.info(ex)
        logger.info(str(dirlist))
        current_time = time.time()
        count = 0
        cpu_group=[]
        #self.lock.acquire()

        global machine_blacklist
        if conf.role=='bu':
            update_success,machine_blacklist=updateBlacklist()
            if update_success==False:
                logger.fatal("unable to check blacklist: giving up on run start")
                return False

        for cpu in dirlist:
            #skip self
            if conf.role=='bu':
                if cpu == os.uname()[1]:continue
                if cpu in machine_blacklist:
                    logger.info("skipping blacklisted resource "+str(cpu))
                    continue
                if self.checkStaleResourceFile(idles+cpu):
                    logger.error("Skipping stale resource "+str(cpu))
                    continue

            count = count+1
            try:
                age = current_time - os.path.getmtime(idles+cpu)
                cpu_group.append(cpu)
                if conf.role == 'fu':
                    if count == nstreams:
                      self.AcquireResource(cpu_group,'idle')
                      cpu_group=[]
                      count=0
                else:
                    logger.info("found resource "+cpu+" which is "+str(age)+" seconds old")
                    if age < 10:
                        cpus = [cpu]
                        self.ContactResource(cpus)
            except Exception as ex:
                logger.error('encountered exception in acquiring resource '+str(cpu)+':'+str(ex))
        return True
        #self.lock.release()

    def checkStaleResourceFile(self,resourcepath):
        try:
            with open(resourcepath,'r') as fi:
                doc = json.load(fi)
                if doc['detectedStaleHandle']==True:
                    return True
        except:
            logger.warning('can not parse ' + rfile)
        return False

    def CheckTemplate(self):
        if conf.role=='bu' and conf.use_elasticsearch:
            logger.info("checking ES template")
            try:
                setupES()
            except Exception as ex:
                logger.error("Unable to check run appliance template:"+str(ex))

    def Start(self):
        self.is_ongoing_run = True
        for resource in self.online_resource_list:
            logger.info('start run '+str(self.runnumber)+' on cpu(s) '+str(resource.cpu))
            if conf.role == 'fu':
                self.StartOnResource(resource)
            else:
                is_stale = resource.checkIfStaleResource()
                if not is_stale:
                    resource.NotifyNewRun(self.runnumber)
                else:
                    logger.error("Run "+str(self.runnumber)+" start: skipping resource "+resource.cpu+" which is stale")
                #update begin time to after notifying FUs
                self.beginTime = datetime.datetime.now()
        if conf.role == 'fu' and conf.dqm_machine==False:
            self.changeMarkerMaybe(Run.ACTIVE)
            #start safeguard monitoring of anelastic.py
            self.startAnelasticWatchdog()
        elif conf.role == 'bu':
            self.startElasticBUWatchdog()
            self.startCompletedChecker()

    def maybeNotifyNewRun(self,resourcename,resourceage):
        if conf.role=='fu':
            logger.fatal('this function should *never* have been called when role == fu')
            return

        if self.rawinputdir != None:
            #TODO:check also for EoR file?
            try:
                os.stat(self.rawinputdir)
            except:
                logger.warning('Unable to find raw directory of '+str(self.runnumber))
                return None

        for resource in self.online_resource_list:
            if resourcename in resource.cpu:
                logger.error('Resource '+str(resource.cpu)+' was already processing run ' + str(self.runnumber) + '. Will not participate in this run.')
                return None
            if resourcename in machine_blacklist:
                logger.info("skipping blacklisted resource "+str(resource.cpu))
                return None
        current_time = time.time()
        age = current_time - resourceage
        logger.info("found resource "+resourcename+" which is "+str(age)+" seconds old")
        if age < 10:
            self.ContactResource([resourcename])
            return self.online_resource_list[-1]
        else:
            return None

    def StartOnResource(self, resource):
        logger.debug("StartOnResource called")
        resource.statefiledir=conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
        mondir = os.path.join(resource.statefiledir,'mon')
        resource.associateddir=mondir
        resource.StartNewProcess(self.runnumber,
                                 self.online_resource_list.index(resource),
                                 self.arch,
                                 self.version,
                                 self.menu_path,
                                 self.transfermode,
                                 int(round((len(resource.cpu)*float(nthreads)/nstreams))),
                                 len(resource.cpu))
        logger.debug("StartOnResource process started")
        #logger.debug("StartOnResource going to acquire lock")
        #self.lock.acquire()
        #logger.debug("StartOnResource lock acquired")
        try:
            os.makedirs(mondir)
        except OSError:
            pass
        monfile = mondir+'/hltd.jsn'

        fp=None
        stat = []
        if not os.path.exists(monfile):
            logger.debug("No log file "+monfile+" found, creating one")
            fp=open(monfile,'w+')
            attempts=0
            while True:
                try:
                    stat.append([resource.cpu,resource.process.pid,resource.processstate])
                    break
                except:
                    if attempts<5:
                        attempts+=1
                        continue
                    else:
                        logger.error("could not retrieve process parameters")
                        logger.exception(ex)
                        break

        else:
            logger.debug("Updating existing log file "+monfile)
            fp=open(monfile,'r+')
            stat=json.load(fp)
            attempts=0
            while True:
                try:
                    me = filter(lambda x: x[0]==resource.cpu, stat)
                    if me:
                        me[0][1]=resource.process.pid
                        me[0][2]=resource.processstate
                    else:
                        stat.append([resource.cpu,resource.process.pid,resource.processstate])
                    break
                except Exception as ex:
                    if attempts<5:
                        attempts+=1
                        time.sleep(.05)
                        continue
                    else:
                        logger.error("could not retrieve process parameters")
                        logger.exception(ex)
                        break
        fp.seek(0)
        fp.truncate()
        json.dump(stat,fp)

        fp.flush()
        fp.close()
        #self.lock.release()
        #logger.debug("StartOnResource lock released")

    def Stop(self):
        #used to gracefully stop CMSSW and finish scripts
        with open(os.path.join(self.dirname,"temp_CMSSW_STOP"),'w') as f:
          writedoc = {}
          bu_lumis = []
          try:
            bu_eols_files = filter( lambda x: x.endswith("_EoLS.jsn"),os.listdir(self.rawinputdir))
            bu_lumis = (sorted([int(x.split('_')[1][2:]) for x in bu_eols_files]))
          except:
            logger.error("Unable to parse BU EoLS files")
          ls_delay=3
          if len(bu_lumis):
              logger.info('last closed lumisection in ramdisk is '+str(bu_lumis[-1])+', requesting to close at LS '+ str(bu_lumis[-1]+ls_delay))
              writedoc['lastLS']=bu_lumis[-1]+ls_delay #current+delay
          else:  writedoc['lastLS']=ls_delay
          json.dump(writedoc,f)
        try:
          os.rename(os.path.join(self.dirname,"temp_CMSSW_STOP"),os.path.join(self.dirname,"CMSSW_STOP"))
        except:pass
        

    def Shutdown(self,killJobs=False,killScripts=False):
        #herod mode sends sigkill to all process, however waits for all scripts to finish
        logger.debug("Run:Shutdown called")
        self.pending_shutdown=False
        self.is_ongoing_run = False

        try:
            self.changeMarkerMaybe(Run.ABORTED)
        except OSError as ex:
            pass

        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            time.sleep(.1)
            for resource in self.online_resource_list:
                if conf.role == 'fu':
                    if resource.processstate==100:
                        logger.info('terminating process '+str(resource.process.pid)+
                                     ' in state '+str(resource.processstate)+' owning '+str(resource.cpu))

                        if killJobs:resource.process.kill()
                        else:resource.process.terminate()
                        if resource.watchdog!=None and resource.watchdog.is_alive():
                            try:
                                resource.join()
                            except:
                                pass
                        logger.info('process '+str(resource.process.pid)+' terminated')
                    time.sleep(.1) 
                    logger.info(' releasing resource(s) '+str(resource.cpu))
                    resource_lock.acquire()
                    q_clear_condition = (not self.checkQuarantinedLimit()) or conf.auto_clear_quarantined
                    cleared_q = resource.clearQuarantined(doLock=False,restore=q_clear_condition)
                    for cpu in resource.cpu:
                        if cpu not in cleared_q:
                            try:
                                os.rename(used+cpu,idles+cpu)
                                self.n_used-=1
                            except OSError:
                                #@SM:can happen if it was quarantined
                                logger.warning('Unable to find resource '+used+cpu)
                            except Exception as ex:
                                resource_lock.release()
                                raise(ex)
                    resource_lock.release()
                    resource.process=None

            self.online_resource_list = []
            try:
                self.changeMarkerMaybe(Run.ABORTCOMPLETE)
            except OSError as ex:
                pass
            try:
                if self.anelastic_monitor:
                    if killScripts:
                        self.anelastic_monitor.terminate()
                    self.anelastic_monitor.wait()
            except OSError as ex:
                if ex.errno==3:
                    logger.info("anelastic.py for run " + str(self.runnumber) + " is not running")
            except Exception as ex:
                logger.exception(ex)
            if conf.use_elasticsearch == True:
                try:
                    if self.elastic_monitor:
                        if killScripts:
                            self.elastic_monitor.terminate()
                        self.elastic_monitor.wait()
                except OSError as ex:
                    if ex.errno==3:
                        logger.info("elastic.py for run " + str(self.runnumber) + " is not running")
                    else :logger.exception(ex)
                except Exception as ex:
                    logger.exception(ex)
            if self.waitForEndThread is not None:
                self.waitForEndThread.join()
        except Exception as ex:
            logger.info("exception encountered in shutting down resources")
            logger.exception(ex)

        resource_lock.acquire()
        try:
            runList.remove(self.runnumber)
        except Exception as ex:
            logger.exception(ex)
        resource_lock.release()

        try:
            if conf.delete_run_dir is not None and conf.delete_run_dir == True:
                shutil.rmtree(conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding))
            os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
        except:
            pass

        logger.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' completed')

    def ShutdownBU(self):

        self.is_ongoing_run = False
        try:
            if self.elastic_monitor:
                #first check if process is alive
                if self.elastic_monitor.poll() is None:
                    self.elastic_monitor.terminate()
                    time.sleep(.1)
        except Exception as ex:
            logger.info("exception encountered in shutting down elasticbu.py: " + str(ex))
            #logger.exception(ex)

        #should also trigger destructor of the Run

        resource_lock.acquire()
        try:
            runList.remove(self.runnumber)
        except Exception as ex:
            logger.exception(ex)
        resource_lock.release()

        logger.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' on BU completed')


    def StartWaitForEnd(self):
        self.is_ongoing_run = False
        self.changeMarkerMaybe(Run.STOPPING)
        try:
            self.waitForEndThread = threading.Thread(target = self.WaitForEnd)
            self.waitForEndThread.start()
        except Exception as ex:
            logger.info("exception encountered in starting run end thread")
            logger.info(ex)

    def WaitForEnd(self):
        logger.info("wait for end thread!")
        global cloud_mode
        global entering_cloud_mode
        global abort_cloud_mode
        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            for resource in self.online_resource_list:
                if resource.processstate is not None:#was:100
                    if resource.process is not None and resource.process.pid is not None: ppid = resource.process.pid
                    else: ppid="None"
                    logger.info('waiting for process '+str(ppid)+
                                 ' in state '+str(resource.processstate) +
                                 ' to complete ')
                    try:
                        resource.join()
                        logger.info('process '+str(resource.process.pid)+' completed')
                    except:pass
                resource.clearQuarantined()
                resource.process=None
            self.online_resource_list = []
            if conf.role == 'fu':
                logger.info('writing complete file')
                self.changeMarkerMaybe(Run.COMPLETE)
                try:
                    os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
                except:pass
                try:
                    if conf.dqm_machine==False:
                        self.anelastic_monitor.wait()
                except OSError,ex:
                    if "No child processes" not in str(ex):
                        logger.info("Exception encountered in waiting for termination of anelastic:" +str(ex))
                self.anelastic_monitor = None

            if conf.use_elasticsearch == True:
                try:
                    self.elastic_monitor.wait()
                except OSError,ex:
                    if "No child processes" not in str(ex):
                        logger.info("Exception encountered in waiting for termination of anelastic:" +str(ex))
                self.elastic_monitor = None
            if conf.delete_run_dir is not None and conf.delete_run_dir == True:
                try:
                    shutil.rmtree(self.dirname)
                except Exception as ex:
                    logger.exception(ex)

            global runList
            #todo:clear this external thread
            resource_lock.acquire()
            logger.info("active runs.."+str(runList.getActiveRunNumbers()))
            try:
                runList.remove(self.runnumber)
            except Exception as ex:
                logger.exception(ex)
            logger.info("new active runs.."+str(runList.getActiveRunNumbers()))

            global resources_blocked_flag
            if cloud_mode==True:
                if len(runList.getActiveRunNumbers())>=1:
                    logger.info("VM mode: waiting for runs: " + str(runList.getActiveRunNumbers()) + " to finish")
                else:
                    logger.info("No active runs. moving all resource files to cloud")
                    #give resources to cloud and bail out
                    entering_cloud_mode=False 
                    #check if cloud mode switch has been aborted in the meantime
                    if abort_cloud_mode:
                        abort_cloud_mode=False
                        resources_blocked_flag=True
                        cloud_mode=False
                        resource_lock.release()
                        return

                    move_resources_to_cloud()
                    resource_lock.release()
                    ignite_cloud()
                    logger.info("cloud is on? : "+str(is_cloud_inactive()==False))
            try:resource_lock.release()
            except:pass

        except Exception as ex:
            logger.error("exception encountered in ending run")
            logger.exception(ex)
            try:resource_lock.release()
            except:pass

    def changeMarkerMaybe(self,marker):
        dir = self.dirname
        current = filter(lambda x: x in Run.VALID_MARKERS, os.listdir(dir))
        if (len(current)==1 and current[0] != marker) or len(current)==0:
            if len(current)==1: os.remove(dir+'/'+current[0])
            fp = open(dir+'/'+marker,'w+')
            fp.close()
        else:
            logger.error("There are more than one markers for run "
                          +str(self.runnumber))
            return

    def checkQuarantinedLimit(self):
        allQuarantined=True
        for r in self.online_resource_list:
            try:
                if r.watchdog.quarantined==False or r.processstate==100:allQuarantined=False
            except:
                allQuarantined=False
        if allQuarantined==True:
            return True
        else:
            return False

    def startAnelasticWatchdog(self):
        try:
            self.anelasticWatchdog = threading.Thread(target = self.runAnelasticWatchdog)
            self.anelasticWatchdog.start()
        except Exception as ex:
            logger.info("exception encountered in starting anelastic watchdog thread")
            logger.info(ex)

    def runAnelasticWatchdog(self):
        try:
            self.anelastic_monitor.wait()
            if self.is_ongoing_run == True:
                #abort the run
                self.anelasticWatchdog=None
                logger.warning("Premature end of anelastic.py")
                self.Shutdown(killJobs=True,killScripts=True)
        except:
            pass
        self.anelastic_monitor=None

    def startElasticBUWatchdog(self):
        try:
            self.elasticBUWatchdog = threading.Thread(target = self.runElasticBUWatchdog)
            self.elasticBUWatchdog.start()
        except Exception as ex:
            logger.info("exception encountered in starting elasticbu watchdog thread")
            logger.info(ex)

    def runElasticBUWatchdog(self):
        try:
            self.elastic_monitor.wait()
        except:
            pass
        self.elastic_monitor=None

    def startCompletedChecker(self):

        try:
            logger.info('start checking completition of run '+str(self.runnumber))
            self.completedChecker = threading.Thread(target = self.runCompletedChecker)
            self.completedChecker.start()
        except Exception,ex:
            logger.error('failure to start run completition checker:')
            logger.exception(ex)

    def runCompletedChecker(self):

        rundirstr = 'run'+ str(self.runnumber).zfill(conf.run_number_padding)
        rundirCheckPath = os.path.join(conf.watch_directory, rundirstr)
        eorCheckPath = os.path.join(rundirCheckPath,rundirstr + '_ls0000_EoR.jsn')
 
        self.threadEvent.wait(10)
        while self.stopThreads == False:
            self.threadEvent.wait(5)
            if os.path.exists(eorCheckPath) or os.path.exists(rundirCheckPath)==False:
                logger.info("Completed checker: detected end of run "+str(self.runnumber))
                break

        while self.stopThreads==False:
            self.threadEvent.wait(5)
            success, runFound = self.checkNotifiedBoxes()
            if success and runFound==False:
                resource_lock.acquire()
                try:
                    runList.remove(self.runnumber)
                except Exception as ex:
                    logger.exception(ex)
                resource_lock.release()
                logger.info("Completed checker: end of processing of run "+str(self.runnumber))
                break

    def createEmptyEoRMaybe(self):

        #this is used to notify elasticBU to fill the end time before it is terminated
        rundirstr = 'run'+ str(self.runnumber).zfill(conf.run_number_padding)
        rundirCheckPath = os.path.join(conf.watch_directory, rundirstr)
        eorCheckPath = os.path.join(rundirCheckPath,rundirstr + '_ls0000_EoR.jsn')
        try:
            os.stat(eorCheckPath)
        except:
            logger.info('creating empty EoR file in run directory '+rundirCheckPath)
            try:
                with open(eorCheckPath,'w') as fi:
                    pass
                time.sleep(.5)
            except Exception as ex:
                logger.exception(ex)

    def checkNotifiedBoxes(self):
        keys = boxinfoFUMap.keys()
        c_time = time.time()
        for key in keys:
            #if key==thishost:continue #checked in inotify thread
            try:
                edata,etime,lastStatus = boxinfoFUMap[key]
            except:
                #key deleted
                return False,False
            if c_time - etime > 20:continue
            #parsing or file access, check failed
            if lastStatus==False: return False,False
            try:
                #run is found in at least one box
                if self.runnumber in edata['activeRuns']:return True,True
            except:
                #invalid boxinfo data
                return False,False
        #all box data are valid, run not found
        return True,False


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

    def isHighestRun(self,runObj):
        return len(filter(lambda x: x.runnumber>runObj.runnumber,self.runs))==0

    def getStateDoc(self):
        docArray = []
        for runObj in self.runs:
          docArray.append({'run':runObj.runnumber,'totalRes':runObj.n_used,'qRes':runObj.n_quarantined,'ongoing':runObj.is_ongoing_run,'errors':runObj.num_errors})
        return docArray


class RunRanger:

    def __init__(self,instance):
        self.inotifyWrapper = InotifyWrapper(self)
        self.instance = instance

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self):
        self.inotifyWrapper.stop()
        self.inotifyWrapper.join()
        logger.info("RunRanger: Inotify wrapper shutdown done")

    def process_IN_CREATE(self, event):
        nr=0
        global runList
        global cloud_mode
        global entering_cloud_mode
        global exiting_cloud_mode
        global abort_cloud_mode
        global resources_blocked_flag
        global cached_pending_run
        fullpath = event.fullpath
        logger.info('RunRanger: event '+fullpath)
        dirname=fullpath[fullpath.rfind("/")+1:]
        logger.info('RunRanger: new filename '+dirname)
        if dirname.startswith('run'):
            if dirname.endswith('.reprocess'):
                #reprocessing triggered
                dirname = dirname[:dirname.rfind('.reprocess')]
                fullpath = fullpath[:fullpath.rfind('.reprocess')]
                logger.info('Triggered reprocessing of '+ dirname)
                try:os.unlink(event.fullpath)
                except:
                    try:os.rmdir(event.fullpath)
                    except:pass
            if os.path.islink(fullpath):
                logger.info('directory ' + fullpath + ' is link. Ignoring this run')
                return
            if not os.path.isdir(fullpath):
                logger.info(fullpath +' is a file. A directory is needed to start a run.')
                return
            nr=int(dirname[3:])
            if nr!=0:
                # the dqm BU processes a run if the "global run file" is not mandatory or if the run is a global run
                is_global_run = os.path.exists(fullpath[:fullpath.rfind("/")+1] + dqm_globalrun_filepattern.format(str(nr).zfill(conf.run_number_padding)))
                dqm_processing_criterion = (not conf.dqm_globallock) or (conf.role != 'bu') or  (is_global_run)

                if (not conf.dqm_machine) or dqm_processing_criterion:
                    try:
                        logger.info('new run '+str(nr))
                        #terminate quarantined runs     
                        for run in runList.getQuarantinedRuns():
                            #run shutdown waiting for scripts to finish
                            run.Shutdown(True,False)
                            time.sleep(.1)

                        resources_blocked_flag=False
                        if cloud_mode==True:
                            logger.info("received new run notification in CLOUD mode. Ignoring new run.")
                            #remember this run and attempt to continue it once hltd exits the cloud mode
                            cached_pending_run = fullpath
                            os.rmdir(fullpath)
                            return
                        if conf.role == 'fu':
                            bu_dir = bu_disk_list_ramdisk_instance[0]+'/'+dirname
                            try:
                                os.symlink(bu_dir+'/jsd',fullpath+'/jsd')
                            except:
                                if not conf.dqm_machine:
                                    logger.warning('jsd directory symlink error, continuing without creating link')
                                pass
                        else:
                            bu_dir = ''

                        #check if this run is a duplicate
                        if runList.getRun(nr)!=None:
                            raise Exception("Attempting to create duplicate run "+str(nr))
 
                        # in case of a DQM machines create an EoR file
                        if conf.dqm_machine and conf.role == 'bu':
                            for run in runList.getOngoingRuns():
                                EoR_file_name = run.dirname + '/' + 'run' + str(run.runnumber).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn'
                                if run.is_ongoing_run and not os.path.exists(EoR_file_name):
                                    # create an EoR file that will trigger all the running jobs to exit nicely
                                    open(EoR_file_name, 'w').close()

                        run = Run(nr,fullpath,bu_dir,self.instance)
                        if not run.inputdir_exists and conf.role=='fu':
                            logger.info('skipping '+ fullpath + ' with raw input directory missing')
                            shutil.rmtree(fullpath)
                            del(run)
                            return
                        resource_lock.acquire()
                        runList.add(run)
                        if run.AcquireResources(mode='greedy'):
                            run.CheckTemplate()
                            run.Start()
                        else:
                            #BU mode: failed to get blacklist
                            runList.remove(nr)
                            resource_lock.release()
                            del(run)
                            return
                        resource_lock.release()

                        if conf.role == 'bu' and conf.instance != 'main':
                            logger.info('creating run symlink in main ramdisk directory')
                            main_ramdisk = os.path.dirname(os.path.normpath(conf.watch_directory))
                            os.symlink(fullpath,os.path.join(main_ramdisk,os.path.basename(fullpath)))
                    except OSError as ex:
                        logger.error("RunRanger: "+str(ex)+" "+ex.filename)
                        logger.exception(ex)
                    except Exception as ex:
                        logger.error("RunRanger: unexpected exception encountered in forking hlt slave")
                        logger.exception(ex)
                    try:resource_lock.release()
                    except:pass

        elif dirname.startswith('emu'):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    """
                    start a new BU emulator run here - this will trigger the start of the HLT run
                    """
                    bu_emulator.startNewRun(nr)

                except Exception as ex:
                    logger.info("exception encountered in starting BU emulator run")
                    logger.info(ex)

                os.remove(fullpath)

        elif dirname.startswith('end'):
            # need to check is stripped name is actually an integer to serve
            # as run number
            if dirname[3:].isdigit():
                nr=int(dirname[3:])
                if nr!=0:
                    try:
                        endingRun = runList.getRun(nr)
                        if endingRun==None:
                            logger.warning('request to end run '+str(nr)
                                          +' which does not exist')
                            os.remove(fullpath)
                        else:
                            logger.info('end run '+str(nr))
                            #remove from runList to prevent intermittent restarts
                            #lock used to fix a race condition when core files are being moved around
                            endingRun.is_ongoing_run==False
                            time.sleep(.1)
                            if conf.role == 'fu':
                                endingRun.StartWaitForEnd()
                            if bu_emulator and bu_emulator.runnumber != None:
                                bu_emulator.stop()
                            #logger.info('run '+str(nr)+' removing end-of-run marker')
                            #os.remove(fullpath)

                    except Exception as ex:
                        logger.info("exception encountered when waiting hlt run to end")
                        logger.info(ex)
                else:
                    logger.error('request to end run '+str(nr)
                                  +' which is an invalid run number - this should '
                                  +'*never* happen')
            else:
                logger.error('request to end run '+str(nr)
                              +' which is NOT a run number - this should '
                              +'*never* happen')

        elif dirname.startswith('herod'):
            os.remove(fullpath)
            if conf.role == 'fu':
                logger.info("killing all CMSSW child processes")
                for run in runList.getActiveRuns():
                    run.Shutdown(True,False)
                time.sleep(.2)
                #clear all quarantined cores
                for cpu in q_list:
                    try:
                        logger.info('Clearing quarantined resource '+cpu)
                        os.rename(quarantined+cpu,idles+cpu)
                    except:
                        logger.info('Qquarantined resource was already cleared: '+cpu)

            elif conf.role == 'bu':
                for run in runList.getActiveRuns():
                    run.createEmptyEoRMaybe()
                    run.ShutdownBU()

                #delete input and output BU directories
                cleanup_bu_disks()

                #contact any FU that appears alive
                boxdir = conf.resource_base +'/boxes/'
                try:
                    dirlist = os.listdir(boxdir)
                    current_time = time.time()
                    logger.info("sending herod to child FUs")
                    for name in dirlist:
                        if name == os.uname()[1]:continue
                        age = current_time - os.path.getmtime(boxdir+name)
                        logger.info('found box '+name+' with keepalive age '+str(age))
                        if age < 20:
                            connection = httplib.HTTPConnection(name, conf.cgi_port - conf.cgi_instance_port_offset)
                            time.sleep(0.05)
                            connection.request("GET",'cgi-bin/herod_cgi.py')
                            time.sleep(0.1)
                            response = connection.getresponse()
                    logger.info("sent herod to all child FUs")
                except Exception as ex:
                    logger.error("exception encountered in contacting resources")
                    logger.info(ex)
        elif dirname.startswith('populationcontrol'):
            if len(runList.runs)>0:
                logger.info("terminating all ongoing runs via cgi interface (populationcontrol): "+str(runList.getActiveRunNumbers()))
                for run in runList.getActiveRuns():
                    if conf.role=='fu':
                        run.Shutdown(True,True)
                    elif conf.role=='bu':
                        run.ShutdownBU()
                logger.info("terminated all ongoing runs via cgi interface (populationcontrol)")
            os.remove(fullpath)

        elif dirname.startswith('harakiri') and conf.role == 'fu':
            os.remove(fullpath)
            pid=os.getpid()
            logger.info('asked to commit seppuku:'+str(pid))
            try:
                logger.info('sending signal '+str(SIGKILL)+' to myself:'+str(pid))
                retval = os.kill(pid, SIGKILL)
                logger.info('sent SIGINT to myself:'+str(pid))
                logger.info('got return '+str(retval)+'waiting to die...and hope for the best')
            except Exception as ex:
                logger.error("exception in committing harakiri - the blade is not sharp enough...")
                logger.error(ex)

        elif dirname.startswith('quarantined'):
            try:
                os.remove(dirname)
            except:
                pass
            if dirname[11:].isdigit():
                nr=int(dirname[11:])
                if nr!=0:
                    try:
                        run = runList.getRun(nr)
                        if run.checkQuarantinedLimit():
                            if runList.isHighestRun(run):
                                run.pending_shutdown=True
                            else:
                                run.Shutdown(True,False)
                    except Exception as ex:
                        logger.exception(ex)

        elif dirname.startswith('suspend') and conf.role == 'fu':
            logger.info('suspend mountpoints initiated')
            replyport = int(dirname[7:]) if dirname[7:].isdigit()==True else conf.cgi_port
            global suspended
            suspended=True

            #terminate all ongoing runs
            for run in runList.getActiveRuns():
                run.Shutdown(True,True)
 
            time.sleep(.5)
            #local request used in case of stale file handle
            if replyport==0:
                umount_success = cleanup_mountpoints()
                try:os.remove(fullpath)
                except:pass
                suspended=False
                logger.info("Remount requested locally is performed.")
                return

            umount_success = cleanup_mountpoints(remount=False)

            if umount_success==False:
                time.sleep(1)
                logger.error("Suspend initiated from BU failed, trying again...")
                #notifying itself again
                try:os.remove(fullpath)
                except:pass
                fp = open(fullpath,"w+")
                fp.close()
                return 

            #find out BU name from bus_config
            bu_name=None
            bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
            if os.path.exists(bus_config):
                for line in open(bus_config):
                    bu_name=line.split('.')[0]
                    break

            #first report to BU that umount was done
            try:
                if bu_name==None:
                    logger.fatal("No BU name was found in the bus.config file. Leaving mount points unmounted until the hltd service restart.")
                    os.remove(fullpath)
                    return
                connection = httplib.HTTPConnection(bu_name, replyport+20,timeout=5)
                connection.request("GET",'cgi-bin/report_suspend_cgi.py?host='+os.uname()[1])
                response = connection.getresponse()
            except Exception as ex:
                logger.error("Unable to report suspend state to BU "+str(bu_name)+':'+str(replyport+20))
                logger.exception(ex)

            #loop while BU is not reachable
            while True:
                try:
                    #reopen bus.config in case is modified or moved around
                    bu_name=None
                    bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
                    if os.path.exists(bus_config):
                        try:
                            for line in open(bus_config):
                                bu_name=line.split('.')[0]
                                break
                        except:
                            logger.info('exception test 1')
                            time.sleep(5)
                            continue
                    if bu_name==None:
                        logger.info('exception test 2')
                        time.sleep(5)
                        continue

                    logger.info('checking if BU hltd is available...')
                    connection = httplib.HTTPConnection(bu_name, replyport,timeout=5)
                    connection.request("GET",'cgi-bin/getcwd_cgi.py')
                    response = connection.getresponse()
                    logger.info('BU hltd is running !...')
                    #if we got here, the service is back up
                    break
                except Exception as ex:
                    try:
                       logger.info('Failed to contact BU hltd service: ' + str(ex.args[0]) +" "+ str(ex.args[1]))
                    except:
                       logger.info('Failed to contact BU hltd service '+str(ex))
                    time.sleep(5)

            #mount again
            cleanup_mountpoints()
            try:os.remove(fullpath)
            except:pass
            suspended=False
            logger.info("Remount is performed")

        elif dirname.startswith('exclude') and conf.role == 'fu':
            #service on this machine is asked to be excluded for cloud use
            if cloud_mode:
                logger.info('already in cloud mode')
                os.remove(fullpath)
                return
            else:
                logger.info('machine exclude initiated')

            if is_cloud_inactive()>=100:
                logger.error("Unable to switch to cloud mode (igniter script error)")
                os.remove(fullpath)
                return

            try:
                #TODO:avoid keeping the run for long..
                for run in runList.getQuarantinedRuns():
                    run.Shutdown(True,False)
            except Exception as ex:
                logger.fatal("Unable to clear quarantined runs. Will not enter VM mode.")
                logger.exception(ex)
                os.remove(fullpath)
                return #
 
            resource_lock.acquire()
            cloud_mode=True
            entering_cloud_mode=True
            try:
                #check again for quarantined runs
                for run in runList.getQuarantinedRuns():
                    run.Shutdown(True,False)

                listOfActiveRuns = runList.getActiveRuns()
                if len(listOfActiveRuns)>0:
                    for run in listOfActiveRuns:
                        #write signal file for CMSSW to quit with 0 after certain LS
                        run.Stop()
                else:
                    #no runs present, switch to cloud mode immediately
                    entering_cloud_mode=False
                    move_resources_to_cloud()
                    resource_lock.release()
                    ignite_cloud()
                    logger.info("cloud is on? : "+str(is_cloud_inactive()==False))
            except Exception as ex:
                logger.fatal("Unable to clear runs. Will not enter VM mode.")
                logger.exception(ex)
                cloud_mode=False
            try:resource_lock.release()
            except:pass
            ####move resources to cloud even if CMSSW aren't finished, and check again later
            #move_resources_to_cloud()
            os.remove(fullpath)

        elif dirname.startswith('include') and conf.role == 'fu':
            #TODO: pick up latest ongoing run when activated ?
            # even if this run was not active before on this FU? (problem with FU EoR in BU output event counting)
            if cloud_mode==False:
                logger.error('received notification to exit from cloud but machine is not in cloud mode!')
                os.remove(fullpath)
                if not is_cloud_inactive():
                    logger.info('cloud scripts are running, trying to stop')
                    extinguish_cloud()
                return

            resource_lock.acquire()
            if entering_cloud_mode:
                abort_cloud_mode=True
                resource_lock.release()
                os.remove(fullpath)
                return
            resource_lock.release()

            #run stop cloud notification
            exiting_cloud_mode=True
            if is_cloud_inactive():
                logger.warning('received command to deactivate cloud, but cloud scripts are not running!')
            extinguish_cloud()

            while True:
                last_status = is_cloud_inactive()
                if last_status==0: #state: running
                    logger.info('cloud scripts are still active')
                    time.sleep(1)
                    continue
                else:
                    logger.info('cloud scripts have been deactivated')
                    if last_status>1:
                        logger.warning('Received error code from cloud igniter script. Switching off cloud mode')
                    resource_lock.acquire()
                    resources_blocked_flag=True
                    cloud_mode=False
                    cleanup_resources()
                    resource_lock.release()
                    break
            exiting_cloud_mode=False
            os.remove(fullpath)
            if cached_pending_run != None:
                #create last pending run received during the cloud mode
                time.sleep(5) #let core file notifications run
                os.mkdir(cached_pending_run)
                cached_pending_run = None
            else: time.sleep(2)
            logger.info('cloud mode in hltd has been switched off')

        elif dirname.startswith('logrestart'):
            #hook to restart logcollector process manually
            restartLogCollector(self.instance)
            os.remove(fullpath)

        logger.debug("RunRanger completed handling of event "+fullpath)

    def process_default(self, event):
        logger.info('RunRanger: event '+event.fullpath+' type '+str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

class ResourceRanger:

    def __init__(self):
        self.inotifyWrapper = InotifyWrapper(self)

        self.managed_monitor = system_monitor()
        self.managed_monitor.start()
        self.regpath = []

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
            resource_lock.acquire()

            if not (resourcestate == 'online' or resourcestate == 'cloud'
                    or resourcestate == 'quarantined'):
                logger.debug('ResourceNotifier: new resource '
                              +resourcename
                              +' in '
                              +resourcepath
                              +' state '
                              +resourcestate
                              )

                if cloud_mode and not entering_cloud_mode and not exiting_cloud_mode and not abort_cloud_mode:
                    time.sleep(1)
                    logging.info('detected resource moved to non-cloud resource dir while already switched to cloud mode. Deactivating cloud.')
                    with open(os.path.join(conf.watch_directory,'include'),'w+') as fobj:
                        pass
                    resource_lock.release()
                    time.sleep(1)
                    return

                run = runList.getLastOngoingRun()
                if run is not None:
                    logger.info("ResourceRanger: found active run "+str(run.runnumber))
                    """grab resources that become available
                    #@@EM implement threaded acquisition of resources here
                    """
                    #find all idle cores
                    idlesdir = '/'+resourcepath
		    try:
                        reslist = os.listdir(idlesdir)
                    except Exception as ex:
                        logger.info("exception encountered in looking for resources")
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
                        resource_lock.release()
                        return
                    #acquire sufficient cores for a multithreaded process start
                    resourcenames = []
                    for resname in reslist:
                        if len(resourcenames) < nstreams:
                            resourcenames.append(resname)
                        else:
                            break

                    acquired_sufficient = False
                    if len(resourcenames) == nstreams:
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
                        idlesdir = '/'+resourcepath
		        try:
                            reslist = os.listdir(idlesdir)
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
                                resource_lock.release()
                                return
                            resourcenames = []
                            for resname in reslist:
                                if len(resourcenames) < nstreams:
                                    resourcenames.append(resname)
                                else:
                                    break
                            if len(resourcenames) == nstreams:
                                for resname in resourcenames:
                                    os.rename(broken+resname,idles+resname)

                        except Exception as ex:
                            logger.info("exception encountered in looking for resources in except")
                            logger.info(ex)
            elif resourcestate=="cloud":
                #check if cloud mode was initiated, activate if necessary
                if conf.role=='fu' and cloud_mode==False:
                    time.sleep(1)
                    logging.info('detected core moved to cloud resources. Triggering cloud activation sequence.')
                    with open(os.path.join(conf.watch_directory,'exclude'),'w+') as fobj:
                        pass
                    time.sleep(1)
        except Exception as ex:
            logger.error("exception in ResourceRanger")
            logger.error(ex)
        try:
            resource_lock.release()
        except:pass

    def process_IN_MODIFY(self, event):
        logger.debug('ResourceRanger-MODIFY: event '+event.fullpath)
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        try:
            #this should be error (i.e. bus.confg should not be modified during a run)
            bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
            if event.fullpath == bus_config:
                if self.managed_monitor:
                    self.managed_monitor.stop()
                    self.managed_monitor.join()
                cleanup_mountpoints()
                if self.managed_monitor:
                    self.managed_monitor = system_monitor()
                    self.managed_monitor.start()
                    logger.info("ResouceRanger: managed monitor is "+str(self.managed_monitor))
        except Exception as ex:
            logger.error("exception in ResourceRanger")
            logger.error(ex)

    def process_IN_CREATE(self, event):
        logger.debug('ResourceRanger-CREATE: event '+event.fullpath)
        if conf.dqm_machine:return
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        if basename=='blacklist':return
        if basename.startswith('test'):return
        if conf.role!='bu' or basename.endswith(os.uname()[1]):
            return
        try:
            resourceage = os.path.getmtime(event.fullpath)
            resource_lock.acquire()
            lrun = runList.getLastRun()
            newRes = None
            if lrun!=None:
                if lrun.checkStaleResourceFile(event.fullpath):
                    logger.error("Run "+str(lrun.runnumber)+" notification: skipping resource "+basename+" which is stale")
                    resource_lock.release()
                    return
                logger.info('Try attaching FU resource: last run is '+str(lrun.runnumber))
                newRes = lrun.maybeNotifyNewRun(basename,resourceage)
            resource_lock.release()
            if newRes:
                newRes.NotifyNewRun(lrun.runnumber)
        except Exception as ex:
            logger.exception(ex)
            try:resource_lock.release()
            except:pass

    def process_default(self, event):
        logger.debug('ResourceRanger: event '+event.fullpath +' type '+ str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

    def process_IN_CLOSE_WRITE(self, event):
        logger.debug('ResourceRanger-IN_CLOSE_WRITE: event '+event.fullpath)
        global machine_blacklist
        resourcepath=event.fullpath[0:event.fullpath.rfind("/")]
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        if conf.role=='fu':return
        if basename == os.uname()[1]:return
        if basename.startswith('test'):return
        if basename == 'blacklist':
            with open(os.path.join(conf.watch_directory,'appliance','blacklist'),'r') as fi:
                try:
                    machine_blacklist = json.load(fi)
                except:
                    pass
        if resourcepath.endswith('boxes'):
            global boxinfoFUMap
            if basename in machine_blacklist:
                try:boxinfoFUMap.remove(basename)
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

                    boxinfoFUMap[basename] = [infile.data,current_time,True]
                except Exception as ex:
                    if not emptyBox:
                        logger.error("Unable to read of parse boxinfo file "+basename)
                        logger.exception(ex)
                    else:
                        logger.warning("got empty box file "+basename)
                    try:
                        boxinfoFUMap[basename][2]=False
                    except:
                        #boxinfo entry doesn't exist yet
                        boxinfoFUMap[basename]=[None,current_time,False]

    def checkNotifiedBoxes(self,runNumber):
        keys = boxinfoFUMap.keys()
        c_time = time.time()
        for key in keys:
            #if key==thishost:continue #checked in inotify thread
            try:
                edata,etime,lastStatus = boxinfoFUMap[key]
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
            if file == thishost:continue
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

    def stop(self):
        #read configuration file
        try:
            setFromConf(self.instance)
        except Exception as ex:
            print " CONFIGURATION error:",str(ex),"(check configuration file) [  \033[1;31mFAILED\033[0;39m  ]"
            sys.exit(4)

        if self.silentStatus():
            try:
                if os.path.exists(conf.watch_directory+'/populationcontrol'):
                    os.remove(conf.watch_directory+'/populationcontrol')
                fp = open(conf.watch_directory+'/populationcontrol','w+')
                fp.close()
                count = 10
                while count:
                    os.stat(conf.watch_directory+'/populationcontrol')
                    if count==10:
                      sys.stdout.write(' o.o')
                    else:
                      sys.stdout.write('o.o')
                    sys.stdout.flush()
                    time.sleep(.5)
                    count-=1
            except OSError, err:
                time.sleep(.1)
                pass
            except IOError, err:
                time.sleep(.1)
                pass
        super(hltd,self).stop()

    def run(self):
        """
        if role is not defined in the configuration (which it shouldn't)
        infer it from the name of the machine
        """

        #read configuration file
        setFromConf(self.instance)
        logger.info(" ")
        logger.info(" ")
        logger.info("[[[[ ---- hltd start : instance " + self.instance + " ---- ]]]]")
        logger.info(" ")

        if conf.enabled==False:
            logger.warning("Service is currently disabled.")
            sys.exit(1)

        if conf.role == 'fu':

            """
            cleanup resources
            """
            global cloud_mode
            is_in_cloud = len(os.listdir(cloud))>0
            while True:
                #switch to cloud mode if cloud files are found (e.g. machine rebooted while in cloud)
                if is_in_cloud:
                    logger.warning('found cores in cloud. this session will start in the cloud mode')
                    try:
                        move_resources_to_cloud()
                    except:
                        pass
                    cloud_mode=True
                    if is_cloud_inactive():
                        ignite_cloud()
                    break
                if cleanup_resources()==True:break
                time.sleep(0.1)
                logger.warning("retrying cleanup_resources")

            """
            recheck mount points
            this is done at start and whenever the file /etc/appliance/bus.config is modified
            mount points depend on configuration which may be updated (by runcontrol)
            (notice that hltd does not NEED to be restarted since it is watching the file all the time)
            """

            cleanup_mountpoints()

            calculate_threadnumber()

            try:
                os.makedirs(conf.watch_directory)
            except:
                pass

            #recursively remove any stale run data and other commands in the FU watch directory
            #if conf.watch_directory.strip()!='/':
            #    p = subprocess.Popen("rm -rf " + conf.watch_directory.strip()+'/{run*,end*,quarantined*,exclude,include,suspend*,populationcontrol,herod,logrestart,emu*}',shell=True)
            #    p.wait()

            if conf.watch_directory.startswith('/fff'):
                p = subprocess.Popen("rm -rf " + conf.watch_directory+'/*',shell=True)
                p.wait()

            global fu_watchdir_is_mountpoint
            if os.path.ismount(conf.watch_directory):fu_watchdir_is_mountpoint=True

            #switch to cloud mode if active and hltd did not have cores in cloud directory in the last session
            if not is_in_cloud:
                if not is_cloud_inactive():
                    logger.warning("cloud is on on this host at hltd startup, switching to cloud mode")
                    move_resources_to_cloud()
                    cloud_mode=True
 
        if conf.role == 'bu':
            global machine_blacklist
            update_success,machine_blacklist=updateBlacklist()
            global ramdisk_submount_size
            if self.instance == 'main':
                #if there are other instance mountpoints in ramdisk, they will be subtracted from size estimate
                ramdisk_submount_size = submount_size(conf.watch_directory)

        """
        the line below is a VERY DIRTY trick to address the fact that
        BU resources are dynamic hence they should not be under /etc
        """
        conf.resource_base = conf.watch_directory+'/appliance' if conf.role == 'bu' else conf.resource_base

        #@SM:is running from symbolic links still needed?
        watch_directory = os.readlink(conf.watch_directory) if os.path.islink(conf.watch_directory) else conf.watch_directory
        resource_base = os.readlink(conf.resource_base) if os.path.islink(conf.resource_base) else conf.resource_base

        global runList
        runList = RunList()

        if conf.use_elasticsearch == True:
            time.sleep(.2)
            restartLogCollector(self.instance)

        #start boxinfo elasticsearch updater
        global nsslock
        global boxInfo
        boxInfo = None
        if conf.role == 'bu':
            try:os.makedirs(os.path.join(watch_directory,'appliance/dn'))
            except:pass
            try:os.makedirs(os.path.join(watch_directory,'appliance/boxes'))
            except:pass
            if conf.use_elasticsearch == True:
                boxInfo = BoxInfoUpdater(watch_directory,conf,nsslock,boxdoc_version)
                boxInfo.start()

        runRanger = RunRanger(self.instance)
        runRanger.register_inotify_path(watch_directory,inotify.IN_CREATE)
        runRanger.start_inotify()
        logger.info("started RunRanger  - watch_directory " + watch_directory)

        appliance_base=resource_base
        if resource_base.endswith('/'):
            resource_base = resource_base[:-1]
        if resource_base.rfind('/')>0:
            appliance_base = resource_base[:resource_base.rfind('/')]

        rr = ResourceRanger()
        try:
            if conf.role == 'bu':
                imask  = inotify.IN_CLOSE_WRITE | inotify.IN_DELETE | inotify.IN_CREATE | inotify.IN_MOVED_TO
                rr.register_inotify_path(resource_base, imask)
                rr.register_inotify_path(resource_base+'/boxes', imask)
            else:
                #status file for cloud
                #with open(os.path.join(watch_directory,'mode'),'w') as fp:
                #  json.dump({"mode":"hlt"},fp))
                #
                imask_appl  = inotify.IN_MODIFY
                imask  = inotify.IN_MOVED_TO
                rr.register_inotify_path(appliance_base, imask_appl)
                rr.register_inotify_path(resource_base+'/idle', imask)
                rr.register_inotify_path(resource_base+'/cloud', imask)
                rr.register_inotify_path(resource_base+'/except', imask)
            rr.start_inotify()
            logger.info("started ResourceRanger - watch_directory "+resource_base)
        except Exception as ex:
            logger.error("Exception caught in starting ResourceRanger notifier")
            logger.error(ex)

        try:
            cgitb.enable(display=0, logdir="/tmp")
            handler = CGIHTTPServer.CGIHTTPRequestHandler
            # the following allows the base directory of the http
            # server to be 'conf.watch_directory, which is writeable
            # to everybody
            if os.path.exists(watch_directory+'/cgi-bin'):
                os.remove(watch_directory+'/cgi-bin')
            os.symlink('/opt/hltd/cgi',watch_directory+'/cgi-bin')

            handler.cgi_directories = ['/cgi-bin']
            logger.info("starting http server on port "+str(conf.cgi_port))
            httpd = BaseHTTPServer.HTTPServer(("", conf.cgi_port), handler)

            logger.info("hltd serving at port "+str(conf.cgi_port)+" with role "+conf.role)
            os.chdir(watch_directory)
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
            if boxInfo is not None:
                logger.info("stopping boxinfo updater")
                boxInfo.stop()
            global logCollector
            if logCollector is not None:
                logger.info("terminating logCollector")
                logCollector.terminate()
            logger.info("stopping system monitor")
            rr.stop_managed_monitor()
            logger.info("closing httpd socket")
            httpd.socket.close()
            logger.info(threading.enumerate())
            logger.info("unmounting mount points")
            if cleanup_mountpoints(remount=False)==False:
              time.sleep(1)
              cleanup_mountpoints(remount=False)
            
            logger.info("shutdown of service (main thread) completed")
        except Exception as ex:
            logger.info("exception encountered in operating hltd")
            logger.info(ex)
            runRanger.stop_inotify()
            rr.stop_inotify()
            rr.stop_managed_monitor()
            raise


if __name__ == "__main__":
    import procname
    procname.setprocname('hltd')
    daemon = hltd(sys.argv[1])
    daemon.start()
