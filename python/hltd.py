#!/bin/env python
import os,sys
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import time
import datetime
import logging
import subprocess
from signal import SIGKILL
from signal import SIGINT
import json
#import SOAPpy
import threading
import fcntl
import CGIHTTPServer
import BaseHTTPServer
import cgitb
import httplib
import demote
import re
import shutil
import socket

#modules distributed with hltd
import prctl

#modules which are part of hltd
from daemon2 import Daemon2
from hltdconf import *
from inotifywrapper import InotifyWrapper
import _inotify as inotify

from elasticbu import BoxInfoUpdater
from elasticbu import RunCompletedChecker

idles = conf.resource_base+'/idle/'
used = conf.resource_base+'/online/'
broken = conf.resource_base+'/except/'
quarantined = conf.resource_base+'/quarantined/'
nthreads = None
nstreams = None
expected_processes = None
run_list=[]
bu_disk_list_ramdisk=[]
bu_disk_list_output=[]
active_runs=[]
resource_lock = threading.Lock()
suspended=False

dqm_globalrun_filepattern = '.run{0}.global'

logging.basicConfig(filename=os.path.join(conf.log_dir,"hltd.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

conf.dump()

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

def cleanup_resources():

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


def cleanup_mountpoints(remount=True):
    bu_disk_list_ramdisk[:] = []
    bu_disk_list_output[:] = []
    if conf.bu_base_dir[0] == '/':
        bu_disk_list_ramdisk[:] = [os.path.join(conf.bu_base_dir,conf.ramdisk_subdirectory)]
        bu_disk_list_output[:] = [os.path.join(conf.bu_base_dir,conf.output_subdirectory)]
        #make subdirectories if necessary and return
        if remount==True:
            try:
                os.makedirs(conf.bu_base_dir)
            except OSError:
                pass
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
        mounts = re.findall('/'+conf.bu_base_dir+'[0-9]+',out)
        mounts = list(set(mounts))
        #if len(mounts)>1 and mounts[0]==mounts[1]: mounts=[mounts[0]]
        logging.info("cleanup_mountpoints: found following mount points ")
        logging.info(mounts)
        umount_failure=False
        for point in mounts:
            logging.info("trying umount of "+point)
            try:
                subprocess.check_call(['umount','/'+point])
            except subprocess.CalledProcessError, err1:
                pass
            except Exception as ex:
                logging.exception(ex)
            try:
                subprocess.check_call(['umount',os.path.join('/'+point,conf.ramdisk_subdirectory)])
            except subprocess.CalledProcessError, err1:
                logging.error("Error calling umount in cleanup_mountpoints")
                logging.error(str(err1.returncode))
                umount_failure=True
            try:
                subprocess.check_call(['umount',os.path.join('/'+point,conf.output_subdirectory)])
            except subprocess.CalledProcessError, err1:
                logging.error("Error calling umount in cleanup_mountpoints")
                logging.error(str(err1.returncode))
                umount_failure=True
            #this will remove directories only if they are empty (as unomunted mount point should be)
            try:
                if os.path.join('/'+point,conf.ramdisk_subdirectory)!='/':
	            os.rmdir(os.path.join('/'+point,conf.ramdisk_subdirectory))
            except Exception as ex:
                logging.exception(ex)
            try:
                if os.path.join('/'+point,conf.output_subdirectory)!='/':
                    os.rmdir(os.path.join('/'+point,conf.output_subdirectory))
            except Exception as ex:
                logging.exception(ex)
            try:
                if os.path.join('/',point)!='/':
                    os.rmdir('/'+point)
            except Exception as ex:
                logging.exception(ex)
        if remount==False:
            if umount_failure:return False
            return True
        i = 0
        bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
        if os.path.exists(bus_config):
            for line in open(bus_config):
                logging.info("found BU to mount at "+line.strip())
                try:
                    os.makedirs('/'+conf.bu_base_dir+str(i))
                except OSError:
                    pass
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
                        logging.warn('unable to ping '+line.strip())
                        dt = p_end - p_begin
                        if dt.seconds < 10:
                            time.sleep(10-dt.seconds)
                    attemptsLeft-=1
                if attemptsLeft==0:
                    logging.fatal('hltd was unable to ping BU '+line.strip())
                    sys.exit(1)
                else:
                    logging.info("trying to mount "+line.strip()+':/fff/'+conf.ramdisk_subdirectory+' '+os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory))
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
                        bu_disk_list_ramdisk.append(os.path.join('/'+conf.bu_base_dir+str(i),conf.ramdisk_subdirectory))
                    except subprocess.CalledProcessError, err2:
                        logging.exception(err2)
                        logging.fatal("Unable to mount ramdisk - exiting.")
                        sys.exit(1)

                    logging.info("trying to mount "+line.strip()+':/fff/'+conf.output_subdirectory+' '+os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory))
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
                        bu_disk_list_output.append(os.path.join('/'+conf.bu_base_dir+str(i),conf.output_subdirectory))
                    except subprocess.CalledProcessError, err2:
                        logging.exception(err2)
                        logging.fatal("Unable to mount output - exiting.")
                        sys.exit(1)


                i+=1
        #clean up suspended state
        try:
            if remount==True:os.unlink(conf.watch_directory+'/suspend')
        except:pass
    except Exception as ex:
        logging.error("Exception in cleanup_mountpoints")
        logging.exception(ex)
        if remount==True:
            logging.fatal("Unable to handle (un)mounting")
            return False
        else:return False

def calculate_threadnumber():
    global nthreads
    global nstreams
    global expected_processes
    idlecount = len(os.listdir(idles))
    if conf.cmssw_threads_autosplit>0:
        nthreads = idlecount/conf.cmssw_threads_autosplit
        nstreams = idlecount/conf.cmssw_threads_autosplit
        if nthreads*conf.cmssw_threads_autosplit != nthreads:
            logging.error("idle cores can not be evenly split to cmssw threads")
    else:
        nthreads = conf.cmssw_threads
        nstreams = conf.cmssw_threads
    expected_processes = idlecount/nstreams

class system_monitor(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.rehash()
        self.threadEvent = threading.Event()

    def rehash(self):
        if conf.role == 'fu':
            self.directory = ['/'+x+'/appliance/boxes/' for x in bu_disk_list_ramdisk]
        else:
            self.directory = [conf.watch_directory+'/appliance/boxes/']
        self.file = [x+self.hostname for x in self.directory]
        for dir in self.directory:
            try:
                os.makedirs(dir)
            except OSError:
                pass
        logging.info("system_monitor: rehash found the following BU disks")
        for disk in self.file:
            logging.info(disk)

    def run(self):
        try:
            logging.debug('entered system monitor thread ')
            global suspended
            while self.running:
#                logging.info('system monitor - running '+str(self.running))
                self.threadEvent.wait(5)
                if suspended:continue
                tstring = datetime.datetime.utcfromtimestamp(time.time()).isoformat()

                fp = None
                for mfile in self.file:
                    if conf.role == 'fu':
                        dirstat = os.statvfs(conf.watch_directory)
                        fp=open(mfile,'w+')
                        fp.write('fm_date='+tstring+'\n')
                        fp.write('idles='+str(len(os.listdir(idles)))+'\n')
                        fp.write('used='+str(len(os.listdir(used)))+'\n')
                        fp.write('broken='+str(len(os.listdir(broken)))+'\n')
                        fp.write('quarantined='+str(len(os.listdir(quarantined)))+'\n')
                        fp.write('usedDataDir='+str(((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)>>20)+'\n')
                        fp.write('totalDataDir='+str((dirstat.f_blocks*dirstat.f_bsize)>>20)+'\n')
                        #two lines with active runs (used to check file consistency)
                        fp.write('activeRuns='+str(active_runs).strip('[]')+'\n')
                        fp.write('activeRuns='+str(active_runs).strip('[]')+'\n')
                        fp.write('entriesComplete=True')
                        fp.close()
                    if conf.role == 'bu':
                        ramdisk = os.statvfs(conf.watch_directory)
                        outdir = os.statvfs('/fff/output')
                        fp=open(mfile,'w+')

                        fp.write('fm_date='+tstring+'\n')
                        fp.write('idles=0\n')
                        fp.write('used=0\n')
                        fp.write('broken=0\n')
                        fp.write('quarantined=0\n')
                        fp.write('usedRamdisk='+str(((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize)>>20)+'\n')
                        fp.write('totalRamdisk='+str((ramdisk.f_blocks*ramdisk.f_bsize)>>20)+'\n')
                        fp.write('usedOutput='+str(((outdir.f_blocks - outdir.f_bavail)*outdir.f_bsize)>>20)+'\n')
                        fp.write('totalOutput='+str((outdir.f_blocks*outdir.f_bsize)>>20)+'\n')
                        fp.write('activeRuns='+str(active_runs).strip('[]')+'\n')
                        fp.write('activeRuns='+str(active_runs).strip('[]')+'\n')
                        fp.write('entriesComplete=True')
                        fp.close()

                if conf.role == 'bu':
                    mfile = conf.resource_base+'/disk.jsn'
                    stat=[]
                    if not os.path.exists(mfile):
                        fp=open(mfile,'w+')
                    else:
                        fp=open(mfile,'r+')
                        stat = json.load(fp)
                    if len(stat)>100:
                        stat[:] = stat[1:]
                    res=os.statvfs(mfile)

                    stat.append([int(time.time()*1000),float(res.f_bfree)/float(res.f_blocks)])
                    fp.seek(0)
                    fp.truncate()
                    json.dump(stat,fp)
                    fp.close()
        except Exception as ex:
            logging.error(ex)

        for mfile in self.file:
            try:
                os.remove(mfile)
            except OSError:
                pass

        logging.debug('exiting system monitor thread ')

    def stop(self):
        logging.debug("system_monitor: request to stop")
        self.running = False
        self.threadEvent.set()

class BUEmu:
    def __init__(self):
        self.process=None
        self.runnumber = None

    def startNewRun(self,nr):
        if self.runnumber:
            logging.error("Another BU emulator run "+str(self.runnumber)+" is already ongoing")
            return
        self.runnumber = nr
        configtouse = conf.test_bu_config
        destination_base = None
        if role == 'fu':
            destination_base = bu_disk_list_ramdisk[startindex%len(bu_disk_list_ramdisk)]
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
                        configtouse,
                        str(nr),
                        '/tmp', #input dir is not needed
                        destination_base,
                        '1',
                        '1',
                        full_release]
        try:
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
        except Exception as ex:
            logging.error("Error in forking BU emulator process")
            logging.error(ex)

    def stop(self):
        os.kill(self.process.pid,SIGINT)
        self.process.wait()
        self.runnumber=None

bu_emulator=BUEmu()

class OnlineResource:

    def __init__(self,resourcenames,lock):
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
            if not os.system("ping -c 1 "+self.cpu[0])==0: self.hoststate = 0

    def NotifyNewRun(self,runnumber):
        self.runnumber = runnumber
        logging.info("calling start of run on "+self.cpu[0]);
        try:
            connection = httplib.HTTPConnection(self.cpu[0], conf.cgi_port)
            connection.request("GET",'cgi-bin/start_cgi.py?run='+str(runnumber))
            response = connection.getresponse()
            #do something intelligent with the response code
            logging.error("response was "+str(response.status))
            if response.status > 300: self.hoststate = 1
            else:
                logging.info(response.read())
        except Exception as ex:
            logging.exception(ex)

    def NotifyShutdown(self):
        try:
            connection = httplib.HTTPConnection(self.cpu[0], conf.cgi_port)
            connection.request("GET",'cgi-bin/stop_cgi.py?run='+str(self.runnumber))
            time.sleep(0.05)
            response = connection.getresponse()
            time.sleep(0.05)
            #do something intelligent with the response code
            if response.status > 300: self.hoststate = 0
        except Exception as ex:
            logging.exception(ex)

    def StartNewProcess(self ,runnumber, startindex, arch, version, menu,num_threads,num_streams):
        logging.debug("OnlineResource: StartNewProcess called")
        self.runnumber = runnumber

        """
        this is just a trick to be able to use two
        independent mounts of the BU - it should not be necessary in due course
        IFF it is necessary, it should address "any" number of mounts, not just 2
        """
        input_disk = bu_disk_list_ramdisk[startindex%len(bu_disk_list_ramdisk)]
        #run_dir = input_disk + '/run' + str(self.runnumber).zfill(conf.run_number_padding)

        logging.info("starting process with "+version+" and run number "+str(runnumber))

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
                            menu,
                            str(runnumber),
                            input_disk,
                            conf.watch_directory,
                            str(num_threads),
                            str(num_streams),
                            full_release]
        else: # a dqm machine
            dqm_globalrun_file = input_disk + '/' + dqm_globalrun_filepattern.format(runnumber)
            try:
                with open(dqm_globalrun_file, 'r') as f:
                    for line in f:
                        run_type = re.search(r'run[_]*type\s*=\s*(\bcollision\b|\bcosmic\b|\bcommissioning\b)_run', line, re.I).group(1).lower()
                        if run_type: break
            except IOError,ex:
                logging.exception(ex)
                logging.info("the default run type will be used for the dqm jobs")
            new_run_args = [conf.cmssw_script_location+'/startDqmRun.sh',
                            conf.cmssw_base,
                            arch,
                            conf.exec_directory,
                            str(runnumber),
                            input_disk,
                            used+self.cpu[0]]
            if self.watchdog:
                new_run_args.append('skipFirstLumis=True')
            if run_type:
                new_run_args.append('runtype={0}.format(run_type)')

        logging.info("arg array "+str(new_run_args).translate(None, "'"))
        try:
#            dem = demote.demote(conf.user)
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
            self.processstate = 100
            logging.info("started process "+str(self.process.pid))
#            time.sleep(1.)
            if self.watchdog==None:
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logging.debug("watchdog thread for "+str(self.process.pid)+" is alive "
                             + str(self.watchdog.is_alive()))
            else:
                self.watchdog.join()
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logging.debug("watchdog thread restarted for "+str(self.process.pid)+" is alive "
                              + str(self.watchdog.is_alive()))
        except Exception as ex:
            logging.info("OnlineResource: exception encountered in forking hlt slave")
            logging.info(ex)

    def join(self):
        logging.debug('calling join on thread ' +self.watchdog.name)
        self.watchdog.join()

    def disableRestart(self):
        logging.debug("OnlineResource "+str(self.cpu)+" restart is now disabled")
        if self.watchdog:
            self.watchdog.disableRestart()

    def clearQuarantined(self):
        resource_lock.acquire()
        try:
            for cpu in self.quarantined:
                logging.info('Clearing quarantined resource '+cpu)
                os.rename(quarantined+cpu,idles+cpu)
            self.quarantined = []
        except Exception as ex:
            logging.exception(ex)
        resource_lock.release()

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
            logging.info('watchdog for process '+str(self.resource.process.pid))
            self.resource.process.wait()
            returncode = self.resource.process.returncode
            pid = self.resource.process.pid

            #update json process monitoring file
            self.resource.processstate=returncode
            logging.debug('ProcessWatchdog: acquire lock thread '+str(pid))
            self.lock.acquire()
            logging.debug('ProcessWatchdog: acquired lock thread '+str(pid))

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
                logging.exception(ex)
            except ValueError:
                pass

            logging.debug('ProcessWatchdog: release lock thread '+str(pid))
            self.lock.release()
            logging.debug('ProcessWatchdog: released lock thread '+str(pid))


            abortedmarker = self.resource.statefiledir+'/'+Run.ABORTED
            if os.path.exists(abortedmarker):
                resource_lock.acquire()
                #release resources
                try:
                    for cpu in self.resource.cpu:
                        try:
                            os.rename(used+cpu,idles+cpu)
                        except Exception as ex:
                            logging.exception(ex)
                except:pass
                resource_lock.release()
                return

            #quit codes (configuration errors):
            quit_codes = [127,90,65,73]

            #cleanup actions- remove process from list and
            # attempt restart on same resource
            #dqm mode will treat configuration error as a crash and eventually move to quarantined
            if returncode != 0 and ( returncode not in quit_codes or conf.dqm_machine==True):
                if returncode < 0:
                    logging.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with signal "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )
                else:
                    logging.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with code "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )



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
                except: logging.exception("unable to create %r" %filename)
                logging.info("pid crash file: %r" %filename)


                if self.resource.retry_attempts < self.retry_limit:
                    """
                    sleep a configurable amount of seconds before
                    trying a restart. This is to avoid 'crash storms'
                    """
                    time.sleep(self.retry_delay)

                    self.resource.process = None
                    self.resource.retry_attempts += 1

                    logging.info("try to restart process for resource(s) "
                                 +str(self.resource.cpu)
                                 +" attempt "
                                 + str(self.resource.retry_attempts))
                    resource_lock.acquire()
                    for cpu in self.resource.cpu:
                      os.rename(used+cpu,broken+cpu)
                    resource_lock.release()
                    logging.debug("resource(s) " +str(self.resource.cpu)+
                                  " successfully moved to except")
                elif self.resource.retry_attempts >= self.retry_limit:
                    logging.error("process for run "
                                  +str(self.resource.runnumber)
                                  +" on resources " + str(self.resource.cpu)
                                  +" reached max retry limit "
                                  )
                    resource_lock.acquire()
                    for cpu in self.resource.cpu:
                        os.rename(used+cpu,quarantined+cpu)
                        self.resource.quarantined.append(cpu)
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
                        logging.exception(ex)

            #successful end= release resource (TODO:maybe should mark aborted for non-0 error codes)
            elif returncode == 0 or returncode in quit_codes:
                if returncode==0:
                    logging.info('releasing resource, exit 0 meaning end of run '+str(self.resource.cpu))
                elif returncode==127:
                    logging.fatal('error executing start script. Maybe CMSSW environment is not available (cmsRun executable not in path).')
                elif returncode==90:
                    logging.fatal('error executing start script: python error.')
                elif returncode in quit_codes:
                    logging.fatal('error executing start script: CMSSW configuration error.')
                else:
                    logging.fatal('error executing start script: unspecified error.')

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
                  os.rename(used+cpu,idles+cpu)
                resource_lock.release()

                #self.resource.process=None

            #        logging.info('exiting thread '+str(self.resource.process.pid))

        except Exception as ex:
            resource_lock.release()
            logging.info("OnlineResource watchdog: exception")
            logging.exception(ex)
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

    def __init__(self,nr,dirname,bu_dir):
        self.runnumber = nr
        self.dirname = dirname
        self.online_resource_list = []
        self.is_active_run = False
        self.anelastic_monitor = None
        self.elastic_monitor = None
        self.elastic_test = None
        self.endChecker = None

        self.arch = None
        self.version = None
        self.menu = None
        self.waitForEndThread = None
        self.beginTime = datetime.datetime.now()
        self.anelasticWatchdog = None
        self.threadEvent = threading.Event()
        global active_runs

        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STARTING)
            if int(self.runnumber) in active_runs:
                raise Exception("Run "+str(self.runnumber)+ "already active")
            active_runs.append(int(self.runnumber))
        else:
            #currently unused on BU
            active_runs.append(int(self.runnumber))

        self.menu_directory = bu_dir+'/'+conf.menu_directory

        readMenuAttempts=0
        #polling for HLT menu directory
        while os.path.exists(self.menu_directory)==False and conf.dqm_machine==False and conf.role=='fu':
            time.sleep(.2)
            readMenuAttempts+=1
            #10 seconds allowed before defaulting to local configuration
            if readMenuAttempts>50: break

        readMenuAttempts=0
        #try to read HLT parameters
        if os.path.exists(self.menu_directory):
            while True:
                self.menu = self.menu_directory+'/'+conf.menu_name
                if os.path.exists(self.menu_directory+'/'+conf.arch_file):
                    fp = open(self.menu_directory+'/'+conf.arch_file,'r')
                    self.arch = fp.readline().strip()
                    fp.close()
                if os.path.exists(self.menu_directory+'/'+conf.version_file):
                    fp = open(self.menu_directory+'/'+conf.version_file,'r')
                    self.version = fp.readline().strip()
                    fp.close()
                try:
                    logging.info("Run "+str(self.runnumber)+" uses "+ self.version+" ("+self.arch+") with "+self.menu)
                    break
                except Exception as ex:
                    logging.exception(ex)
                    logging.error("Run parameters obtained for run "+str(self.runnumber)+": "+ str(self.version)+" ("+str(self.arch)+") with "+str(self.menu))
                    time.sleep(.5)
                    readMenuAttempts+=1
                    if readMenuAttempts==3: raise Exception("Unable to parse HLT parameters")
                    continue
        else:
            self.arch = conf.cmssw_arch
            self.version = conf.cmssw_default_version
            self.menu = conf.test_hlt_config1
            if conf.role=='fu':
                logging.warn("Using default values for run "+str(self.runnumber)+": "+self.version+" ("+self.arch+") with "+self.menu)

        self.rawinputdir = None
        if conf.role == "bu":
            try:
                self.rawinputdir = conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
                self.buoutputdir = conf.micromerge_output+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
                os.mkdir(self.rawinputdir+'/mon')
            except Exception, ex:
                logging.error("could not create mon dir inside the run input directory")
        else:
            self.rawinputdir= bu_disk_list_ramdisk[0]+'/run' + str(self.runnumber).zfill(conf.run_number_padding)

        self.lock = threading.Lock()
        #conf.use_elasticsearch = False
            #note: start elastic.py first!
        if conf.use_elasticsearch == True:
            try:
                if conf.role == "bu":
                    logging.info("starting elasticbu.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elasticbu.py',str(self.runnumber)]
                else:
                    logging.info("starting elastic.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elastic.py',self.dirname,self.rawinputdir+'/mon',str(expected_processes),str(conf.elastic_cluster)]

                self.elastic_monitor = subprocess.Popen(elastic_args,
                                                        preexec_fn=preexec_function,
                                                        close_fds=True
                                                        )

            except OSError as ex:
                logging.error("failed to start elasticsearch client")
                logging.error(ex)
        if conf.role == "fu" and conf.dqm_machine==False:
            try:
                logging.info("starting anelastic.py with arguments:"+self.dirname)
                elastic_args = ['/opt/hltd/python/anelastic.py',self.dirname,str(self.runnumber), self.rawinputdir]
                self.anelastic_monitor = subprocess.Popen(elastic_args,
                                                    preexec_fn=preexec_function,
                                                    close_fds=True
                                                    )
            except OSError as ex:
                logging.fatal("failed to start anelastic.py client:")
                logging.exception(ex)
                sys.exit(1)


    def AcquireResource(self,resourcenames,fromstate):
        idles = conf.resource_base+'/'+fromstate+'/'
        try:
            logging.debug("Trying to acquire resource "
                          +str(resourcenames)
                          +" from "+fromstate)

            for resourcename in resourcenames:
              os.rename(idles+resourcename,used+resourcename)
            if not filter(lambda x: x.cpu==resourcenames,self.online_resource_list):
                logging.debug("resource(s) "+str(resourcenames)
                              +" not found in online_resource_list, creating new")
                self.online_resource_list.append(OnlineResource(resourcenames,self.lock))
                return self.online_resource_list[-1]
            logging.debug("resource(s) "+str(resourcenames)
                          +" found in online_resource_list")
            return filter(lambda x: x.cpu==resourcenames,self.online_resource_list)[0]
        except Exception as ex:
            logging.info("exception encountered in looking for resources")
            logging.info(ex)

    def ContactResource(self,resourcename):
        self.online_resource_list.append(OnlineResource(resourcename,self.lock))
        self.online_resource_list[-1].ping() #@@MO this is not doing anything useful, afaikt

    def ReleaseResource(self,res):
        self.online_resource_list.remove(res)

    def AcquireResources(self,mode):
        logging.info("acquiring resources from "+conf.resource_base)
        idles = conf.resource_base
        idles += '/idle/' if conf.role == 'fu' else '/boxes/'
        try:
            dirlist = os.listdir(idles)
        except Exception as ex:
            logging.info("exception encountered in looking for resources")
            logging.info(ex)
        logging.info(dirlist)
        current_time = time.time()
        count = 0
        cpu_group=[]
        #self.lock.acquire()

        for cpu in dirlist:
            #skip self
            if conf.role=='bu' and cpu == os.uname()[1]:continue

            count = count+1
            cpu_group.append(cpu)
            age = current_time - os.path.getmtime(idles+cpu)
            logging.info("found resource "+cpu+" which is "+str(age)+" seconds old")
            if conf.role == 'fu':
                if count == nstreams:
                  self.AcquireResource(cpu_group,'idle')
                  cpu_group=[]
                  count=0
            else:
                if age < 10:
                    cpus = [cpu]
                    self.ContactResource(cpus)
        #self.lock.release()

    def Start(self):
        self.is_active_run = True
        for resource in self.online_resource_list:
            logging.info('start run '+str(self.runnumber)+' on cpu(s) '+str(resource.cpu))
            if conf.role == 'fu':
                self.StartOnResource(resource)
            else:
                resource.NotifyNewRun(self.runnumber)
                #update begin time to after notifying FUs
                self.beginTime = datetime.datetime.now()
        if conf.role == 'fu' and conf.dqm_machine==False:
            self.changeMarkerMaybe(Run.ACTIVE)
            #start safeguard monitoring of anelastic.py
            self.startAnelasticWatchdog()
        else:
            self.startCompletedChecker()

    def StartOnResource(self, resource):
        logging.debug("StartOnResource called")
        resource.statefiledir=conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
        mondir = os.path.join(resource.statefiledir,'mon')
        resource.associateddir=mondir
        logging.info(str(nthreads)+' '+str(nstreams))
        resource.StartNewProcess(self.runnumber,
                                 self.online_resource_list.index(resource),
                                 self.arch,
                                 self.version,
                                 self.menu,
                                 int(round((len(resource.cpu)*float(nthreads)/nstreams))),
                                 len(resource.cpu))
        logging.debug("StartOnResource process started")
        #logging.debug("StartOnResource going to acquire lock")
        #self.lock.acquire()
        #logging.debug("StartOnResource lock acquired")
        try:
            os.makedirs(mondir)
        except OSError:
            pass
        monfile = mondir+'/hltd.jsn'

        fp=None
        stat = []
        if not os.path.exists(monfile):
            logging.debug("No log file "+monfile+" found, creating one")
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
                        logging.error("could not retrieve process parameters")
                        logging.exception(ex)
                        break

        else:
            logging.debug("Updating existing log file "+monfile)
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
                        logging.error("could not retrieve process parameters")
                        logging.exception(ex)
                        break
        fp.seek(0)
        fp.truncate()
        json.dump(stat,fp)

        fp.flush()
        fp.close()
        #self.lock.release()
        #logging.debug("StartOnResource lock released")

    def Shutdown(self,herod=False):
        #herod mode sends sigkill to all process, however waits for all scripts to finish
        logging.debug("Run:Shutdown called")
        self.is_active_run = False
        try:
            self.changeMarkerMaybe(Run.ABORTED)
        except OSError as ex:
            pass

        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            for resource in self.online_resource_list:
                if conf.role == 'fu':
                    if resource.processstate==100:
                        logging.info('terminating process '+str(resource.process.pid)+
                                     ' in state '+str(resource.processstate))

                        if herod:resource.process.kill()
                        else:resource.process.terminate()
                        logging.info('process '+str(resource.process.pid)+' join watchdog thread')
                        #                    time.sleep(.1)
                        resource.join()
                        logging.info('process '+str(resource.process.pid)+' terminated')
                    logging.info('releasing resource(s) '+str(resource.cpu))
                    resource.clearQuarantined()
                    
                    resource_lock.acquire()
                    for cpu in resource.cpu:
                        try:
                            os.rename(used+cpu,idles+cpu)
                        except OSError:
                            #@SM:happens if t was quarantined
                            logging.warning('Unable to find resource file '+used+cpu+'.')
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
                    if herod:
                        self.anelastic_monitor.wait()
                    else:
                        self.anelastic_monitor.terminate()
                        self.anelastic_monitor.wait()
            except Exception as ex:
                logging.info("exception encountered in shutting down anelastic.py "+ str(ex))
                #logging.exception(ex)
            if conf.use_elasticsearch == True:
                try:
                    if self.elastic_monitor:
                        if herod:
                            self.elastic_monitor.wait()
                        else:
                            self.elastic_monitor.terminate()
                            self.elastic_monitor.wait()
                except Exception as ex:
                    logging.info("exception encountered in shutting down elastic.py")
                    logging.exception(ex)
            if self.waitForEndThread is not None:
                self.waitForEndThread.join()
        except Exception as ex:
            logging.info("exception encountered in shutting down resources")
            logging.exception(ex)

        global active_runs
        active_runs_copy = active_runs[:]
        for run_num in active_runs_copy:
            if run_num == self.runnumber:
                active_runs.remove(run_num)

        try:
            if conf.delete_run_dir is not None and conf.delete_run_dir == True:
                shutil.rmtree(conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding))
            os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
        except:
            pass

        logging.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' completed')

    def ShutdownBU(self):

        self.is_active_run = False
        if conf.role == 'bu':
            for resource in self.online_resource_list:
                if self.endChecker:
                    try:
                        self.endChecker.stop()
                        seld.endChecker.join()
                    except Exception,ex:
                        pass

        if conf.use_elasticsearch == True:
            try:
                if self.elastic_monitor:
                    self.elastic_monitor.terminate()
                    time.sleep(.1)
                    self.elastic_monitor.wait()
            except Exception as ex:
	        logging.info("exception encountered in shutting down elasticbu.py: " + str(ex))
                #logging.exception(ex)

        global active_runs
        active_runs_copy = active_runs[:]
        for run_num in active_runs_copy:
            if run_num == self.runnumber:
                active_runs.remove(run_num)

        logging.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' on BU completed')


    def StartWaitForEnd(self):
        self.is_active_run = False
        self.changeMarkerMaybe(Run.STOPPING)
        try:
            self.waitForEndThread = threading.Thread(target = self.WaitForEnd)
            self.waitForEndThread.start()
        except Exception as ex:
            logging.info("exception encountered in starting run end thread")
            logging.info(ex)

    def WaitForEnd(self):
        logging.info("wait for end thread!")
        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            for resource in self.online_resource_list:
                if resource.processstate is not None:#was:100
                    if resource.process is not None and resource.process.pid is not None: ppid = resource.process.pid
                    else: ppid="None"
                    logging.info('waiting for process '+str(ppid)+
                                 ' in state '+str(resource.processstate) +
                                 ' to complete ')
                    try:
                        resource.join()
                        logging.info('process '+str(resource.process.pid)+' completed')
                    except:pass
#                os.rename(used+resource.cpu,idles+resource.cpu)
                resource.clearQuarantined()
                resource.process=None
            self.online_resource_list = []
            if conf.role == 'fu':
                logging.info('writing complete file')
                self.changeMarkerMaybe(Run.COMPLETE)
                try:
                    os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
                except:pass
                try:
                    if conf.dqm_machine==False:
                        self.anelastic_monitor.wait()
                except OSError,ex:
                    logging.info("Exception encountered in waiting for termination of anelastic:" +str(ex))

            if conf.use_elasticsearch == True:
                try:
                    self.elastic_monitor.wait()
                except OSError,ex:
                    logging.info("Exception encountered in waiting for termination of anelastic:" +str(ex))
            if conf.delete_run_dir is not None and conf.delete_run_dir == True:
                try:
                    shutil.rmtree(self.dirname)
                except Exception as ex:
                    logging.exception(ex)

            global active_runs
            logging.info("active runs.."+str(active_runs))
            for run_num  in active_runs:
                if run_num == self.runnumber:
                    active_runs.remove(run_num)
            logging.info("new active runs.."+str(active_runs))

        except Exception as ex:
            logging.error("exception encountered in ending run")
            logging.exception(ex)

    def changeMarkerMaybe(self,marker):
        dir = self.dirname
        current = filter(lambda x: x in Run.VALID_MARKERS, os.listdir(dir))
        if (len(current)==1 and current[0] != marker) or len(current)==0:
            if len(current)==1: os.remove(dir+'/'+current[0])
            fp = open(dir+'/'+marker,'w+')
            fp.close()
        else:
            logging.error("There are more than one markers for run "
                          +str(self.runnumber))
            return

    def startAnelasticWatchdog(self):
        try:
            self.anelasticWatchdog = threading.Thread(target = self.runAnelasticWatchdog)
            self.anelasticWatchdog.start()
        except Exception as ex:
            logging.info("exception encountered in starting anelastic watchdog thread")
            logging.info(ex)

    def runAnelasticWatchdog(self):
        try:
            self.anelastic_monitor.wait()
            if self.is_active_run == True:
                #abort the run
                self.anelasticWatchdog=None
                logging.fatal("Premature end of anelastic.py")
                self.Shutdown()
        except:
            pass

    def stopAnelasticWatchdog(self):
        self.threadEvent.set()
        if self.anelasticWatchdog:
            self.anelasticWatchdog.join()

    def startCompletedChecker(self):
        if conf.role == 'bu': #and conf.use_elasticsearch == True:
            try:
                logging.info('start checking completition of run '+str(self.runnumber))
                #mode 1: check for complete entries in ES
                #mode 2: check for runs in 'boxes' files
                self.endChecker = RunCompletedChecker(1,int(self.runnumber),self.online_resource_list,self.dirname, active_runs,self.elastic_monitor)
                self.endChecker.start()
            except Exception,ex:
                logging.error('failure to start run completition checker:')
                logging.exception(ex)

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
       


class RunRanger:

    def __init__(self):
        self.inotifyWrapper = InotifyWrapper(self)

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self):
        logging.info("RunRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        logging.info("RunRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        logging.info("RunRanger: Inotify wrapper returned")

    def process_IN_CREATE(self, event):
        nr=0
        global run_list
        logging.info('RunRanger: event '+event.fullpath)
        dirname=event.fullpath[event.fullpath.rfind("/")+1:]
        logging.info('RunRanger: new filename '+dirname)
        if dirname.startswith('run'):
            nr=int(dirname[3:])
            if nr!=0:
                is_global_run = os.path.exists(event.fullpath[:event.fullpath.rfind("/")+1] + dqm_globalrun_filepattern.format(nr))
                # the dqm machines ignore a run if it's NOT a global run or porcess it if the dqm global run file in not mandatory
                if (not conf.dqm_machine) or (is_global_run or (not conf.dqm_globalrunfile_mandatory)):
                    try:
                        logging.info('new run '+str(nr))
                        if conf.role == 'fu':
                            bu_dir = bu_disk_list_ramdisk[0]+'/'+dirname
                            try:
                                os.symlink(bu_dir+'/jsd',event.fullpath+'/jsd')
                            except:
                                if not dqm_machine:
                                    self.logger.warning('jsd directory symlink error, continuing without creating link')
                                pass
                        else:
                            bu_dir = ''

                        run_list.append(Run(nr,event.fullpath,bu_dir))
                        resource_lock.acquire()
                        run_list[-1].AcquireResources(mode='greedy')
                        run_list[-1].Start()
                        resource_lock.release()
                    except OSError as ex:
                        logging.error("RunRanger: "+str(ex)+" "+ex.filename)
                        logging.exception(ex)
                    except Exception as ex:
                        logging.error("RunRanger: unexpected exception encountered in forking hlt slave")
                        logging.exception(ex)

        elif dirname.startswith('emu'):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    """
                    start a new BU emulator run here - this will trigger the start of the HLT run
                    """
                    bu_emulator.startNewRun(nr)

                except Exception as ex:
                    logging.info("exception encountered in starting BU emulator run")
                    logging.info(ex)

                os.remove(event.fullpath)

        elif dirname.startswith('end'):
            # need to check is stripped name is actually an integer to serve
            # as run number
            if dirname[3:].isdigit():
                nr=int(dirname[3:])
                if nr!=0:
                    try:
                        runtoend = filter(lambda x: x.runnumber==nr,run_list)
                        if len(runtoend)==1:
                            logging.info('end run '+str(nr))
                            #remove from run_list to prevent intermittent restarts
                            #lock used to fix a race condition when core files are being moved around
                            resource_lock.acquire()
                            run_list.remove(runtoend[0])
                            time.sleep(.1)
                            resource_lock.release()
                            if conf.role == 'fu':
                                runtoend[0].StartWaitForEnd()
                            if bu_emulator and bu_emulator.runnumber != None:
                                bu_emulator.stop()
                            #logging.info('run '+str(nr)+' removing end-of-run marker')
                            #os.remove(event.fullpath)
                        elif len(runtoend)==0:
                            logging.warning('request to end run '+str(nr)
                                          +' which does not exist')
                            os.remove(event.fullpath)
                        else:
                            logging.error('request to end run '+str(nr)
                                          +' has more than one run object - this should '
                                          +'*never* happen')

                    except Exception as ex:
                        resource_lock.release()
                        logging.info("exception encountered when waiting hltrun to end")
                        logging.info(ex)
                else:
                    logging.error('request to end run '+str(nr)
                                  +' which is an invalid run number - this should '
                                  +'*never* happen')
            else:
                logging.error('request to end run '+str(nr)
                              +' which is NOT a run number - this should '
                              +'*never* happen')

        elif dirname.startswith('herod'):
            os.remove(event.fullpath)
            if conf.role == 'fu':
                logging.info("killing all CMSSW child processes")
                for run in run_list:
                    run.Shutdown(True)
            elif conf.role == 'bu':
                for run in run_list:
                    run.ShutdownBU()
                boxdir = conf.resource_base +'/boxes/'
                try:
                    dirlist = os.listdir(boxdir)
                    current_time = time.time()
                    logging.info("sending herod to child FUs")
                    for name in dirlist:
                        if name == os.uname()[1]:continue
                        age = current_time - os.path.getmtime(boxdir+name)
                        logging.info('found box '+name+' with keepalive age '+str(age))
                        if age < 20:
                            connection = httplib.HTTPConnection(name, conf.cgi_port)
                            connection.request("GET",'cgi-bin/herod_cgi.py')
                            response = connection.getresponse()
                    logging.info("sent herod to all child FUs")
                except Exception as ex:
                    logging.error("exception encountered in contacting resources")
                    logging.info(ex)
            run_list=[]
            active_runs=[]

        elif dirname.startswith('populationcontrol'):
            logging.info("terminating all ongoing runs")
            for run in run_list:
                if conf.role=='fu':
                    run.Shutdown()
                elif conf.role=='bu':
                    run.ShutdownBU()
            run_list = []
            active_runs=[]
            logging.info("terminated all ongoing runs via cgi interface (populationcontrol)")
            os.remove(event.fullpath)

        elif dirname.startswith('harakiri') and conf.role == 'fu':
            os.remove(event.fullpath)
            pid=os.getpid()
            logging.info('asked to commit seppuku:'+str(pid))
            try:
                logging.info('sending signal '+str(SIGKILL)+' to myself:'+str(pid))
                retval = os.kill(pid, SIGKILL)
                logging.info('sent SIGINT to myself:'+str(pid))
                logging.info('got return '+str(retval)+'waiting to die...and hope for the best')
            except Exception as ex:
                logging.error("exception in committing harakiri - the blade is not sharp enough...")
                logging.error(ex)

        elif dirname.startswith('quarantined'):
            try:
                os.remove(dirname)
            except:
                pass
            if dirname[11:].isdigit():
                nr=int(dirname[11:])
                if nr!=0:
                    try:
                        runtoend = filter(lambda x: x.runnumber==nr,run_list)
                        if len(runtoend)==1:
                            if runtoend[0].checkQuarantinedLimit()==True:
                                runtoend[0].Shutdown(True)#run abort in herod mode (wait for anelastic/elastic to shut down)
                    except Exception as ex:
                        logging.exception(ex)

        elif dirname.startswith('suspend') and conf.role == 'fu':
            logging.info('suspend mountpoints initiated')
            global suspended
            suspended=True
            for run in run_list:
                run.Shutdown(False)#terminate all ongoing runs
            run_list=[]
            time.sleep(.5)
            umount_success = cleanup_mountpoints(remount=False)

            if umount_success==False:
                time.sleep(1)
                logging.error("Suspend initiated from BU failed, trying again...")
                #notifying itself again
                try:os.remove(event.fullpath)
                except:pass
                fp = open(event.fullpath,"w+")
                fp.close()
                return 
                #logging.info("Suspend failed, preparing for harakiri...")
                #time.sleep(.1)
                #fp = open(os.path.join(os.path.dirname(event.fullpath.rstrip(os.path.sep)),'harakiri'),"w+")
                #fp.close()
                #return

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
                    logging.fatal("No BU name was found in the bus.config file. Leaving mount points unmounted until the hltd service restart.")
                    os.remove(event.fullpath)
                    return
                connection = httplib.HTTPConnection(bu_name, conf.cgi_port+5,timeout=5)
                connection.request("GET",'cgi-bin/report_suspend_cgi.py?host='+os.uname()[1])
                response = connection.getresponse()
            except Exception as ex:
                logging.error("Unable to report suspend state to BU "+str(bu_name)+':'+str(conf.cgi_port+5))
                logging.exception(ex)

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
                            logging.info('exception test 1')
                            time.sleep(5)
                            continue
                    if bu_name==None:
                        logging.info('exception test 2')
                        time.sleep(5)
                        continue

                    logging.info('checking if BU hltd is available...')
                    connection = httplib.HTTPConnection(bu_name, conf.cgi_port,timeout=5)
                    connection.request("GET",'cgi-bin/getcwd_cgi.py')
                    response = connection.getresponse()
                    logging.info('BU hltd is running !...')
                    #if we got here, the service is back up
                    break
                except Exception as ex:
                    try:
                       logging.info('Failed to contact BU hltd service: ' + str(ex.args[0]) +" "+ str(ex.args[1]))
                    except:
                       logging.info('Failed to contact BU hltd service: ')
                    time.sleep(5)

            #mount again
            cleanup_mountpoints()
            try:os.remove(event.fullpath)
            except:pass
            suspended=False
            logging.info("Remount is performed")
 
        logging.debug("RunRanger completed handling of event "+event.fullpath)

    def process_default(self, event):
        logging.info('RunRanger: event '+event.fullpath+' type '+str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

class ResourceRanger:

    def __init__(self):
        self.inotifyWrapper = InotifyWrapper(self)

        self.managed_monitor = system_monitor()
        self.managed_monitor.start()

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_managed_monitor(self):
        logging.info("ResourceRanger: Stop managed monitor")
        self.managed_monitor.stop()
        logging.info("ResourceRanger: Join managed monitor")
        self.managed_monitor.join()
        logging.info("ResourceRanger: managed monitor returned")

    def stop_inotify(self):
        logging.info("ResourceRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        logging.info("ResourceRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        logging.info("ResourceRanger: Inotify wrapper returned")

    def process_IN_MOVED_TO(self, event):
        logging.debug('ResourceRanger-MOVEDTO: event '+event.fullpath)
        try:
            resourcepath=event.fullpath[1:event.fullpath.rfind("/")]
            resourcestate=resourcepath[resourcepath.rfind("/")+1:]
            resourcename=event.fullpath[event.fullpath.rfind("/")+1:]
            resource_lock.acquire()
            if not (resourcestate == 'online' or resourcestate == 'offline'
                    or resourcestate == 'quarantined'):
                logging.debug('ResourceNotifier: new resource '
                              +resourcename
                              +' in '
                              +resourcepath
                              +' state '
                              +resourcestate
                              )
                ongoing_runs = filter(lambda x: x.is_active_run==True,run_list)
                if ongoing_runs:
                    ongoing_run = ongoing_runs[0]
                    logging.info("ResourceRanger: found active run "+str(ongoing_run.runnumber))
                    """grab resources that become available
                    #@@EM implement threaded acquisition of resources here
                    """
                    #find all idle cores
                    idlesdir = '/'+resourcepath
		    try:
                        reslist = os.listdir(idlesdir)
                    except Exception as ex:
                        logging.info("exception encountered in looking for resources")
                        logging.exception(ex)
                    #put inotify-ed resource as the first item
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
                        res = ongoing_run.AcquireResource(resourcenames,resourcestate)

                    if acquired_sufficient:
                        logging.info("ResourceRanger: acquired resource(s) "+str(res.cpu))
                        ongoing_run.StartOnResource(res)
                        logging.info("ResourceRanger: started process on resource "
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
                            logging.info("exception encountered in looking for resources in except")
                            logging.info(ex)

        except Exception as ex:
            logging.error("exception in ResourceRanger")
            logging.error(ex)
        try:
            resource_lock.release()
        except:pass

    def process_IN_MODIFY(self, event):

        logging.debug('ResourceRanger-MODIFY: event '+event.fullpath)
        try:
            bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
            if event.fullpath == bus_config:
                if self.managed_monitor:
                    self.managed_monitor.stop()
                    self.managed_monitor.join()
                cleanup_mountpoints()
                if self.managed_monitor:
                    self.managed_monitor = system_monitor()
                    self.managed_monitor.start()
                    logging.info("ResouceRanger: managed monitor is "+str(self.managed_monitor))
        except Exception as ex:
            logging.error("exception in ResourceRanger")
            logging.error(ex)

    def process_default(self, event):
        logging.debug('ResourceRanger: event '+event.fullpath +' type '+ str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]


class hltd(Daemon2,object):
    def __init__(self, pidfile):
        Daemon2.__init__(self,pidfile,'hltd')

    def stop(self):
        if self.silentStatus():
            try:
                if os.path.exists(conf.watch_directory+'/populationcontrol'):
                    os.remove(conf.watch_directory+'/populationcontrol')
                fp = open(conf.watch_directory+'/populationcontrol','w+')
                fp.close()
                count = 10
                while count:
                    os.stat(conf.watch_directory+'/populationcontrol')
                    sys.stdout.write('o.o')
                    sys.stdout.flush()
                    time.sleep(1.)
                    count-=1
            except OSError, err:
                pass
            except IOError, err:
                pass
        super(hltd,self).stop()

    def run(self):
        """
        if role is not defined in the configuration (which it shouldn't)
        infer it from the name of the machine
        """

        if conf.enabled==False:
            logging.warning("Service is currently disabled.")
            sys.exit(1)

        if conf.role == 'fu':

            """
            cleanup resources
            """

            cleanup_resources()
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


        """
        the line below is a VERY DIRTY trick to address the fact that
        BU resources are dynamic hence they should not be under /etc
        """
        conf.resource_base = conf.watch_directory+'/appliance' if conf.role == 'bu' else conf.resource_base

        #@SM:is running from symbolic links still needed?
        watch_directory = os.readlink(conf.watch_directory) if os.path.islink(conf.watch_directory) else conf.watch_directory
        resource_base = os.readlink(conf.resource_base) if os.path.islink(conf.resource_base) else conf.resource_base

        #start boxinfo elasticsearch updater
        boxInfo = None
        if conf.role == 'bu' and conf.use_elasticsearch == True:
            boxInfo = BoxInfoUpdater(watch_directory)
            boxInfo.start()

        logCollector = None
        if conf.use_elasticsearch == True:
            logging.info("starting logcollector.py")
            logcolleccor_args = ['/opt/hltd/python/logcollector.py',]
            logCollector = subprocess.Popen(['/opt/hltd/python/logcollector.py'],preexec_fn=preexec_function,close_fds=True)

        runRanger = RunRanger()
        runRanger.register_inotify_path(watch_directory,inotify.IN_CREATE)
        runRanger.start_inotify()
        logging.info("started RunRanger  - watch_directory " + watch_directory)

        rr = ResourceRanger()
        try:
            imask  = inotify.IN_MOVED_TO | inotify.IN_CREATE | inotify.IN_DELETE | inotify.IN_MODIFY
            if conf.role == 'bu':
                #currently does nothing on bu
                rr.register_inotify_path(resource_base, imask)
                rr.register_inotify_path(resource_base+'/boxes', imask)
            else:
                rr.register_inotify_path(resource_base, imask)
                rr.register_inotify_path(resource_base+'/idle', imask)
                rr.register_inotify_path(resource_base+'/except', imask)
            rr.start_inotify()
            logging.info("started ResourceRanger - watch_directory "+resource_base)
        except Exception as ex:
            logging.error("Exception caught in starting notifier2")
            logging.error(ex)

        try:
            cgitb.enable(display=0, logdir="/tmp")
            handler = CGIHTTPServer.CGIHTTPRequestHandler
            # the following allows the base directory of the http
            # server to be 'conf.watch_directory, which is writeable
            # to everybody
            if os.path.exists(conf.watch_directory+'/cgi-bin'):
                os.remove(conf.watch_directory+'/cgi-bin')
            os.symlink('/opt/hltd/cgi',conf.watch_directory+'/cgi-bin')

            handler.cgi_directories = ['/cgi-bin']
            logging.info("starting http server on port "+str(conf.cgi_port))
            httpd = BaseHTTPServer.HTTPServer(("", conf.cgi_port), handler)

            logging.info("hltd serving at port "+str(conf.cgi_port)+" with role "+conf.role)
            os.chdir(conf.watch_directory)
            httpd.serve_forever()
        except KeyboardInterrupt:
            logging.info("terminating all ongoing runs")
            for run in run_list:
                if conf.role=='fu':
                    run.Shutdown()
                elif conf.role=='bu':
                    run.ShutdownBU()
            logging.info("terminated all ongoing runs")
            logging.info("stopping run ranger inotify helper")
            runRanger.stop_inotify()
            logging.info("stopping resource ranger inotify helper")
            rr.stop_inotify()
            if boxInfo is not None:
                logging.info("stopping boxinfo updater")
                boxInfo.stop()
            if logCollector is not None:
                logCollector.terminate()
            logging.info("stopping system monitor")
            rr.stop_managed_monitor()
            logging.info("closing httpd socket")
            httpd.socket.close()
            logging.info(threading.enumerate())
            logging.info("unmounting mount points")
            if cleanup_mountpoints(remount=False)==False:
              time.sleep(1)
              cleanup_mountpoints(remount=False)
            
            logging.info("shutdown of service completed")
        except Exception as ex:
            logging.info("exception encountered in operating hltd")
            logging.info(ex)
            runRanger.stop_inotify()
            rr.stop_inotify()
            rr.stop_managed_monitor()
            raise


if __name__ == "__main__":
    daemon = hltd('/var/run/hltd.pid')
    daemon.start()
