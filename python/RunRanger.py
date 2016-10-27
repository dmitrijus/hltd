import os
import signal
import time
import threading
import httplib
import shutil
import demote
import prctl
from signal import SIGKILL
import subprocess
import logging
import socket

import Run
from HLTDCommon import restartLogCollector,dqm_globalrun_filepattern
from inotifywrapper import InotifyWrapper
from buemu import BUEmu

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

class RunRanger:

    def __init__(self,instance,confClass,stateInfo,resInfo,runList,resourceRanger,mountMgr,logCollector,nsslock,resource_lock):
        self.inotifyWrapper = InotifyWrapper(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.instance = instance
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.rr = resourceRanger
        self.mm = mountMgr
        self.logCollector = logCollector
        self.nsslock = nsslock
        self.resource_lock = resource_lock
        self.bu_emulator = None
        global conf
        conf = confClass

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self):
        self.inotifyWrapper.stop()
        self.inotifyWrapper.join()
        self.logger.info("Inotify wrapper shutdown done")

    def process_IN_CREATE(self, event):
        fullpath = event.fullpath
        self.logger.info('event '+fullpath)
        dirname=fullpath[fullpath.rfind("/")+1:]
        self.logger.info('new filename '+dirname)
        nr=0
        if dirname.startswith('run'):
            if os.path.islink(fullpath):
                self.logger.info('directory ' + fullpath + ' is link. Ignoring this run')
                return
            if not os.path.isdir(fullpath):
                self.logger.info(fullpath +' is a file. A directory is needed to start a run.')
                return
            nr=int(dirname[3:])
            if nr!=0:
                # the dqm BU processes a run if the "global run file" is not mandatory or if the run is a global run
                is_global_run = os.path.exists(fullpath[:fullpath.rfind("/")+1] + dqm_globalrun_filepattern.format(str(nr).zfill(conf.run_number_padding)))
                dqm_processing_criterion = (not conf.dqm_globallock) or (conf.role != 'bu') or  (is_global_run)

                if (not conf.dqm_machine) or dqm_processing_criterion:
                    try:
                        self.logger.info('new run '+str(nr))
                        #terminate quarantined runs
                        for run in self.runList.getQuarantinedRuns():
                            #run shutdown waiting for scripts to finish
                            run.startShutdown(True,False)
                            time.sleep(.1)

                        #self.state.resources_blocked_flag=False
                        if self.state.cloud_mode==True:
                            self.logger.info("received new run notification in CLOUD mode. Ignoring new run.")
                            os.rmdir(fullpath)
                            return
                        self.state.masked_resources=False #clear this flag for run that was stopped manually
                        if conf.role == 'fu':
                            bu_dir = self.mm.bu_disk_list_ramdisk_instance[0]+'/'+dirname
                            try:
                                os.symlink(bu_dir+'/jsd',fullpath+'/jsd')
                            except:
                                if not conf.dqm_machine:
                                    self.logger.warning('jsd directory symlink error, continuing without creating link')
                                pass
                        else:
                            bu_dir = ''

                        #check if this run is a duplicate
                        if self.runList.getRun(nr)!=None:
                            raise Exception("Attempting to create duplicate run "+str(nr))

                        # in case of a DQM machines create an EoR file
                        if conf.dqm_machine and conf.role == 'bu':
                            for run in self.runList.getOngoingRuns():
                                EoR_file_name = run.dirname + '/' + 'run' + str(run.runnumber).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn'
                                if run.is_ongoing_run and not os.path.exists(EoR_file_name):
                                    # create an EoR file that will trigger all the running jobs to exit nicely
                                    open(EoR_file_name, 'w').close()

                        if not len(self.runList.getActiveRuns()) and conf.role == 'fu':
                          if self.state.os_cpuconfig_change:
                            self.state.lock.acquire()
                            tmp_os_cpuconfig_change = self.state.os_cpuconfig_change
                            self.state.os_cpuconfig_change=0
                            self.state.lock.release()
                            self.resource_lock.acquire()
                            tmp_change = self.resInfo.updateIdles(tmp_os_cpuconfig_change,checkLast=False)
                            self.state.os_cpuconfig_change=tmp_change
                            self.resource_lock.release()

                        run = Run.Run(nr,fullpath,bu_dir,self.instance,conf,self.state,self.resInfo,self.runList,self.rr,self.mm,self.nsslock,self.resource_lock)
                        if not run.inputdir_exists and conf.role=='fu':
                            self.logger.info('skipping '+ fullpath + ' with raw input directory missing')
                            shutil.rmtree(fullpath)
                            del run
                            return
                        self.resource_lock.acquire()
                        self.runList.add(run)
                        try:
                            if conf.role=='fu' and not self.state.entering_cloud_mode and not self.resInfo.has_active_resources():
                                self.logger.error("RUN:"+str(run.runnumber)+' - trying to start a run without any available resources (all are QUARANTINED) - this requires manual intervention !')
                        except Exception,ex:
                            self.logger.exception(ex)

                        if run.AcquireResources(mode='greedy'):
                            run.CheckTemplate()
                            run.Start()
                        else:
                            #BU mode: failed to get blacklist
                            self.runList.remove(nr)
                            self.resource_lock.release()
                            try:del run
                            except:pass
                            return
                        self.resource_lock.release()

                        if conf.role == 'bu' and conf.instance != 'main':
                            self.logger.info('creating run symlink in main ramdisk directory')
                            main_ramdisk = os.path.dirname(os.path.normpath(conf.watch_directory))
                            os.symlink(fullpath,os.path.join(main_ramdisk,os.path.basename(fullpath)))
                    except OSError as ex:
                        self.logger.error("RUN:"+str(nr)+" - exception in new run handler: "+str(ex)+" / "+ex.filename)
                        self.logger.exception(ex)
                    except Exception as ex:
                        self.logger.error("RUN:"+str(nr)+" - RunRanger: unexpected exception encountered in forking hlt slave")
                        self.logger.exception(ex)
                    try:self.resource_lock.release()
                    except:pass

        elif dirname.startswith('emu'):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    """
                    start a new BU emulator run here - this will trigger the start of the HLT test run
                    """
                    #TODO:fix this constructor in buemu.py
                    #self.bu_emulator = BUEmu(conf,self.mm.bu_disk_list_ramdisk_instance,preexec_function)
                    self.bu_emulator = BUEmu(conf,self.mm.bu_disk_list_ramdisk_instance)
                    self.bu_emulator.startNewRun(nr)

                except Exception as ex:
                    self.logger.info("exception encountered in starting BU emulator run")
                    self.logger.info(ex)

                os.remove(fullpath)

        elif dirname.startswith('end'):
            # need to check is stripped name is actually an integer to serve
            # as run number
            if dirname[3:].isdigit():
                nr=int(dirname[3:])
                if nr!=0:
                    try:
                        endingRun = self.runList.getRun(nr)
                        if endingRun==None:
                            self.logger.warning('request to end run '+str(nr)
                                          +' which does not exist')
                            os.remove(fullpath)
                        else:
                            self.logger.info('end run '+str(nr))
                            #remove from runList to prevent intermittent restarts
                            #lock used to fix a race condition when core files are being moved around
                            endingRun.is_ongoing_run==False
                            time.sleep(.1)
                            if conf.role == 'fu':
                                endingRun.StartWaitForEnd()
                            if self.bu_emulator and self.bu_emulator.runnumber != None:
                                self.bu_emulator.stop()
                            #self.logger.info('run '+str(nr)+' removing end-of-run marker')
                            #os.remove(fullpath)

                    except Exception as ex:
                        self.logger.info("exception encountered when waiting hlt run to end")
                        self.logger.info(ex)
                else:
                    self.logger.error('request to end run '+str(nr)
                                  +' which is an invalid run number - this should '
                                  +'*never* happen')
            else:
                self.logger.error('request to end run '+str(nr)
                              +' which is NOT a run number - this should '
                              +'*never* happen')

        elif dirname.startswith('herod') or dirname.startswith('tsunami') or dirname.startswith('brutus'):
            try:
                os.remove(fullpath)
            except:
                #safety net if cgi script removes herod
                pass
            if dirname.startswith('herod'): nlen=len('herod')
            if dirname.startswith('tsunami'): nlen=len('tsunami')
            if dirname.startswith('brutus'): nlen=len('brutus')
            try:
              rn=0
              if nlen!=len(dirname):
                if dirname[nlen:].isdigit():
                  rn = int(dirname[nlen:])
                else: self.logger.error("can not read run number suffix from "+dirname+ ". Aborting command")
            except:
                pass
            if conf.role == 'fu':
                self.logger.info("killing all CMSSW child processes")
                for run in self.runList.getActiveRuns():
                  if nlen==len(dirname) or rn==0 or run.runnumber==rn or run.checkQuarantinedLimit():
                    if dirname.startswith('brutus'):
                        run.Shutdown(True,False)
                    else:
                        run.Shutdown(True,True)
                time.sleep(.2)
                #clear all quarantined cores
                for cpu in self.resInfo.q_list:
                    try:
                        self.logger.info('Clearing quarantined resource '+cpu)
                        self.resInfo.resmove(self.resInfo.quarantined,self.resInfo.idles,cpu)
                    except:
                        self.logger.info('Quarantined resource was already cleared: '+cpu)
                self.resInfo.q_list=[]

            elif conf.role == 'bu':
                for run in self.runList.getActiveRuns():
                    if rn==0 or run.runnumber==rn:
                      run.createEmptyEoRMaybe()
                      run.ShutdownBU()

                #delete input and output BU directories
                if dirname.startswith('tsunami'):
                    self.logger.info('tsunami approaching: cleaning all ramdisk and output run data')
                    if rn:
                        self.mm.cleanup_bu_disks(rn,True,True)
                    else:
                        self.mm.cleanup_bu_disks(None,True,True)

                #contact any FU that appears alive
                boxdir = conf.resource_base +'/boxes/'
                try:
                    dirlist = os.listdir(boxdir)
                    current_time = time.time()
                    self.logger.info("sending "+dirname+" to child FUs")
                    herod_threads = []
                    for name in dirlist:
                        if name == os.uname()[1]:continue
                        age = current_time - os.path.getmtime(boxdir+name)
                        self.logger.info('found box '+name+' with keepalive age '+str(age))
                        if age < 300:
                            self.logger.info('contacting '+str(name))
                            def notifyHerod(hname):

                                host_short = hname.split('.')[0]
                                #get hosts from cached ip if possible to avoid hammering DNS
                                try:
                                    hostip = self.rr.boxInfo.FUMap[host_short][0]['ip']
                                except:
                                    self.logger.info(str(host_short) + ' not in FUMap')
                                    hostip=hname

                                attemptsLeft=4
                                while attemptsLeft>0:
                                    attemptsLeft-=1
                                    try:
                                        connection = httplib.HTTPConnection(hostip, conf.cgi_port - conf.cgi_instance_port_offset,timeout=10)
                                        time.sleep(0.2)
                                        connection.request("GET",'cgi-bin/herod_cgi.py?command='+str(dirname))
                                        time.sleep(0.3)
                                        response = connection.getresponse()
                                        self.logger.info("sent "+ dirname +" to child FUs")
                                        break
                                    except Exception as ex:
                                        self.logger.error("exception encountered in contacting resource "+str(hostip))
                                        self.logger.exception(ex)
                                        time.sleep(.3)
 
                            #try:
                            #    connection = httplib.HTTPConnection(name, conf.cgi_port - conf.cgi_instance_port_offset,timeout=10)
                            #    time.sleep(0.1)
                            #    connection.request("GET",'cgi-bin/herod_cgi.py?command='+str(dirname))
                            #    time.sleep(0.15)
                            #    response = connection.getresponse()
                            #except Exception as ex:
                            #    self.logger.error("exception encountered in contacting resource "+str(name))
                            #    self.logger.exception(ex)
                            #self.logger.info("sent "+ dirname +" to child FUs")

                            try:
                                herodThread = threading.Thread(target=notifyHerod,args=[name])
                                herodThread.start()
                                herod_threads.append(herodThread)
                            except Exception as ex:
                                self.logger.exception(ex)

                    #join herod before returning
                    for herodThread in herod_threads:
                      herodThread.join()

                except Exception as ex:
                    self.logger.error("exception encountered in contacting resources")
                    self.logger.info(ex)

        elif dirname.startswith('cleanoutput'):
            try:os.remove(fullpath)
            except:pass
            nlen = len('cleanoutput')
            if len(dirname)==nlen:
                self.logger.info('cleaning output (all run data)')
                self.mm.cleanup_bu_disks(None,False,True)
            else:
                try:
                    rn = int(dirname[nlen:])
                    self.logger.info('cleaning output (only for run '+str(rn)+')')
                    self.mm.cleanup_bu_disks(rn,False,True)
                except:
                    self.logger.error('Could not parse '+dirname)

        elif dirname.startswith('cleanramdisk'):
            try:os.remove(fullpath)
            except:pass
            nlen = len('cleanramdisk')
            if len(dirname)==nlen:
                self.logger.info('cleaning ramdisk (all run data)')
                self.mm.cleanup_bu_disks(None,True,False)
            else:
                try:
                    rn = int(dirname[nlen:])
                    self.logger.info('cleaning ramdisk (only for run '+str(rn)+')')
                    self.mm.cleanup_bu_disks(rn,True,False)
                except:
                    self.logger.error('Could not parse '+dirname)

        elif dirname.startswith('populationcontrol'):
            if len(self.runList.runs)>0:
                self.logger.info("terminating all ongoing runs via cgi interface (populationcontrol): "+str(self.runList.getActiveRunNumbers()))
                for run in self.runList.getActiveRuns():
                    if conf.role=='fu':
                        run.Shutdown(True,True)
                    elif conf.role=='bu':
                        run.ShutdownBU()
                self.logger.info("terminated all ongoing runs via cgi interface (populationcontrol)")
            try:os.remove(fullpath)
            except:pass

        elif dirname.startswith('harakiri') and conf.role == 'fu':
            try:os.remove(fullpath)
            except:pass
            pid=os.getpid()
            self.logger.info('asked to commit seppuku:'+str(pid))
            try:
                self.logger.info('sending signal '+str(SIGKILL)+' to myself:'+str(pid))
                retval = os.kill(pid, SIGKILL)
                self.logger.info('sent SIGINT to myself:'+str(pid))
                self.logger.info('got return '+str(retval)+'waiting to die...and hope for the best')
            except Exception as ex:
                self.logger.error("exception in committing harakiri - the blade is not sharp enough...")
                self.logger.error(ex)

        elif dirname.startswith('quarantined'):
            try:
                os.remove(dirname)
            except:
                pass
            if dirname[11:].isdigit():
                nr=int(dirname[11:])
                if nr!=0:
                    try:
                        run = self.runList.getRun(nr)
                        if run.checkQuarantinedLimit():
                            if self.runList.isLatestRun(run):
                                self.logger.info('reached quarantined limit - pending Shutdown for run:'+str(nr))
                                run.pending_shutdown=True
                            else:
                                self.logger.info('reached quarantined limit - initiating Shutdown for run:'+str(nr))
                                run.startShutdown(True,False)
                    except Exception as ex:
                        self.logger.exception(ex)

        elif dirname.startswith('suspend') and conf.role == 'fu':
            self.logger.info('suspend mountpoints initiated')
            self.state.suspended=True
            replyport = int(dirname[7:]) if dirname[7:].isdigit()==True else conf.cgi_port

            #terminate all ongoing runs
            for run in self.runList.getActiveRuns():
                run.Shutdown(True,True)

            time.sleep(.5)
            #local request used in case of stale file handle
            if replyport==0:
                umount_success = self.mm.cleanup_mountpoints(self.nsslock)
                try:os.remove(fullpath)
                except:pass
                self.state.suspended=False
                self.logger.info("Remount requested locally is performed.")
                return

            umount_success = self.mm.cleanup_mountpoints(self.nsslock,remount=False)

            if umount_success==False:
                time.sleep(1)
                self.logger.error("Suspend initiated from BU failed, trying again...")
                #notifying itself again
                try:os.remove(fullpath)
                except:pass
                fp = open(fullpath,"w+")
                fp.close()
                return

            #find out BU name from bus_config
            bu_name=None
            bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
            try:
                if os.path.exists(bus_config):
                    for line in open(bus_config,'r'):
                        bu_name=line.split('.')[0]
                        break
            except:
                pass

            #first report to BU that umount was done
            try:
                if bu_name==None:
                    self.logger.fatal("No BU name was found in the bus.config file. Leaving mount points unmounted until the hltd service restart.")
                    os.remove(fullpath)
                    return
                connection = httplib.HTTPConnection(bu_name, replyport+20,timeout=5)
                connection.request("GET",'cgi-bin/report_suspend_cgi.py?host='+os.uname()[1])
                response = connection.getresponse()
            except Exception as ex:
                self.logger.error("Unable to report suspend state to BU "+str(bu_name)+':'+str(replyport+20))
                self.logger.exception(ex)

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
                            self.logger.info('exception test 1')
                            time.sleep(5)
                            continue
                    if bu_name==None:
                        self.logger.info('exception test 2')
                        time.sleep(5)
                        continue

                    self.logger.info('checking if BU hltd is available...')
                    connection = httplib.HTTPConnection(bu_name, replyport,timeout=5)
                    connection.request("GET",'cgi-bin/getcwd_cgi.py')
                    response = connection.getresponse()
                    self.logger.info('BU hltd is running !...')
                    #if we got here, the service is back up
                    break
                except Exception as ex:
                    try:
                        self.logger.info('Failed to contact BU hltd service: ' + str(ex.args[0]) +" "+ str(ex.args[1]))
                    except:
                        self.logger.info('Failed to contact BU hltd service '+str(ex))
                    time.sleep(5)

            #mount again
            self.mm.cleanup_mountpoints(self.nsslock)
            try:os.remove(fullpath)
            except:pass
            self.state.suspended=False

            self.logger.info("Remount is performed")

        elif dirname.startswith('stop') and conf.role == 'fu':
            self.logger.fatal("Stopping all runs..")
            self.state.masked_resources=True
            #make sure to not run inotify acquire while we are here
            self.resource_lock.acquire()
            self.state.disabled_resource_allocation=True
            self.resource_lock.release()

            #shut down any quarantined runs
            try:
                for run in self.runList.getQuarantinedRuns():
                    run.Shutdown(True,False)
                listOfActiveRuns = self.runList.getActiveRuns()
                for run in listOfActiveRuns:
                    if not run.pending_shutdown:
                        if len(run.online_resource_list)==0:
                            run.Shutdown(True,False)
                        else:
                            self.resource_lock.acquire()
                            if dirname!='stopnow':run.Stop()
                            else:run.Stop(stop_now=True)
                            self.resource_lock.release()
                time.sleep(.1)
            except Exception as ex:
                self.logger.fatal("Unable to stop run(s)")
                self.logger.exception(ex)
            self.state.disabled_resource_allocation=False
            try:self.resource_lock.release()
            except:pass
            os.remove(fullpath)

        elif dirname.startswith('exclude') and conf.role == 'fu':
            #service on this machine is asked to be excluded for cloud use
            if self.state.cloud_mode:
                if self.state.abort_cloud_mode:
                  self.logger.info('received exclude during cloud mode abort. machine exclude will be resumed..')
                  self.state.abort_cloud_mode=False
                else:
                  self.logger.info('already in cloud mode...')
                  os.remove(fullpath)
                  return
            else:
                self.logger.info('machine exclude for cloud initiated. stopping any existing runs...')

            if self.state.cloud_status()>1:
                #execute run cloud stop script in case something is running
                self.state.extinguish_cloud(repeat=True)
                self.logger.error("Unable to switch to cloud mode (external script error).")
                os.remove(fullpath)
                return

            #make sure to not run not acquire resources by inotify while we are here
            self.resource_lock.acquire()
            self.state.cloud_mode=True
            self.state.entering_cloud_mode=True
            self.resource_lock.release()
            time.sleep(.1)

            #shut down any quarantined runs
            try:
                for run in self.runList.getQuarantinedRuns():
                    run.Shutdown(True,False)

                requested_stop=False
                listOfActiveRuns = self.runList.getActiveRuns()
                for run in listOfActiveRuns:
                    if not run.pending_shutdown:
                        if len(run.online_resource_list)==0:
                            run.Shutdown(True,False)
                        else:
                            self.resource_lock.acquire()
                            requested_stop=True
                            if dirname.startswith('excludenow'):
                              #let jobs stop without 3 LS drain
                              run.Stop(stop_now=True)
                            else:
                              #regular stop with 3 LS drain
                              run.Stop()
                            self.resource_lock.release()

                time.sleep(.1)
                self.resource_lock.acquire()
                if requested_stop==False:
                    #no runs present, switch to cloud mode immediately
                    self.state.entering_cloud_mode=False
                    self.resInfo.move_resources_to_cloud()
                    self.resource_lock.release()
                    result = self.state.ignite_cloud()
                    cloud_st = self.state.cloud_status()
                    self.logger.info("cloud is on? : "+str(cloud_st == 1) + ' (status code '+str(cloud_st)+')')
            except Exception as ex:
                self.logger.fatal("Unable to clear runs. Will not enter VM mode.")
                self.logger.exception(ex)
                self.state.entering_cloud_mode=False
                self.state.cloud_mode=False
            try:self.resource_lock.release()
            except:pass
            os.remove(fullpath)

        elif dirname.startswith('include') and conf.role == 'fu':
            if not self.state.cloud_mode:
                self.logger.warning('received notification to exit from cloud but machine is not in cloud mode!')
                if self.state.cloud_status():
                    self.logger.info('cloud scripts are still running, trying to stop')
                    returnstatus = self.state.extinguish_cloud(repeat=True)
                os.remove(fullpath)
                return

            self.resource_lock.acquire()
            #schedule cloud mode cancel when HLT shutdown is completed
            if self.state.entering_cloud_mode:
                self.logger.info('include receiver while entering cloud mode. setting abort flag...')
                self.state.abort_cloud_mode=True
                self.resource_lock.release()
                os.remove(fullpath)
                return

            #switch to cloud stopping
            self.state.exiting_cloud_mode=True

            #unlock before stopping cloud scripts
            self.resource_lock.release()

            #cloud is being switched off so we don't care if its running status is false
            if not self.state.cloud_status(reportExitCodeError=False):
                self.logger.warning('received command to deactivate cloud, but external script reports that cloud is not running!')

            #stop cloud
            returnstatus = self.state.extinguish_cloud(True)

            retried=False
            attempts=0
            err_attempts=0

            while True:
                last_status = self.state.cloud_status()
                if last_status>=1: #state: running or error
                    self.logger.info('cloud is still active')
                    time.sleep(1)
                    attempts+=1
                    if last_status>1:
                      err_attempts+=1
                      self.logger.warning('external cloud script reports error code' + str(last_status) + '.')
                      if err_attempts>100:
                        #if error is persistent, give up eventually and complain with fatal error 
                        os.remove(fullpath)
                        self.state.exiting_cloud_mode=False
                        time.sleep(1)
                        self.logger.critical('failed to switch off cloud. last status reported: '+str(last_status))
                        return
                    if (attempts%60==0 and not retried):
                        self.logger.info('retrying cloud kill after 1 minute')
                        returnstatus = self.state.extinguish_cloud(True)
                        retried=True
                    elif (err_attempts and err_attempts%10==0):
                        self.logger.info('retrying cloud kill after 10 status checks returning error')
                        returnstatus = self.state.extinguish_cloud(True)
                        retried=True
                    if attempts>600:
                        os.remove(fullpath)
                        self.state.exiting_cloud_mode=False
                        time.sleep(1)
                        self.logger.critical('failed to switch off cloud after attempting for 10 minutes! last status reports cloud is running...')
                        return
                    continue
                else:
                    self.logger.info('cloud scripts have been deactivated')
                    #switch resources back to normal
                    self.resource_lock.acquire()
                    #self.state.resources_blocked_flag=True
                    self.state.cloud_mode=False
                    self.resInfo.cleanup_resources()
                    self.resource_lock.release()
                    break

            self.state.exiting_cloud_mode=False
            os.remove(fullpath)
            #sleep some time to let core file notifications to finish
            time.sleep(2)
            self.logger.info('cloud mode in hltd has been switched off')
        
        elif dirname.startswith('resourceupdate'):
            self.logger.info('resource update event received with '+str(self.state.os_cpuconfig_change))
            #freeze any update during this operation
            self.state.lock.acquire()
            self.resource_lock.acquire()
            try:
                tmp_change = self.state.os_cpuconfig_change
                self.state.os_cpuconfig_change=0
                lastRun = self.runList.getLastOngoingRun()
                #check if this can be done without touching current run
                if tmp_change>0:
                   self.logger.info('adding ' + str(tmp_change))
                   tmp_change = self.resInfo.addResources(tmp_change)
                elif tmp_change<0:
                   self.logger.info('removing ' + str(-tmp_change))
                   tmp_change = self.resInfo.updateIdles(tmp_change,checkLast=False)
                self.logger.info('left ' + str(tmp_change))
                if not tmp_change:pass 
                elif lastRun:
                  #1st case (removed resources): quarantine surplus resources
                  if tmp_change<0:
                    numQuarantine = -tmp_change
                    res_list_join = []
                    for resource in lastRun.online_resource_list:
                      if numQuarantine>0:
                        #if len(resource.cpu)<= numQuarantine:
                        if resource.Stop(delete_resources=True):
                          numQuarantine -= len(resource.cpu)
                          res_list_join.append(resource)
                    self.resource_lock.release()

                    #join threads with timeout
                    time_left = 120
                    res_list_join_alive=[]
                    for resource in res_list_join:
                      time_start = time.time()
                      try: resource.watchdog.join(time_left) #synchronous stop
                      except: self.logger.info('join failed')
                      self.logger.info('joined resource...')
                      if resource.watchdog.isAlive():
                        res_list_join_alive.append(resource)
                      time_left -= time.time()-time_start
                      if time_left<=0.1:time_left=0.1
                      
                    #invoke terminate if any threads/processes still alive
                    for resource in res_list_join_alive:
                      try:
                        self.logger.info('terminating process ' + str(resource.process.pid))
                        resource.process.terminate()
                      except Exception as ex:
                        self.logger.exception(ex)

                    for resource in res_list_join_alive:
                      try:
                        resource.watchdog.join(30)
                        if resource.watchdog.isAlive():
                          self.logger.info('killing process ' + str(resource.process.pid))
                          resource.process.kill()
                          resource.watchdog.join(10)
                      except:
                        pass

                    res_list_join=[]
                    res_list_join_alive=[]
                    self.resource_lock.acquire()
                    if numQuarantine<0:
                      #if not a matching number of resources was stopped,add back part of resources later (next run start) 
                      self.state.os_cpuconfig_change=-numQuarantine
                      #let add back resources
                      tmp_change = self.state.os_cpuconfig_change
                  if tmp_change>0:
                    #add more resources
                    left = self.resInfo.addResources(tmp_change)
                    self.state.os_cpuconfig_change=left
                else:
                  #if run is ending (not ongoing, but still keeping resources), update at the next run start
                  self.state.os_cpuconfig_change=tmp_change
            except Exception as ex:
              self.logger.error('failed to process resourceupdate event: ' + str(ex))
              self.logger.exception(ex)
            finally:
              #refresh parameter values
              self.resInfo.calculate_threadnumber()
              try:self.resource_lock.release()
              except:pass
              self.state.lock.release()
            self.logger.info('end resourceupdate. Left:'+str(self.state.os_cpuconfig_change))
            os.remove(fullpath)
 

        elif dirname.startswith('logrestart'):
            #hook to restart logcollector process manually
            restartLogCollector(conf,self.logger,self.logCollector,self.instance)
            os.remove(fullpath)

        elif dirname.startswith('restart'):
            self.logger.info('restart event')

            if conf.role=='bu':
                process = subprocess.Popen(['/opt/hltd/scripts/appliancefus.py'],stdout=subprocess.PIPE)
                out = process.communicate()[0].split('\n')[0]
                fus=[]
                if process.returncode==0:fus = out.split(',')
                else:
                  dirlist = os.listdir(os.path.join(conf.watch_directory,'appliance','boxes'))
                  for machine in dirlist:
                    if machine == os.uname()[1]:continue
                    fus.append(machine)

                def contact_restart(host):
                  host_short = host.split('.')[0]
                  #get hosts from cached ip if possible to avoid hammering DNS
                  try:
                    host = self.rr.boxInfo.FUMap[host_short][0]['ip']
                    self.logger.info(host_short + ' ' + host)
                  except:
                    self.logger.warning(str(host_short) + ' not in FUMap')
                  try:
                    connection = httplib.HTTPConnection(host,conf.cgi_port,timeout=20)
                    connection.request("GET",'cgi-bin/restart_cgi.py')
                    time.sleep(.2)
                    response = connection.getresponse()
                    connection.close()
                  except socket.error as ex:
                    self.logger.warning('error contacting '+str(host)+': socket.error: ' + str(ex))
                    #try again in a moment (DNS could be loaded)
                    time.sleep(1)
                    try:
                      connection = httplib.HTTPConnection(host,conf.cgi_port,timeout=20)
                      connection.request("GET",'cgi-bin/restart_cgi.py')
                      time.sleep(.2)
                      response = connection.getresponse()
                      connection.close()
                    except socket.error as ex:
                      self.logger.warning('error contacting '+str(host)+': socket.error: ' + str(ex))

                  #catch general exception
                  except Exception as ex:
                    self.logger.warning('problem contacting host' + str(host) + ' ' + str(ex))

                fu_threads = []
                for fu in fus:
                    if not len(fu):continue
                    fu_thread = threading.Thread(target=contact_restart,args=[fu])
                    fu_threads.append(fu_thread)
                    fu_thread.start()
                for fu_thread in fu_threads:
                    fu_thread.join()
                  
            #some time to allow cgi return
            time.sleep(1)
            os.remove(fullpath)
            pr = subprocess.Popen(["/opt/hltd/scripts/restart.py"],close_fds=True)
            self.logger.info('restart imminent, waiting to die and raise from the ashes once again')

        self.logger.debug("completed handling of event "+fullpath)

    def process_default(self, event):
        self.logger.info('event '+event.fullpath+' type '+str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]


