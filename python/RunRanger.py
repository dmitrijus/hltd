import os
import time
import httplib
import shutil
import demote
import prctl
from signal import SIGKILL
import logging

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
        cached_pending_run = None
        fullpath = event.fullpath
        self.logger.info('event '+fullpath)
        dirname=fullpath[fullpath.rfind("/")+1:]
        self.logger.info('new filename '+dirname)
        nr=0
        if dirname.startswith('run'):
            if dirname.endswith('.reprocess'):
                #reprocessing triggered
                dirname = dirname[:dirname.rfind('.reprocess')]
                fullpath = fullpath[:fullpath.rfind('.reprocess')]
                self.logger.info('Triggered reprocessing of '+ dirname)
                try:os.unlink(event.fullpath)
                except:
                    try:os.rmdir(event.fullpath)
                    except:pass
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

                        self.state.resources_blocked_flag=False
                        if self.state.cloud_mode==True:
                            self.logger.info("received new run notification in CLOUD mode. Ignoring new run.")
                            #remember this run and attempt to continue it once hltd exits the cloud mode
                            cached_pending_run = fullpath
                            os.rmdir(fullpath)
                            return
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
                                self.logger.error("RUN:"+str(run.runnumber)+' - trying to start a run without any available resources - this requires manual intervention !')
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
            if conf.role == 'fu':
                self.logger.info("killing all CMSSW child processes")
                for run in self.runList.getActiveRuns():
                    if dirname.startswith('tsunami') or dirname.startswith('brutus'):
                        run.Shutdown(True,True)
                    else:
                        run.Shutdown(True,False)
                time.sleep(.2)
                #clear all quarantined cores
                for cpu in self.resInfo.q_list:
                    try:
                        self.logger.info('Clearing quarantined resource '+cpu)
                        self.resInfo.resmove(self.resInfo.quarantined,self.resInfo.idles,cpu)
                    except:
                        self.logger.info('Quarantined resource was already cleared: '+cpu)
                self.resInfo.self.resInfo.q_list=[]

            elif conf.role == 'bu':
                for run in self.runList.getActiveRuns():
                    run.createEmptyEoRMaybe()
                    run.ShutdownBU()

                #delete input and output BU directories
                if dirname.startswith('tsunami'):
                    self.logger.info('tsunami approaching: cleaning all ramdisk and output run data')
                    self.mm.cleanup_bu_disks(None,True,True)

                #contact any FU that appears alive
                boxdir = conf.resource_base +'/boxes/'
                try:
                    dirlist = os.listdir(boxdir)
                    current_time = time.time()
                    self.logger.info("sending "+dirname+" to child FUs")
                    for name in dirlist:
                        if name == os.uname()[1]:continue
                        age = current_time - os.path.getmtime(boxdir+name)
                        self.logger.info('found box '+name+' with keepalive age '+str(age))
                        if (age < 20 and dirname.startswith('herod')) or age < 300:
                            try:
                                self.logger.info('contacting '+name)
                                connection = httplib.HTTPConnection(name, conf.cgi_port - conf.cgi_instance_port_offset,timeout=5)
                                time.sleep(0.05)
                                connection.request("GET",'cgi-bin/herod_cgi.py?command='+str(dirname))
                                time.sleep(0.1)
                                response = connection.getresponse()
                            except Exception as ex:
                                self.logger.error("exception encountered in contacting resource "+str(name))
                                self.logger.exception(ex)
                    self.logger.info("sent "+ dirname +" to child FUs")
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

        elif dirname=='stop' and conf.role == 'fu':
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
                            run.Stop()
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
                self.logger.info('already in cloud mode...')
                os.remove(fullpath)
                return
            else:
                self.logger.info('machine exclude for cloud initiated. stopping any existing runs...')

            if self.state.cloud_status()>1:
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
                    self.logger.info("cloud is on? : "+str(self.state.cloud_status() == 1))
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
                    returnstatus = self.state.extinguish_cloud(True)
                os.remove(fullpath)
                return

            self.resource_lock.acquire()
            #schedule cloud mode cancel when HLT shutdown is completed
            if self.state.entering_cloud_mode:
                self.state.abort_cloud_mode=True
                self.resource_lock.release()
                os.remove(fullpath)
                return

            #switch to cloud stopping
            self.state.exiting_cloud_mode=True

            #unlock before stopping cloud scripts
            self.resource_lock.release()

            #cloud is being switched off so we don't care if its running status is ok or not
            if not self.state.cloud_status(reportExitCodeError=False):
                self.logger.warning('received command to deactivate cloud, but external script reports that cloud is not running!')

            #stop cloud
            returnstatus = self.state.extinguish_cloud(True)

            retried=False
            attempts=0

            while True:
                last_status = self.state.cloud_status()
                if last_status==1: #state: running
                    self.logger.info('cloud is still active')
                    time.sleep(1)
                    attempts+=1
                    if attempts%60==0 and not retried:
                        self.logger.info('retrying cloud kill after 1 minute')
                        returnstatus = self.state.extinguish_cloud(True)
                        retried=True
                    continue
                else:
                    if last_status>1:
                        self.logger.warning('external cloud script report error code' + str(last_status) + 'assuming that cloud is off')
                    else:
                        self.logger.info('cloud scripts have been deactivated')
                    #switch resources back to normal
                    self.resource_lock.acquire()
                    self.state.resources_blocked_flag=True
                    self.state.cloud_mode=False
                    self.resInfo.cleanup_resources()
                    self.resource_lock.release()
                    break

            self.state.exiting_cloud_mode=False
            os.remove(fullpath)
            if cached_pending_run != None:
                #create last pending run received during the cloud mode
                time.sleep(5) #let core file notifications run
                os.mkdir(cached_pending_run)
                cached_pending_run = None
            else: time.sleep(2)
            self.logger.info('cloud mode in hltd has been switched off')

        elif dirname.startswith('logrestart'):
            #hook to restart logcollector process manually
            restartLogCollector(conf,self.logger,self.logCollector,self.instance)
            os.remove(fullpath)

        self.logger.debug("completed handling of event "+fullpath)

    def process_default(self, event):
        self.logger.info('event '+event.fullpath+' type '+str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]


