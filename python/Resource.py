import os
import time
import simplejson as json
import re
import httplib
import subprocess
import threading
import demote
import prctl
from signal import SIGKILL
import logging

from HLTDCommon import dqm_globalrun_filepattern

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

class RunCommon:

    STARTING = 'starting'
    ACTIVE = 'active'
    STOPPING = 'stopping'
    ABORTED = 'aborted'
    COMPLETE = 'complete'
    ABORTCOMPLETE = 'abortcomplete'

    VALID_MARKERS = [STARTING,ACTIVE,STOPPING,COMPLETE,ABORTED,ABORTCOMPLETE]



class OnlineResource:

    def __init__(self,parent,resourcenames,resource_lock,f_ip=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parent = parent
        global conf
        conf = self.parent.conf
        self.resInfo = parent.resInfo
        #self.hoststate = 0 #@@MO what is this used for?
        self.cpu = resourcenames
        self.hostip = f_ip
        self.process = None
        self.processstate = None
        self.watchdog = None
        self.runnumber = None
        self.assigned_run_dir = None
        self.resource_lock = resource_lock
        self.retry_attempts = 0
        self.quarantined = []

    def ping(self):
        if conf.role == 'bu':
            if not os.system("ping -c 1 "+self.cpu[0])==0: pass #self.hoststate = 0

    def NotifyNewRunStart(self,runnumber):
        self.runnumber = runnumber
        self.notifyNewRunThread = threading.Thread(target=self.NotifyNewRun,args=[runnumber])
        self.notifyNewRunThread.start()

    def NotifyNewRunJoin(self):
        self.notifyNewRunThread.join()
        self.notifyNewRunThread=None

    def NotifyNewRun(self,runnumber):
        self.runnumber = runnumber
        self.logger.info("calling start of run on "+self.cpu[0])
        attemptsLeft=3
        while attemptsLeft>0:
            attemptsLeft-=1
            try:
                if self.hostip: resaddr = self.hostip
                else: resaddr = self.cpu[0]
                connection = httplib.HTTPConnection(resaddr, conf.cgi_port - conf.cgi_instance_port_offset,timeout=10)
                connection.request("GET",'cgi-bin/start_cgi.py?run='+str(runnumber))
                response = connection.getresponse()
                #do something intelligent with the response code
                self.logger.info("response was "+str(response.status))
                if response.status > 300: pass #self.hoststate = 1
                else:
                    self.logger.info(response.read())
                break
            except Exception as ex:
                if attemptsLeft>0:
                    self.logger.error('RUN:'+str(self.runnumber)+' - '+str(ex) + ' contacting '+str(self.cpu[0]))
                    self.logger.info('retrying connection to '+str(self.cpu[0]))
                else:
                    self.logger.error('RUN:'+str(self.runnumber)+' - exhausted attempts to contact '+str(self.cpu[0]))
                    self.logger.exception(ex)

    def NotifyShutdown(self):
        try:
            if self.hostip: resaddr = self.hostip
            else: resaaddr = self.cpu[0]
            connection = httplib.HTTPConnection(resaddr, conf.cgi_port - conf.cgi_instance_port_offset,timeout=5)
            connection.request("GET",'cgi-bin/stop_cgi.py?run='+str(self.runnumber))
            time.sleep(0.05)
            response = connection.getresponse()
            time.sleep(0.05)
            #do something intelligent with the response code
            #if response.status > 300: self.hoststate = 0
        except Exception as ex:
            self.logger.exception(ex)

    def StartNewProcess(self, runnumber, input_disk, arch, version, menu, transfermode, num_threads, num_streams):
        self.logger.debug("OnlineResource: StartNewProcess called")
        self.runnumber = runnumber

        """
        this is just a trick to be able to use two
        independent mounts of the BU - it should not be necessary in due course
        IFF it is necessary, it should address "any" number of mounts, not just 2
        """
        inputdirpath = os.path.join(input_disk,'run'+str(runnumber).zfill(conf.run_number_padding))
        #run_dir = input_disk + '/run' + str(self.runnumber).zfill(conf.run_number_padding)
        self.logger.info("starting process with "+version+" and run number "+str(runnumber)+ ' threads:'+str(num_threads)+' streams:'+str(num_streams))

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
                            self.resInfo.used+self.cpu[0]]
            if self.watchdog:
                new_run_args.append('skipFirstLumis=True')
            if runkey:
                new_run_args.append('runkey={0}'.format(runkey))
            else:
                logging.info('Not able to determine the DQM run key from the "global" file. Default value from the input source will be used.')

        try:
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
            self.logger.info("arg array "+str(new_run_args).translate(None, "'")+' started with pid '+str(self.process.pid))
        except Exception as ex:
            self.logger.warning("OnlineResource: exception encountered in forking hlt slave")
            self.logger.warning(ex)
        try:
            if self.watchdog:
                #release lock while joining thread to let it complete
                self.resource_lock.release()
                self.watchdog.join()
                self.resource_lock.acquire()

            self.processstate = 100
            self.watchdog = ProcessWatchdog(self,inputdirpath)
            self.watchdog.start()
            self.logger.debug("watchdog thread restarted for "+str(self.process.pid)+" is alive " + str(self.watchdog.is_alive()))

        except Exception as ex:
            self.logger.warning("OnlineResource: exception encountered in watching hlt slave")
            self.logger.warning(ex)

    def join(self):
        self.logger.debug('calling join on thread ' +self.watchdog.name)
        self.watchdog.join()

    def clearQuarantined(self,doLock=True,restore=True):
        retq=[]
        if not restore:
            self.resInfo.q_list+=self.quarantined
            return self.quarantined
        if doLock:self.resource_lock.acquire()
        try:
            for cpu in self.quarantined:
                self.logger.info('Clearing quarantined resource '+cpu)
                self.resInfo.resmove(self.resInfo.quarantined,self.resInfo.idles,cpu)
                retq.append(cpu)
            self.quarantined = []
            self.parent.n_used=0
            self.parent.n_quarantined=0
        except Exception as ex:
            self.logger.exception(ex)
        if doLock:self.resource_lock.release()
        return retq

    def moveUsedToIdles(self):
        self.resource_lock.acquire()
        try:
            for cpu in self.cpu:
                try:
                    self.resInfo.resmove(self.resInfo.used,self.resInfo.idles,cpu)
                    self.parent.n_used-=1
                except Exception as ex:
                    self.logger.warning('problem moving core ' + cpu + ' from used to idle:'+str(ex))
        finally:
            self.resource_lock.release()

    def moveUsedToQuarantined(self):
        self.resource_lock.acquire()
        try:
            for cpu in self.cpu:
                try:
                    self.resInfo.resmove(self.resInfo.used,self.resInfo.quarantined,cpu)
                    self.quarantined.append(cpu)
                    self.parent.n_quarantined+=1
                except Exception as ex:
                    self.logger.warning('problem moving core ' + cpu + ' from used to quarantined:'+str(ex))
        finally:
            self.resource_lock.release()

    def moveUsedToBroken(self):
        self.resource_lock.acquire()
        try:
            for cpu in self.cpu:
                try:
                    self.resInfo.resmove(self.resInfo.used,self.resInfo.broken,cpu)
                    self.parent.n_used-=1
                except Exception as ex:
                    self.logger.warning('problem moving core ' + cpu + ' from used to except:'+str(ex))
        finally:
            self.resource_lock.release()

class ProcessWatchdog(threading.Thread):
    def __init__(self,resource,inputdirpath):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.resource = resource
        self.inputdirpath=inputdirpath
        self.retry_limit = conf.process_restart_limit
        self.retry_delay = conf.process_restart_delay_sec
        self.quarantined = False

    def run(self):
        try:
            self.logger.info('watchdog thread for process '+str(self.resource.process.pid) + ' on resource '+str(self.resource.cpu)+" for run "+str(self.resource.runnumber) + ' started ')
            self.resource.process.wait()
            returncode = self.resource.process.returncode
            pid = self.resource.process.pid

            #update json process monitoring file
            self.resource.processstate=returncode

            outdir = self.resource.assigned_run_dir
            abortedmarker = os.path.join(outdir,RunCommon.ABORTED)
            stoppingmarker = os.path.join(outdir,RunCommon.STOPPING)
            abortcompletemarker = os.path.join(outdir,RunCommon.ABORTCOMPLETE)
            completemarker = os.path.join(outdir,RunCommon.COMPLETE)
            rnsuffix = str(self.resource.runnumber).zfill(conf.run_number_padding)

            if os.path.exists(abortedmarker):
                #abort issued
                self.resource.moveUsedToIdles()
                return

            #input dir check if cmsRun can not find the input
            inputdir_exists = os.path.exists(self.inputdirpath)
            configuration_reachable = False if conf.dqm_machine==False and returncode==90 and not inputdir_exists else True

            if conf.dqm_machine==False and returncode==90 and inputdir_exists:
                if not os.path.exists(os.path.join(self.inputdirpath,'hlt','HltConfig.py')):
                    self.logger.error('RUN:'+str(self.resource.runnumber)+" - input run dir exists, but " + str(os.path.join(self.inputdirpath,'hlt','HltConfig.py')) + " is not present (cmsRun exit code 90)")
                    configuration_reachable=False

            #cleanup actions- remove process from list and attempt restart on same resource
            if returncode != 0 and returncode!=None and configuration_reachable:

                #bump error count in active_runs_errors which is logged in the box file
                self.resource.parent.num_errors+=1

                if returncode < 0:
                    self.logger.error('RUN:' + str(self.resource.runnumber)+" - process "+str(pid)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with signal "
                              +str(returncode) + ', retries left: '+str(self.retry_limit-self.resource.retry_attempts)
                              )
                else:
                    self.logger.error('RUN:'+str(self.resource.runnumber)+" - process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with code "
                              +str(returncode) +', retries left: '+str(self.retry_limit-self.resource.retry_attempts)
                              )
                #quit codes (configuration errors):
                #removed 65 because it is not only configuration error
                quit_codes = [127,90,73]

                #dqm mode will treat configuration error as a crash and eventually move to quarantined
                if conf.dqm_machine==False and returncode in quit_codes:
                    if self.resource.retry_attempts < self.retry_limit:

                        self.logger.warning('for this type of error, restarting this process is disabled')
                        self.resource.retry_attempts=self.retry_limit
                    if returncode==127:
                        self.logger.fatal('RUN:'+str(self.resource.runnumber)+ ' - exit code indicates that CMSSW environment might not be available (cmsRun executable not in path).')
                    elif returncode==90:
                        self.logger.fatal('RUN:'+str(self.resource.runnumber)+ ' - exit code indicates that there might be a python error in the CMSSW configuration.')
                    else:
                        self.logger.fatal('RUN:'+str(self.resource.runnumber)+ ' - exit code indicates that there might be a C/C++ error in the CMSSW configuration.')

                #generate crashed pid json file like: run000001_ls0000_crash_pid12345.jsn
                oldpid = "pid"+str(pid).zfill(5)
                runnumber = "run"+str(self.resource.runnumber).zfill(conf.run_number_padding)
                ls = "ls0000"
                filename = "_".join([runnumber,ls,"crash",oldpid])+".jsn"
                filepath = os.path.join(outdir,filename)
                document = {"errorCode":returncode}
                try:
                    with open(filepath,"w+") as fi:
                        json.dump(document,fi)
                except: self.logger.exception("unable to create %r" %filename)
                self.logger.info("pid crash file: %r" %filename)


                if self.resource.retry_attempts < self.retry_limit:
                    """
                    sleep a configurable amount of seconds before
                    trying a restart. This is to avoid 'crash storms'
                    """
                    time.sleep(self.retry_delay)

                    self.resource.process = None
                    self.resource.retry_attempts += 1

                    self.logger.info("try to restart process for resource(s) "
                                 +str(self.resource.cpu) + " attempt " + str(self.resource.retry_attempts))

                    self.resource.moveUsedToBroken()
                    self.logger.debug("resource(s) " +str(self.resource.cpu)+ " successfully moved to except(broken)")

                elif self.resource.retry_attempts >= self.retry_limit:
                    self.logger.info("process for run " + str(self.resource.runnumber)
                                  +" on resources " + str(self.resource.cpu)
                                  +" reached max retry limit ")
                    
                    self.resource.moveUsedToQuarantined()

                    #write quarantined marker for RunRanger
                    try:
                        os.remove(conf.watch_directory+'/quarantined'+rnsuffix)
                    except:
                        pass
                    try:
                        with open(conf.watch_directory+'/quarantined'+rnsuffix,'w+') as fp:
                            self.quarantined = True
                    except Exception as ex:
                        self.logger.exception(ex)

            #successful end= release resource (TODO:maybe should mark aborted for non-0 error codes)
            elif returncode == 0 or returncode == None or not configuration_reachable:

                if not configuration_reachable:
                    self.logger.info('pid '+str(pid)+' exit 90 (input directory and menu missing) from run ' + str(self.resource.runnumber) + ' - releasing resource ' + str(self.resource.cpu))
                else:
                    self.logger.info('pid '+str(pid)+' exit 0 from run ' + str(self.resource.runnumber) + ' - releasing resource ' + str(self.resource.cpu))

                # generate an end-of-run marker if it isn't already there - it will be picked up by the RunRanger
                endmarker = conf.watch_directory+'/end'+rnsuffix
                if not os.path.exists(endmarker):
                    with open(endmarker,'w+') as fp:
                        pass

                count=0
                # wait until the request to end has been handled
                while not os.path.exists(stoppingmarker):
                    if os.path.exists(completemarker):
                        break
                    if os.path.exists(abortedmarker) or os.path.exists(abortcompletemarker):
                        self.logger.warning('quitting watchdog thread because run ' + str(self.resource.runnumber) + ' has been aborted ( pid' + str(pid) + ' resource' + str(self.resource.cpu) + ')')
                        break
                    if not os.path.exists(outdir):
                        self.logger.warning('quitting watchdog thread because run directory ' + outdir  + ' has disappeared ( pid' + str(pid) + ' resource' + str(self.resource.cpu) + ')')
                        break
                    time.sleep(.1)
                    count+=1
                    if count>=100 and count%100==0:
                        self.logger.warning("still waiting for complete marker for run "+str(self.resource.runnumber) + ' in watchdog for resource '+str(self.resource.cpu))

                # release resources for this case
                self.resource.moveUsedToIdles()

            #self.logger.info('exiting watchdog thread for '+str(self.resource.cpu))

        except Exception as ex:
            self.logger.info("OnlineResource watchdog: exception")
            self.logger.exception(ex)
            try:self.resource.resource_lock.release()
            except:pass
        return


