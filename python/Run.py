import os
import sys
import shutil
import time
import simplejson as json
import subprocess
import threading
import datetime
import demote
import prctl
from signal import SIGKILL
import logging

import Resource
from HLTDCommon import updateBlacklist,dqm_globalrun_filepattern
from setupES import setupES

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

class RunList:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
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
            self.logger.error("Multiple runs entries for "+str(runNumber)+" were found while removing run")
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


class Run:

    def __init__(self,nr,dirname,bu_dir,instance,confClass,stateInfo,resInfo,runList,rr,mountMgr,nsslock,resource_lock):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.pending_shutdown=False
        self.is_ongoing_run=True
        self.num_errors = 0

        self.runnumber = nr
        self.dirname = dirname
        self.instance = instance
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.rr = rr
        self.mm = mountMgr
        self.nsslock = nsslock
        self.resource_lock = resource_lock

        global conf
        conf = confClass
        self.conf = conf

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
        self.runShutdown = None
        self.threadEvent = threading.Event()
        self.stopThreads = False

        #stats on usage of resources
        self.n_used = 0
        self.n_quarantined = 0

        self.inputdir_exists = False

        if conf.role == 'fu':
            self.changeMarkerMaybe(Resource.RunCommon.STARTING)
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
                        self.logger.info("Run " + str(self.runnumber) + " uses " + self.version + " ("+self.arch + ") with " + str(conf.menu_name) + ' transferDest:'+self.transfermode)
                    break

                except ValueError as ex:
                    if readMenuAttempts>50:
                        self.logger.exception(ex)
                        break
                except Exception as ex:
                    if readMenuAttempts>50:
                        self.logger.exception(ex)
                        break

            else:
                if readMenuAttempts>50:
                    if not os.path.exists(bu_dir):
                        self.logger.info("FFF parameter or HLT menu files not found in ramdisk - BU run directory is gone")
                    else:
                        self.logger.error("FFF parameter or HLT menu files not found in ramdisk")
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
                self.logger.warning("Using default values for run " + str(self.runnumber) + ": " + self.version + " (" + self.arch + ") with " + self.menu_path)

        #give this command line parameter quoted in case it is empty
        if len(self.transfermode)==0:
            self.transfermode='null'

        #backup HLT menu and parameters
        if conf.role=='fu':
            try:
                hltTargetName = 'HltConfig.py_run'+str(self.runnumber)+'_'+self.arch+'_'+self.version+'_'+self.transfermode
                shutil.copy(self.menu_path,os.path.join(conf.log_dir,'pid',hltTargetName))
            except:
                self.logger.warning('Unable to backup HLT menu')

        self.rawinputdir = None
        #
        if conf.role == "bu":
            try:
                self.rawinputdir = conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
                os.stat(self.rawinputdir)
                self.inputdir_exists = True
            except Exception, ex:
                self.logger.error("failed to stat "+self.rawinputdir)
            try:
                os.mkdir(self.rawinputdir+'/mon')
            except Exception, ex:
                self.logger.error("could not create mon dir inside the run input directory")
        else:
            self.rawinputdir= os.path.join(self.mm.bu_disk_list_ramdisk_instance[0],'run' + str(self.runnumber).zfill(conf.run_number_padding))

        #verify existence of the input directory
        if conf.role=='fu':
            if not paramsDetected and conf.dqm_machine==False:
                try:
                    os.stat(self.rawinputdir)
                    self.inputdir_exists = True
                except:
                    self.logger.warning("unable to stat raw input directory for run "+str(self.runnumber))
                    return
            else:
                self.inputdir_exists = True

        if conf.use_elasticsearch == True:
            try:
                if conf.role == "bu":
                    self.nsslock.acquire()
                    self.logger.info("starting elasticbu.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elasticbu.py',self.instance,str(self.runnumber)]
                else:
                    self.logger.info("starting elastic.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elastic.py',self.dirname,self.rawinputdir+'/mon',str(self.resInfo.expected_processes)]

                self.elastic_monitor = subprocess.Popen(elastic_args,
                                                        preexec_fn=preexec_function,
                                                        close_fds=True
                                                        )
            except OSError as ex:
                self.logger.error("failed to start elasticsearch client")
                self.logger.error(ex)
            try:self.nsslock.release()
            except:pass
        if conf.role == "fu" and conf.dqm_machine==False:
            try:
                self.logger.info("starting anelastic.py with arguments:"+self.dirname)
                elastic_args = ['/opt/hltd/python/anelastic.py',self.dirname,str(self.runnumber), self.rawinputdir,self.mm.bu_disk_list_output_instance[0]]
                self.anelastic_monitor = subprocess.Popen(elastic_args,
                                                    preexec_fn=preexec_function,
                                                    close_fds=True
                                                    )
            except OSError as ex:
                self.logger.fatal("failed to start anelastic.py client:")
                self.logger.exception(ex)
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
        if self.runShutdown:
            self.joinShutdown()

        self.logger.info('Run '+ str(self.runnumber) +' object __del__ has completed')

    def countOwnedResourcesFrom(self,resourcelist):
        ret = 0
        try:
            for p in self.online_resource_list:
                for c in p.cpu:
                    for resourcename in resourcelist:
                        if resourcename == c:
                            ret+=1
        except:pass
        return ret

    def AcquireResource(self,resourcenames,fromstate):
        fromDir = conf.resource_base+'/'+fromstate+'/'
        try:
            self.logger.debug("Trying to acquire resource "
                          +str(resourcenames)
                          +" from "+fromstate)

            for resourcename in resourcenames:
                self.resInfo.resmove(fromDir,self.resInfo.used,resourcename)
                self.n_used+=1
            #TODO:fix core pairing with resource.cpu list (otherwise - restarting will not work properly)
            if not filter(lambda x: sorted(x.cpu)==sorted(resourcenames),self.online_resource_list):
                self.logger.debug("resource(s) "+str(resourcenames)
                              +" not found in online_resource_list, creating new")
                self.online_resource_list.append(Resource.OnlineResource(self,resourcenames,self.resource_lock))
                return self.online_resource_list[-1]
            self.logger.debug("resource(s) "+str(resourcenames)
                          +" found in online_resource_list")
            return filter(lambda x: sorted(x.cpu)==sorted(resourcenames),self.online_resource_list)[0]
        except Exception as ex:
            self.logger.info("exception encountered in looking for resources")
            self.logger.info(ex)

    def MatchResource(self,resourcenames):
        for res in self.online_resource_list:
            #first resource in the list is the one that triggered inotify event
            if resourcenames[0] in res.cpu:
                found_all = True
                for name in res.cpu:
                    if name not in resourcenames:
                        found_all = False
                if found_all:
                    return res.cpu
        return None

    def ContactResource(self,resourcename):
        self.online_resource_list.append(Resource.OnlineResource(self,resourcename,self.resource_lock))
        #self.online_resource_list[-1].ping() #@@MO this is not doing anything useful, afaikt

    def ReleaseResource(self,res):
        self.online_resource_list.remove(res)

    def AcquireResources(self,mode):
        self.logger.info("acquiring resources from "+conf.resource_base)
        res_dir = self.resInfo.idles if conf.role == 'fu' else os.path.join(conf.resource_base,'boxes')
        try:
            dirlist = os.listdir(res_dir)
            self.logger.info(str(dirlist))
        except Exception as ex:
            self.logger.info("exception encountered in looking for resources")
            self.logger.info(ex)
        current_time = time.time()
        count = 0
        cpu_group=[]
        #self.lock.acquire()

        bldir = os.path.join(self.dirname,'hlt')
        blpath = os.path.join(self.dirname,'hlt','blacklist')
        if conf.role=='bu':
            attempts=100
            while not os.path.exists(bldir) and attempts>0:
                time.sleep(0.05)
                attempts-=1
                if attempts<=0:
                    self.logger.error('Timeout waiting for directory '+ bldir)
                    break
            if os.path.exists(blpath):
                update_success,self.rr.boxInfo.machine_blacklist=updateBlacklist(conf,self.logger,blpath)
            else:
                self.logger.error("unable to find blacklist file in "+bldir)

        for cpu in dirlist:
            #skip self
            if conf.role=='bu':
                if cpu == os.uname()[1]:continue
                if cpu in self.rr.boxInfo.machine_blacklist:
                    self.logger.info("skipping blacklisted resource "+str(cpu))
                    continue
                if self.checkStaleResourceFile(os.path.join(res_dir,cpu)):
                    self.logger.error("Skipping stale resource "+str(cpu))
                    continue

            count = count+1
            try:
                age = current_time - os.path.getmtime(os.path.join(res_dir,cpu))
                cpu_group.append(cpu)
                if conf.role == 'fu':
                    if count == self.resInfo.nstreams:
                        self.AcquireResource(cpu_group,'idle')
                        cpu_group=[]
                        count=0
                else:
                    self.logger.info("found resource "+cpu+" which is "+str(age)+" seconds old")
                    if age < 10:
                        cpus = [cpu]
                        self.ContactResource(cpus)
            except Exception as ex:
                self.logger.error('encountered exception in acquiring resource '+str(cpu)+':'+str(ex))
        return True
        #self.lock.release()

    def checkStaleResourceFile(self,resourcepath):
        try:
            with open(resourcepath,'r') as fi:
                doc = json.load(fi)
                if doc['detectedStaleHandle']==True:
                    return True
        except:
            time.sleep(.05)
            try:
                with open(resourcepath,'r') as fi:
                    doc = json.load(fi)
                    if doc['detectedStaleHandle']==True:
                        return True
            except:
                self.logger.warning('can not parse ' + str(resourcepath))
        return False

    def CheckTemplate(self,run=None):
        if conf.role=='bu' and conf.use_elasticsearch and conf.update_es_template:
            self.logger.info("checking ES template")
            try:
                #new: try to create index with template mapping after template check
                new_index_name = 'run'+str(self.runnumber)+'_'+conf.elastic_cluster
                setupES(es_server_url='http://'+conf.es_local+':9200',forceReplicas=conf.force_replicas,forceShards=conf.force_shards,create_index_name=new_index_name,subsystem=conf.elastic_runindex_name)
            except Exception as ex:
                self.logger.error("Unable to check run appliance template:"+str(ex))

    def Start(self):
        self.is_ongoing_run = True
        #create mon subdirectory before starting
        try:
            os.makedirs(os.path.join(self.dirname,'mon'))
        except OSError:
            pass
        #start/notify run for each resource
        if conf.role == 'fu':
            for resource in self.online_resource_list:
                self.logger.info('start run '+str(self.runnumber)+' on cpu(s) '+str(resource.cpu))
                self.StartOnResource(resource)

            if conf.dqm_machine==False:
                self.changeMarkerMaybe(Resource.RunCommon.ACTIVE)
                #start safeguard monitoring of anelastic.py
                self.startAnelasticWatchdog()

        elif conf.role == 'bu':
            for resource in self.online_resource_list:
                self.logger.info('start run '+str(self.runnumber)+' on resources '+str(resource.cpu))
                resource.NotifyNewRunStart(self.runnumber)
            #update begin time at this point
            self.beginTime = datetime.datetime.now()
            for resource in self.online_resource_list:
                resource.NotifyNewRunJoin()
            self.logger.info('sent start run '+str(self.runnumber)+' notification to all resources')

            self.startElasticBUWatchdog()
            self.startCompletedChecker()

    def maybeNotifyNewRun(self,resourcename,resourceage):
        if conf.role=='fu':
            self.logger.fatal('this function should *never* have been called when role == fu')
            return

        if self.rawinputdir != None:
            #TODO:check also for EoR file?
            try:
                os.stat(self.rawinputdir)
            except:
                self.logger.warning('Unable to find raw directory of '+str(self.runnumber))
                return None

        for resource in self.online_resource_list:
            if resourcename in resource.cpu:
                self.logger.error('Resource '+str(resource.cpu)+' was already processing run ' + str(self.runnumber) + '. Will not participate in this run.')
                return None
            if resourcename in self.rr.boxInfo.machine_blacklist:
                self.logger.info("skipping blacklisted resource "+str(resource.cpu))
                return None
        current_time = time.time()
        age = current_time - resourceage
        self.logger.info("found resource "+resourcename+" which is "+str(age)+" seconds old")
        if age < 10:
            self.ContactResource([resourcename])
            return self.online_resource_list[-1]
        else:
            return None

    def StartOnResource(self, resource):
        self.logger.debug("StartOnResource called")
        resource.assigned_run_dir=conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
        new_index = self.online_resource_list.index(resource)%len(self.mm.bu_disk_list_ramdisk_instance)
        resource.StartNewProcess(self.runnumber,
                                 self.mm.bu_disk_list_ramdisk_instance[new_index],
                                 self.arch,
                                 self.version,
                                 self.menu_path,
                                 self.transfermode,
                                 int(round((len(resource.cpu)*float(self.resInfo.nthreads)/self.resInfo.nstreams))),
                                 len(resource.cpu))
        self.logger.debug("StartOnResource process started")


    def Stop(self):
        #used to gracefully stop CMSSW and finish scripts
        with open(os.path.join(self.dirname,"temp_CMSSW_STOP"),'w') as f:
            writedoc = {}
            bu_lumis = []
            try:
                bu_eols_files = filter(lambda x: x.endswith("_EoLS.jsn"),os.listdir(self.rawinputdir))
                bu_lumis = (sorted([int(x.split('_')[1][2:]) for x in bu_eols_files]))
            except:
                self.logger.error("Unable to parse BU EoLS files")
            ls_delay=3
            if len(bu_lumis):
                self.logger.info('last closed lumisection in ramdisk is '+str(bu_lumis[-1])+', requesting to close at LS '+ str(bu_lumis[-1]+ls_delay))
                writedoc['lastLS']=bu_lumis[-1]+ls_delay #current+delay
            else:  writedoc['lastLS']=ls_delay
            json.dump(writedoc,f)
        try:
            os.rename(os.path.join(self.dirname,"temp_CMSSW_STOP"),os.path.join(self.dirname,"CMSSW_STOP"))
        except:pass

    def startShutdown(self,killJobs=False,killScripts=False):
        self.runShutdown = threading.Thread(target=self.Shutdown,args=[killJobs,killScripts])
        self.runShutdown.start()

    def joinShutdown(self):
        if self.runShutdown:
            try:
                self.runShutdown.join()
            except:
                return

    def Shutdown(self,killJobs=False,killScripts=False):
        #herod mode sends sigkill to all process, however waits for all scripts to finish
        self.logger.info("run"+str(self.runnumber)+": Shutdown called")
        self.pending_shutdown=False
        self.is_ongoing_run = False

        try:
            self.changeMarkerMaybe(Resource.RunCommon.ABORTED)
        except OSError as ex:
            pass

        time.sleep(.1)
        try:
            for resource in self.online_resource_list:
                if resource.processstate==100:
                    try:
                        self.logger.info('terminating process '+str(resource.process.pid)+
                                 ' in state '+str(resource.processstate)+' owning '+str(resource.cpu))

                        if killJobs:resource.process.kill()
                        else:resource.process.terminate()
                    except AttributeError:
                        pass
                    if resource.watchdog!=None and resource.watchdog.is_alive():
                        try:
                            resource.join()
                        except:
                            pass
                    try:
                        self.logger.info('process '+str(resource.process.pid)+' terminated')
                    except AttributeError:
                        self.logger.info('terminated process (in another thread)')
                    time.sleep(.1)
                    self.logger.info(' releasing resource(s) '+str(resource.cpu))

            self.resource_lock.acquire()
            q_clear_condition = (not self.checkQuarantinedLimit()) or conf.auto_clear_quarantined
            for resource in self.online_resource_list:
                cleared_q = resource.clearQuarantined(doLock=False,restore=q_clear_condition)
                for cpu in resource.cpu:
                    if cpu not in cleared_q:
                        try:
                            self.resInfo.resmove(self.resInfo.used,self.resInfo.idles,cpu)
                            self.n_used-=1
                        except OSError:
                            #@SM:can happen if it was quarantined
                            self.logger.warning('Unable to find resource '+self.resInfo.used+cpu)
                        except Exception as ex:
                            self.resource_lock.release()
                            raise ex
                resource.process=None
            self.resource_lock.release()
            self.logger.info('completed clearing resource list')

            self.online_resource_list = []
            try:
                self.changeMarkerMaybe(Resource.RunCommon.ABORTCOMPLETE)
            except OSError as ex:
                pass
            try:
                if self.anelastic_monitor:
                    if killScripts:
                        self.anelastic_monitor.terminate()
                    self.anelastic_monitor.wait()
            except OSError as ex:
                if ex.errno==3:
                    self.logger.info("anelastic.py for run " + str(self.runnumber) + " is not running")
            except Exception as ex:
                self.logger.exception(ex)
            if conf.use_elasticsearch == True:
                try:
                    if self.elastic_monitor:
                        if killScripts:
                            self.elastic_monitor.terminate()
                        #allow monitoring thread to finish, but no more than 30 seconds after others
                        killtimer = threading.Timer(30., self.elastic_monitor.kill)
                        try:
                            killtimer.start()
                            self.elastic_monitor.wait()
                        finally:
                            killtimer.cancel()
                        try:self.elastic_monitor=None
                        except:pass
                except OSError as ex:
                    if ex.errno==3:
                        self.logger.info("elastic.py for run " + str(self.runnumber) + " is not running")
                    else:self.logger.exception(ex)
                except Exception as ex:
                    self.logger.exception(ex)
            if self.waitForEndThread is not None:
                self.waitForEndThread.join()
        except Exception as ex:
            self.logger.info("exception encountered in shutting down resources")
            self.logger.exception(ex)

        self.resource_lock.acquire()
        try:
            self.runList.remove(self.runnumber)
        except Exception as ex:
            self.logger.exception(ex)
        self.resource_lock.release()

        try:
            if conf.delete_run_dir is not None and conf.delete_run_dir:
                shutil.rmtree(conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding))
            os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
        except:
            pass

        self.logger.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' completed')

    def ShutdownBU(self):
        self.is_ongoing_run = False
        try:
            if self.elastic_monitor:
                #first check if process is alive
                if self.elastic_monitor.poll() is None:
                    self.elastic_monitor.terminate()
                    time.sleep(.1)
        except Exception as ex:
            self.logger.info("exception encountered in shutting down elasticbu.py: " + str(ex))
            #self.logger.exception(ex)

        #should also trigger destructor of the Run

        self.resource_lock.acquire()
        try:
            self.runList.remove(self.runnumber)
        except Exception as ex:
            self.logger.exception(ex)
        self.resource_lock.release()

        self.logger.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' on BU completed')


    def StartWaitForEnd(self):
        self.is_ongoing_run = False
        self.changeMarkerMaybe(Resource.RunCommon.STOPPING)
        try:
            self.waitForEndThread = threading.Thread(target=self.WaitForEnd)
            self.waitForEndThread.start()
        except Exception as ex:
            self.logger.info("exception encountered in starting run end thread")
            self.logger.info(ex)

    def WaitForEnd(self):
        self.logger.info("wait for end thread!")
        try:
            for resource in self.online_resource_list:
                if resource.processstate is not None:
                    if resource.process is not None and resource.process.pid is not None: ppid = resource.process.pid
                    else: ppid="None"
                    self.logger.info('waiting for process '+str(ppid)+
                                 ' in state '+str(resource.processstate) +
                                 ' to complete ')
                    try:
                        resource.join()
                        self.logger.info('process '+str(resource.process.pid)+' completed')
                    except:pass
                resource.clearQuarantined()
                resource.process=None
            self.online_resource_list = []
            if conf.role == 'fu':
                self.logger.info('writing complete file')
                self.changeMarkerMaybe(Resource.RunCommon.COMPLETE)
                try:
                    os.remove(conf.watch_directory+'/end'+str(self.runnumber).zfill(conf.run_number_padding))
                except:pass
                try:
                    if conf.dqm_machine==False:
                        self.anelastic_monitor.wait()
                except OSError,ex:
                    if "No child processes" not in str(ex):
                        self.logger.info("Exception encountered in waiting for termination of anelastic:" +str(ex))
                self.anelastic_monitor = None

            if conf.use_elasticsearch == True:
                try:
                    self.elastic_monitor.wait()
                except OSError,ex:
                    if "No child processes" not in str(ex):
                        self.logger.info("Exception encountered in waiting for termination of anelastic:" +str(ex))
                self.elastic_monitor = None
            if conf.delete_run_dir is not None and conf.delete_run_dir == True:
                try:
                    shutil.rmtree(self.dirname)
                except Exception as ex:
                    self.logger.exception(ex)

            #todo:clear this external thread
            self.resource_lock.acquire()
            self.logger.info("active runs.."+str(self.runList.getActiveRunNumbers()))
            try:
                self.runList.remove(self.runnumber)
            except Exception as ex:
                self.logger.exception(ex)
            self.logger.info("new active runs.."+str(self.runList.getActiveRunNumbers()))

            if self.state.cloud_mode==True:
                if len(self.runList.getActiveRunNumbers())>=1:
                    self.logger.info("VM mode: waiting for runs: " + str(self.runList.getActiveRunNumbers()) + " to finish")
                else:
                    self.logger.info("No active runs. moving all resource files to cloud")
                    #give resources to cloud and bail out
                    self.state.entering_cloud_mode=False
                    #check if cloud mode switch has been aborted in the meantime
                    if self.state.abort_cloud_mode:
                        self.state.abort_cloud_mode=False
                        self.state.resources_blocked_flag=True
                        self.state.cloud_mode=False
                        self.resource_lock.release()
                        return

                    self.resInfo.move_resources_to_cloud()
                    self.resource_lock.release()
                    result = self.state.ignite_cloud()
                    c_status = self.state.cloud_status()
                    if c_status == 0:  self.logger.info("cloud is activated")
                    elif c_status == 1:  self.logger.info("cloud is not activated")
                    else:  self.logger.warning("cloud is in error state:"+str(c_status))

        except Exception as ex:
            self.logger.error("exception encountered in ending run")
            self.logger.exception(ex)
        try:self.resource_lock.release()
        except:pass

    def changeMarkerMaybe(self,marker):
        current = filter(lambda x: x in Resource.RunCommon.VALID_MARKERS, os.listdir(self.dirname))
        if (len(current)==1 and current[0] != marker) or len(current)==0:
            if len(current)==1: os.remove(self.dirname+'/'+current[0])
            fp = open(self.dirname+'/'+marker,'w+')
            fp.close()
        else:
            self.logger.error("There are more than one markers for run "
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
            self.anelasticWatchdog = threading.Thread(target=self.runAnelasticWatchdog)
            self.anelasticWatchdog.start()
        except Exception as ex:
            self.logger.info("exception encountered in starting anelastic watchdog thread")
            self.logger.info(ex)

    def runAnelasticWatchdog(self):
        try:
            self.anelastic_monitor.wait()
            if self.is_ongoing_run == True:
                #abort the run
                self.anelasticWatchdog=None
                self.logger.warning("Premature end of anelastic.py for run "+str(self.runnumber))
                self.Shutdown(killJobs=True,killScripts=True)
        except:
            pass
        self.anelastic_monitor=None

    def startElasticBUWatchdog(self):
        try:
            self.elasticBUWatchdog = threading.Thread(target=self.runElasticBUWatchdog)
            self.elasticBUWatchdog.start()
        except Exception as ex:
            self.logger.info("exception encountered in starting elasticbu watchdog thread")
            self.logger.info(ex)

    def runElasticBUWatchdog(self):
        try:
            self.elastic_monitor.wait()
        except:
            pass
        self.elastic_monitor=None

    def startCompletedChecker(self):

        try:
            self.logger.info('start checking completion of run '+str(self.runnumber))
            self.completedChecker = threading.Thread(target=self.runCompletedChecker)
            self.completedChecker.start()
        except Exception,ex:
            self.logger.error('failure to start run completion checker:')
            self.logger.exception(ex)

    def runCompletedChecker(self):

        rundirstr = 'run'+ str(self.runnumber).zfill(conf.run_number_padding)
        rundirCheckPath = os.path.join(conf.watch_directory, rundirstr)
        eorCheckPath = os.path.join(rundirCheckPath,rundirstr + '_ls0000_EoR.jsn')

        self.threadEvent.wait(10)
        while self.stopThreads == False:
            self.threadEvent.wait(5)
            if os.path.exists(eorCheckPath) or os.path.exists(rundirCheckPath)==False:
                self.logger.info("Completed checker: detected end of run "+str(self.runnumber))
                break

        while self.stopThreads==False:
            self.threadEvent.wait(5)
            success, runFound = self.rr.checkNotifiedBoxes(self.runnumber)
            if success and runFound==False:
                self.resource_lock.acquire()
                try:
                    self.runList.remove(self.runnumber)
                except Exception as ex:
                    self.logger.exception(ex)
                self.resource_lock.release()
                self.logger.info("Completed checker: end of processing of run "+str(self.runnumber))
                break

    def createEmptyEoRMaybe(self):

        #this is used to notify elasticBU to fill the end time before it is terminated
        rundirstr = 'run'+ str(self.runnumber).zfill(conf.run_number_padding)
        rundirCheckPath = os.path.join(conf.watch_directory, rundirstr)
        eorCheckPath = os.path.join(rundirCheckPath,rundirstr + '_ls0000_EoR.jsn')
        try:
            os.stat(eorCheckPath)
        except:
            self.logger.info('creating empty EoR file in run directory '+rundirCheckPath)
            try:
                with open(eorCheckPath,'w') as fi:
                    pass
                time.sleep(.5)
            except Exception as ex:
                self.logger.exception(ex)

