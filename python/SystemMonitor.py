import os
import time
import subprocess
import threading
import simplejson as json
import datetime
import logging

from aUtils import ES_DIR_NAME
from elasticbu import elasticBandBU

class system_monitor(threading.Thread):

    def __init__(self,confClass,stateInfo,resInfo,runList,mountMgr,boxInfo,indexCreator):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.create_file=True
        self.threadEvent = threading.Event()
        self.threadEventStat = threading.Event()
        self.threadEventESBox = threading.Event()
        self.statThread = None
        self.esBoxThread = None
        self.stale_flag = False
        self.highest_run_number = None
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.mm = mountMgr
        self.boxInfo = boxInfo
        self.indexCreator = indexCreator
        global conf
        conf = confClass
        self.rehash()
        if conf.mount_control_path:
            self.startStatNFS()
        #start direct injection into central index (fu role)
        if conf.use_elasticsearch == True:
            self.startESBox()

    def rehash(self):
        if conf.role == 'fu':
            self.check_directory = [os.path.join(x,'appliance','dn') for x in self.mm.bu_disk_list_ramdisk_instance]
            #write only in one location
            if conf.mount_control_path:
                self.logger.info('Updating box info via control interface')
                self.directory = [os.path.join(self.mm.bu_disk_ramdisk_CI_instance,'appliance','boxes')]
            else:
                self.logger.info('Updating box info via data interface')
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

        self.logger.info("rehash found the following BU disk(s):"+str(self.file))
        for disk in self.file:
            self.logger.info(disk)

    def startESBox(self):
        if conf.role == "fu":
            self.esBoxThread = threading.Thread(target=self.runESBox)
            self.esBoxThread.start()

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
                        self.logger.fatal('detected stale file handle: '+str(disk))
                else:
                    self.logger.warning('stat mountpoint ' + str(disk) + ' caught Error: '+str(ex))
                fu_stale_counter+=1
                err_detected=True
            except Exception as ex:
                err_detected=True
                self.logger.warning('stat mountpoint ' + str(disk) + ' caught exception: '+str(ex))

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
                            self.logger.warning('unable to update '+mfile+ ' : '+str(ex))
                    else:
                        self.logger.error('update file ' + mfile + ' caught Error:'+str(ex))
                except Exception as ex:
                    err_detected = True
                    self.logger.error('update file ' + mfile + ' caught exception:'+str(ex))

            #measure time needed to do these actions. stale flag is set if it takes more than 10 seconds
            stat_time_delta = time.time()-time_start
            if stat_time_delta>5:
                if conf.mount_control_path:
                    self.logger.warning("unusually long time ("+str(stat_time_delta)+"s) was needed to perform file handle and boxinfo stat check")
                else:
                    self.logger.warning("unusually long time ("+str(stat_time_delta)+"s) was needed to perform stale file handle check")
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
            self.logger.debug('entered system monitor thread ')
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
                if self.state.suspended: continue
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
                        self.logger.info('incorrect ramdisk occupancy:' + str(ramdisk_occ))
                    if ramdisk_occ>1:
                        ramdisk_occ=1
                        self.logger.info('incorrect ramdisk occupancy:' + str(ramdisk_occ))

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
                                    self.logger.warning('box file version mismatch from '+str(key)+' got:'+str(edata['version'])+' required:'+str(self.boxInfo.boxdoc_version))
                                    continue
                            except:
                                self.logger.warning('box file version for '+str(key)+' not found')
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
                            self.logger.warning('problem updating boxinfo summary: '+str(ex))
                        try:
                            lastFURuns.append(edata['activeRuns'][-1])
                        except:pass
                    if len(stale_machines) and counter==1:
                        self.logger.warning("detected stale box resources: "+str(stale_machines))
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
                            d_used = ((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)>>20
                            d_total =  (dirstat.f_blocks*dirstat.f_bsize)>>20
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

                            cloud_state = self.getCloudState()

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
                            self.logger.warning('boxinfo file write failed :'+str(ex))
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
                            self.logger.warning('exception on boxinfo file write failed : +'+str(ex))

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
            self.logger.exception(ex)

        for mfile in self.file:
            try:
                os.remove(mfile)
            except OSError:
                pass

        self.logger.debug('exiting system monitor thread ')

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

    def getCPUInfo(self):
        try:
            cpu_name = ""
            cpu_freq = 0.
            cpu_cores = 0
            cpu_siblings = 0
            with open('/proc/cpuinfo','r') as fi:
              for line in fi.readlines():
                if line.startswith("model name") and not cpu_name:
                    for word in line[line.find(':')+1:].split():
                      if word=='' or '(R)' in word  or '(TM)' in word or 'CPU' in word or '@' in word :continue
                      if 'GHz' in word: cpu_freq = float(word[:word.find('GHz')])
                      else:
                        if cpu_name: cpu_name = cpu_name+" "+word
                        else: cpu_name=word

                if line.startswith("siblings") and not cpu_siblings:
                    cpu_siblings = int(line.split()[-1])
                if line.startswith("cpu cores") and not cpu_cores:
                    cpu_cores = int(line.split()[-1])
            return cpu_name,cpu_freq,cpu_cores,cpu_siblings
        except:
            return "",0.,0,0


    def runESBox(self):

        #find out BU name from bus_config
        self.logger.info("started ES box thread")
        #parse bus.config to find BU name 
        bu_name="unknown"
        bus_config = os.path.join(os.path.dirname(conf.resource_base.rstrip(os.path.sep)),'bus.config')
        try:
            if os.path.exists(bus_config):
                for line in open(bus_config,'r'):
                    bu_name=line.split('.')[0]
                    break
        except:pass

        cpu_name,cpu_freq,cpu_cores,cpu_siblings = self.getCPUInfo()

        self.threadEventESBox.wait(1)
        eb = elasticBandBU(conf,0,'',False,update_run_mapping=False,update_box_mapping=True)
        while self.running:
            try:
                dirstat = os.statvfs('/')
                d_used = ((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)>>20
                d_total =  (dirstat.f_blocks*dirstat.f_bsize)>>20
                dirstat_var = os.statvfs('/var')
                d_used_var = ((dirstat_var.f_blocks - dirstat_var.f_bavail)*dirstat_var.f_bsize)>>20
                d_total_var =  (dirstat_var.f_blocks*dirstat_var.f_bsize)>>20
                doc = {
                    "date":datetime.datetime.utcfromtimestamp(time.time()).isoformat(),
                    "appliance":bu_name,
                    "cpu_name":cpu_name,
                    "cpu_GHz":cpu_freq,
                    "cpu_phys_cores":cpu_cores,
                    "cpu_hyperthreads":cpu_siblings,
                    "cloudState":self.getCloudState(),
                    "activeRunList":self.runList.getActiveRunNumbers(),
                    "usedDisk":d_used,
                    "totalDisk":d_total,
                    "diskOccupancy":d_used/(1.*d_total) if d_total>0 else 0.,
                    "usedDiskVar":d_used_var,
                    "totalDiskVar":d_total_var,
                    "diskVarOccupancy":d_used_var/(1.*d_total_var) if d_total_var>0 else 0.
                }
                    #TODO: CPU info(cores,type, usage), RAM info(usage,full), net traffic, disk traffic(iostat)
                    #see: http://stackoverflow.com/questions/1296703/getting-system-status-in-python
                eb.elasticize_fubox(doc)
            except Exception as ex:
                self.logger.exception(ex)
            try:
                self.threadEventESBox.wait(5)
            except:
                self.logger.info("Interrupted ESBox thread - ending")
                break
        del eb


    def getCloudState(self):
        cloud_st = "off"
        if self.state.cloud_mode:
          if self.state.entering_cloud_mode: cloud_st="starting"
          elif self.state.exiting_cloud_mode:cloud_st="stopping"
          else: cloud_st="on"
        elif self.state.resources_blocked_flag:
            cloud_st = "resourcesReleased"
        elif self.state.masked_resources:
            cloud_st = "resourcesMasked"
        else:
            cloud_st = "off"
        return cloud_st


    def stop(self):
        self.logger.debug("request to stop")
        self.running = False
        self.threadEvent.set()
        self.threadEventStat.set()
        self.threadEventESBox.set()
        if self.statThread:
            self.statThread.join()
        if self.esBoxThread:
            self.esBoxThread.join()


