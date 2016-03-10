import os
import time
import subprocess
import threading
import simplejson as json
import datetime
import logging
import psutil
import struct

import getnifs
from aUtils import ES_DIR_NAME
from elasticbu import elasticBandBU

class system_monitor(threading.Thread):

    def __init__(self,confClass,stateInfo,resInfo,runList,mountMgr,boxInfo):
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
        global conf
        conf = confClass
        self.indexCreator = None #placeholder
        #self.rehash()
        #if conf.mount_control_path:
        #    self.startStatNFS()
        #start direct injection into central index (fu role)
        if conf.use_elasticsearch == True:
            self.found_data_interfaces=False
            self.ifs=[]
            self.log_ifconfig=0
            self.startESBox()

    def preStart(self):
        self.rehash()
        if conf.mount_control_path:
            self.startStatNFS()
 

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
            self.esBoxThread.daemon=True #set as daemon thread (not blocking process termination). this should be tested
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
            num_cpu = 1
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
                if line.startswith("physical id"):
                    phys_id = int(line.split()[-1])
                    if phys_id+1>num_cpu: num_cpu=phys_id+1
            return cpu_name,cpu_freq,num_cpu*cpu_cores,num_cpu*cpu_siblings
        except:
            return "",0.,0,0

    def getCPUFreqInfo(self):
      avg=0.
      avg_c = 0
      try:
        #obtain cpu frequencies and get avg (in GHz)
        p = subprocess.Popen('/usr/bin/cpufreq-info | grep "current CPU"', shell=True, stdout=subprocess.PIPE)
        p.wait()
        std_out=p.stdout.readlines()
        for stdl in std_out:
          avg+=float(stdl.strip().split()[4])*1000
          avg_c+=1
      except:pass
      if avg_c == 0:return 0
      else: return int(avg/avg_c)


    def testCPURange(self):
      cnt=0
      while True:
        try: 
          fd = os.open("/dev/cpu/"+str(cnt)+"/msr",os.O_RDONLY)
          os.close(fd)
        except:
          try:os.close(fd)
          except:pass
          return cnt
        cnt+=1

    def getIntelCPUPerfAvgs(self,nthreads):
      tsc=0
      aperf=0
      mperf=0
      cnt=0
      while cnt<nthreads:
        try:
          fd = os.open("/dev/cpu/"+str(cnt)+"/msr",os.O_RDONLY)
          os.lseek(fd,0x10,os.SEEK_SET)
          tsc += struct.unpack("Q",os.read(fd,8))[0]
          os.lseek(fd,0xe7,os.SEEK_SET)
          mperf += struct.unpack("Q",os.read(fd,8))[0]
          os.lseek(fd,0xe8,os.SEEK_SET)
          aperf += struct.unpack("Q",os.read(fd,8))[0]
          cnt+=1
          os.close(fd)
        except OSError as ex:
          self.logger.exception(ex)
          try:os.close(fd)
          except:pass
          return 0,0,0
      return tsc,mperf,aperf


    #def getTurbostatInfo(self):
    #    try:
    #      #turbostat is compiled to take counters for 5ms
    #      p = subprocess.Popen('/opt/hltd/bin/turbostat', shell=False, stderr=subprocess.PIPE)
    #      p.wait()
    #      std_out=p.stderr.readlines()
    #      cnt=0
    #      for stdl in std_out:
    #        if cnt==1:return (int(float(stdl.strip().split()[1])*1000))
    #        cnt+=1
    #    except:
    #      return 0

    def getMEMInfo(self):
        return dict((i.split()[0].rstrip(':'),int(i.split()[1])) for i in open('/proc/meminfo').readlines())

    def findMountInterfaces(self):
        ipaddrs = []
        for line in open('/proc/mounts').readlines():
          mountpoint = line.split()[1]
          if mountpoint.startswith('/fff/'):
            opts = line.split()[3].split(',')
            for opt in opts:
              if opt.startswith('clientaddr='):
                ipaddrs.append(opt.split('=')[1])
        ipaddrs = list(set(ipaddrs))
        ifs = []
        #update list and reset counters only if interface is missing from the previous list 
        if len(ipaddrs)>len(self.ifs):
          self.found_data_interfaces=True
          ifcdict = getnifs.get_network_interfaces()
          for ifc in ifcdict:
            name = ifc.name
            addresses = ifc.addresses
            if 2 in addresses and len(addresses[2]):
              if addresses[2][0] in ipaddrs:
                ifs.append(name)
                if self.log_ifconfig<2:
                  self.logger.info('monitoring '+name)
          ifs = list(set(ifs))
          self.ifs = ifs
          self.ifs_in=0
          self.ifs_out=0
          self.ifs_last = 0
          self.getRatesMBs(silent=self.log_ifconfig<2) #initialize
          self.threadEventESBox.wait(0.1)
        self.log_ifconfig+=1

    def getRatesMBs(self,silent=True):
        try:
          sum_in=0
          sum_out=0
          for ifc in self.ifs:
            sum_in+=int(open('/sys/class/net/'+ifc+'/statistics/rx_bytes').read())
            sum_out+=int(open('/sys/class/net/'+ifc+'/statistics/tx_bytes').read())
          new_time = time.time()
          old_time = self.ifs_last
          delta_t = new_time-self.ifs_last
          self.ifs_last = new_time
          #return 0 if this is first read (last counters=0)
          if self.ifs_in==0 or self.ifs_out==0:
            self.ifs_in = sum_in
            self.ifs_out = sum_out
            return [0,0,new_time-old_time]
          else: 
            self.ifs_in = sum_in
            self.ifs_out = sum_out
            delta_in = ((sum_in - self.ifs_in) / delta_t) / (1024*1024) # Bytes/ms >> 10 == MB/s
            delta_out = ((sum_out - self.ifs_out) / delta_t) /(1024*1024)
            return [delta_in,delta_out,new_time-old_time]
        except Exception as ex:
          if not silent:
            self.logger.exception(ex)
          return [0,0,0]

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
        num_cpus = self.testCPURange() 
        ts_old = time.time()
        if cpu_name.startswith('AMD'):
          tsc_old=mperf_old=aperf_old=tsc_new=mperf_new=aperf_new=0
          is_intel=False
        else:
          tsc_old,mperf_old,aperf_old=self.getIntelCPUPerfAvgs(num_cpus)
          is_intel = True
        self.threadEventESBox.wait(1)
        eb = elasticBandBU(conf,0,'',False,update_run_mapping=False,update_box_mapping=True)
        rc = 0
        while self.running:
            try:
                if not self.found_data_interfaces or (rc%10)==0:
                  #check mountpoints every 10 loops
                  try:
                    self.findMountInterfaces()
                  except:
                    pass
                dirstat = os.statvfs('/')
                d_used = ((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)>>20
                d_total =  (dirstat.f_blocks*dirstat.f_bsize)>>20
                dirstat_var = os.statvfs('/var')
                d_used_var = ((dirstat_var.f_blocks - dirstat_var.f_bavail)*dirstat_var.f_bsize)>>20
                d_total_var =  (dirstat_var.f_blocks*dirstat_var.f_bsize)>>20
                meminfo = self.getMEMInfo()
                #convert to MB
                memtotal = meminfo['MemTotal'] >> 10
                memused = memtotal - ((meminfo['MemFree']+meminfo['Buffers']+meminfo['Cached']+meminfo['SReclaimable']) >> 10)
                netrates = self.getRatesMBs()
                cpu_freq_avg = self.getCPUFreqInfo()

                #check cpu counters to estimate "Turbo" frequency
                ts_new = time.time()
                if is_intel:
                  tsc_new,mperf_new,aperf_new=self.getIntelCPUPerfAvgs(num_cpus)
                if num_cpus>0 and mperf_new-mperf_old>0 and ts_new-ts_old>0:
                  cpu_freq_avg_real = int((1.* (tsc_new-tsc_old))/num_cpus / 1000000 * (aperf_new-aperf_old) / (mperf_new-mperf_old) /(ts_new-ts_old))
                else:
                  cpu_freq_avg_real = 0
                ts_old=ts_new
                tsc_old=tsc_new
                aperf_old=aperf_new
                mperf_old=mperf_new

                #cpu_freq_avg_real = self.getTurbostatInfo() 
                doc = {
                    "date":datetime.datetime.utcfromtimestamp(time.time()).isoformat(),
                    "appliance":bu_name,
                    "cpu_name":cpu_name,
                    "cpu_MHz_nominal":int(cpu_freq*1000),
                    "cpu_phys_cores":cpu_cores,
                    "cpu_hyperthreads":cpu_siblings,
                    "cpu_usage_frac":psutil.cpu_percent()/100.,
                    "cloudState":self.getCloudState(),
                    "activeRunList":self.runList.getActiveRunNumbers(),
                    "usedDisk":d_used,
                    "totalDisk":d_total,
                    "diskOccupancy":d_used/(1.*d_total) if d_total>0 else 0.,
                    "usedDiskVar":d_used_var,
                    "totalDiskVar":d_total_var,
                    "diskVarOccupancy":d_used_var/(1.*d_total_var) if d_total_var>0 else 0.,
                    "memTotal":memtotal,
                    "memUsed":memused,
                    "memUsedFrac":float(memused)/memtotal,
                    "dataNetIn":netrates[0],
                    "dataNetOut":netrates[1],
                    "cpu_MHz_avg":cpu_freq_avg,
                    "cpu_MHz_avg_real":cpu_freq_avg_real
                }
                    #TODO: disk traffic(iostat)
                    #see: http://stackoverflow.com/questions/1296703/getting-system-status-in-python
                eb.elasticize_fubox(doc)
            except Exception as ex:
                self.logger.exception(ex)
            try:
                self.threadEventESBox.wait(5)
                rc+=1
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


