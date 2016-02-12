import os
import time
import simplejson as json
import dateutil
import dateutil.parser
import datetime
import logging

import SystemMonitor
from HLTDCommon import dqm_globalrun_filepattern
from inotifywrapper import InotifyWrapper
from aUtils import fileHandler


class ResourceRanger:

    def __init__(self,confClass,stateInfo,resInfo,runList,mountMgr,boxInfo,monitor,resource_lock):
        self.inotifyWrapper = InotifyWrapper(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.state = stateInfo
        self.resInfo = resInfo
        self.runList = runList
        self.managed_monitor = monitor
        self.managed_monitor.preStart()
        self.managed_monitor.start()
        self.regpath = []
        self.mm = mountMgr
        self.boxInfo = boxInfo
        self.resource_lock = resource_lock
        self.hostname = os.uname()[1]
        global conf
        conf = confClass

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)
        self.regpath.append(path)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_managed_monitor(self):
        self.managed_monitor.stop()
        self.managed_monitor.join()
        self.logger.info("ResourceRanger: managed monitor shutdown done")

    def stop_inotify(self):
        self.inotifyWrapper.stop()
        self.inotifyWrapper.join()
        self.logger.info("ResourceRanger: Inotify wrapper shutdown done")

    def process_IN_MOVED_TO(self, event):
        self.logger.debug('ResourceRanger-MOVEDTO: event '+event.fullpath)
        basename = os.path.basename(event.fullpath)
        if basename.startswith('resource_summary'):return
        try:
            resourcepath=event.fullpath[1:event.fullpath.rfind("/")]
            resourcestate=resourcepath[resourcepath.rfind("/")+1:]
            resourcename=event.fullpath[event.fullpath.rfind("/")+1:]
            self.resource_lock.acquire()

            if not (resourcestate == 'online' or resourcestate == 'cloud'
                    or resourcestate == 'quarantined'):
                self.logger.debug('ResourceNotifier: new resource '
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
                    self.logger.info("ResourceRanger: found active run "+str(run.runnumber)+ " when received inotify MOVED event for "+event.fullpath)
                    """grab resources that become available
                    #@@EM implement threaded acquisition of resources here
                    """
                    #find all ready cores in same dir where inotify was triggered
                    try:
                        reslist = os.listdir('/'+resourcepath)
                    except Exception as ex:
                        self.logger.error("exception encountered in looking for resources")
                        self.logger.exception(ex)
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
                        self.logger.info("ResourceRanger: acquired resource(s) "+str(res.cpu))
                        run.StartOnResource(res)
                        self.logger.info("ResourceRanger: started process on resource "
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
                                    self.resInfo.resmove(self.resInfo.broken,self.resInfo.idles,resname)
                        #move this except after listdir?
                        except Exception as ex:
                            self.logger.info("exception encountered in looking for resources in except")
                            self.logger.info(ex)
            elif resourcestate=="cloud":
                #check if cloud mode was initiated, activate if necessary
                if conf.role=='fu' and self.state.cloud_mode==False:
                    time.sleep(1)
                    logging.info('detected core moved to cloud resources. Triggering cloud activation sequence.')
                    with open(os.path.join(conf.watch_directory,'exclude'),'w+') as fobj:
                        pass
                    time.sleep(1)
        except Exception as ex:
            self.logger.error("exception in ResourceRanger")
            self.logger.error(ex)
        try:
            self.resource_lock.release()
        except:pass

    def process_IN_CREATE(self, event):
        self.logger.debug('ResourceRanger-CREATE: event '+event.fullpath)
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
                    self.logger.error("Run "+str(lrun.runnumber)+" notification: skipping resource "+basename+" which is stale")
                    self.resource_lock.release()
                    return
                self.logger.info('Try attaching FU resource: last run is '+str(lrun.runnumber))
                newRes = lrun.maybeNotifyNewRun(basename,resourceage)
            self.resource_lock.release()
            if newRes:
                newRes.NotifyNewRun(lrun.runnumber)
        except Exception as ex:
            self.logger.exception(ex)
            try:self.resource_lock.release()
            except:pass

    def process_default(self, event):
        self.logger.debug('ResourceRanger: event '+event.fullpath +' type '+ str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

    def process_IN_CLOSE_WRITE(self, event):
        self.logger.debug('ResourceRanger-IN_CLOSE_WRITE: event '+event.fullpath)
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
                        self.logger.warning('setting stale flag for resource '+basename + ' which is '+str(dt)+' seconds behind')
                        #should be << 1s if NFS is responsive, set stale handle flag
                        infile.data['detectedStaleHandle']=True
                    elif dt < -5:
                        self.logger.error('setting stale flag for resource '+basename + ' which is '+str(dt)+' seconds ahead (clock skew)')
                        infile.data['detectedStaleHandle']=True

                    self.boxInfo.FUMap[basename] = [infile.data,current_time,True]
                except Exception as ex:
                    if not emptyBox:
                        self.logger.error("Unable to read of parse boxinfo file "+basename)
                        self.logger.exception(ex)
                    else:
                        self.logger.warning("got empty box file "+basename)
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
                self.logger.exception(ex)
                checkSuccessful=False
                break;
            try:
                if runNumber in doc['activeRuns']:
                    runFound=True
                    break;
            except Exception as ex:
                self.logger.exception(ex)
                checkSuccessful=False
                break
        return checkSuccessful,runFound



