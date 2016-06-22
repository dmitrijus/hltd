import sys,traceback
import os,stat
import time,datetime
import shutil
import simplejson as json
import logging
import zlib
import subprocess
import threading
#import fcntl

from inotifywrapper import InotifyWrapper
import _inotify as inotify
import _zlibextras as zlibextras

ES_DIR_NAME = "TEMP_ES_DIRECTORY"
#file types
UNKNOWN,OUTPUTJSD,DEFINITION,STREAM,INDEX,FAST,SLOW,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT,INI,EOLS,BOLS,EOR,COMPLETE,DAT,PDAT,PJSNDATA,PIDPB,PB,CRASH,MODULELEGEND,PATHLEGEND,BOX,QSTATUS,FLUSH,PROCESSING = range(27)
TO_ELASTICIZE = [STREAM,INDEX,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT,EOLS,EOR,COMPLETE,FLUSH]
TEMPEXT = ".recv"
STREAMERRORNAME = 'streamError'
THISHOST = os.uname()[1]

jsdCache = {}

bw_cnt = 0

##Output redirection class
#class stdOutLog:
#    def __init__(self):
#        self.logger = logging.getLogger(self.__class__.__name__)
#    def write(self, message):
#        self.logger.debug(message)
#class stdErrorLog:
#    def __init__(self):
#        self.logger = logging.getLogger(self.__class__.__name__)
#    def write(self, message):
#        self.logger.error(message)


    #on notify, put the event file in a queue
class MonitorRanger:

    def __init__(self,recursiveMode=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.eventQueue = False
        self.inotifyWrapper = InotifyWrapper(self,recursiveMode)
        self.queueStatusPath = None
        self.queueStatusPathMon = None
        self.queueStatusPathDir = None
        self.queuedLumiList = []
        self.maxQueuedLumi=-1
        #max seen/closed by anelastic thread
        self.maxReceivedEoLS=-1
        self.maxClosedLumi=-1
        self.numOpenLumis=-1
        self.maxCMSSWLumi=-1
        self.maxLSWithOutput=-1
        self.lock = threading.Lock()

        self.output_bw=0
        self.statsCollectorThread = None

    def startStatsCollector(self):
        self.statsCollectorThread = threading.Thread(target=self.statsCollector)
        self.statsCollectorThread.daemon=True #set as daemon thread (not blocking process termination)
        self.statsCollectorThread.start()

    def statsCollector(self):
        global bw_cnt
        bw_cnt_time=None
        while True:
            new_time = time.time()
            if bw_cnt_time is not None:
                d_t = new_time-bw_cnt_time
                if d_t!=0:
                    self.output_bw=bw_cnt/d_t
                    bw_cnt=0
            bw_cnt_time=new_time
            if self.queueStatusPathDir and not os.path.exists(self.queueStatusPathDir):
              self.logger.info('no queue status dir yet.')
            else:
              self.updateQueueStatusFile()
            time.sleep(23.4)

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotifyTimeout(self,timeout):
        self.logger.info("MonitorRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        self.logger.info("MonitorRanger: Join inotify wrapper")
        self.inotifyWrapper.join(timeout)
        if self.inotifyWrapper.isAlive():
            self.logger.info("MonitorRanger: Inotify wrapper join timeout ("+str(timeout)+")")
            return False
        else:
            self.logger.info("MonitorRanger: Inotify wrapper returned")
            return True

    def stop_inotify(self):
        self.logger.info("MonitorRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        self.logger.info("MonitorRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        self.logger.info("MonitorRanger: Inotify wrapper returned")

    def process_default(self, event):
        self.logger.debug("event: %s on: %s" %(str(event.mask),event.fullpath))
        if self.eventQueue:

            if self.queueStatusPath!=None:
                if self.checkNewLumi(event):
                    self.eventQueue.put(event)
            else:
                self.eventQueue.put(event)

    def setEventQueue(self,queue):
        self.eventQueue = queue

    def checkNewLumi(self,event):
        if event.fullpath.endswith("_EoLS.jsn"):
            try:
                queuedLumi = int(os.path.basename(event.fullpath).split('_')[1][2:])
                self.lock.acquire()
                if queuedLumi not in self.queuedLumiList:
                    if queuedLumi>self.maxQueuedLumi:
                        self.maxQueuedLumi=queuedLumi
                    self.queuedLumiList.append(queuedLumi)
                    self.lock.release()
                    self.updateQueueStatusFile()
                else:
                    self.lock.release()
                    #skip if EoL for LS in queue has already been written once (e.g. double file create race)
                    return False
            except Exception as ex:
                self.logger.warning("Problem checking new EoLS filename: "+str(os.path.basename(event.fullpath)) + " error:"+str(ex))
                try:self.lock.release()
                except:pass
            #delete associated BoLS file
            try:
                os.unlink(event.fullpath[:event.fullpath.rfind("_EoLS.jsn")]+"_BoLS.jsn")
            except:
                pass
        elif event.fullpath.endswith("_BoLS.jsn"):
            try:
                queuedLumi = int(os.path.basename(event.fullpath).split('_')[1][2:])
                if queuedLumi>self.maxCMSSWLumi:
                    self.maxCMSSWLumi = queuedLumi
                self.updateQueueStatusFile()
            except:
                pass
            #not passed to the queue
            return False
        return True

    def notifyLumi(self,ls,maxReceivedEoLS,maxClosedLumi,numOpenLumis):
        if self.queueStatusPath==None:return
        self.lock.acquire()
        if ls!=None and ls in self.queuedLumiList:
            self.queuedLumiList.remove(ls)
        self.maxReceivedEoLS=maxReceivedEoLS
        self.maxClosedLumi=maxClosedLumi
        self.numOpenLumis=numOpenLumis
        self.lock.release()
        self.updateQueueStatusFile()

    def notifyMaxLsWithOutput(self,ls):
        self.maxLSWithOutput=max(ls,self.maxLSWithOutput)

    def setQueueStatusPath(self,path,monpath):
        self.queueStatusPath = path
        self.queueStatusPathMon = monpath
        self.queueStatusPathDir = path[:path.rfind('/')]

    def updateQueueStatusFile(self):
        if self.queueStatusPath==None:return
        num_queued_lumis = len(self.queuedLumiList)
        if not os.path.exists(self.queueStatusPathDir):
            self.logger.error("No directory to write queueStatusFile: "+str(self.queueStatusPathDir))
        else:
            self.logger.info("Update status file - queued lumis:"+str(num_queued_lumis)+ " EoLS:: max queued:"+str(self.maxQueuedLumi) \
                             +" un-queued:"+str(self.maxReceivedEoLS)+"  Lumis:: last closed:"+str(self.maxClosedLumi) \
                             + " num open:"+str(self.numOpenLumis) + " max LS in cmssw:"+str(self.maxCMSSWLumi))
        #write json
        doc = {"numQueuedLS":num_queued_lumis,
               "maxQueuedLS":self.maxQueuedLumi,
               "numReadFromQueueLS:":self.maxReceivedEoLS,
               "maxClosedLS":self.maxClosedLumi,
               "numReadOpenLS":self.numOpenLumis,
               "CMSSWMaxLS":self.maxCMSSWLumi,
               "maxLSWithOutput":self.maxLSWithOutput,
               "outputBW": self.output_bw
               }
        try:
            if self.queueStatusPath!=None:
                attempts=3
                while attempts>0:
                    try:
                        with open(self.queueStatusPath+TEMPEXT,"w") as fp:
                            #fcntl.flock(fp, fcntl.LOCK_EX)
                            json.dump(doc,fp)
                        os.rename(self.queueStatusPath+TEMPEXT,self.queueStatusPath)
                        break
                    except Exception as ex:
                        attempts-=1
                        if attempts==0:
                            raise ex
                        self.logger.warning("Unable to write status file, with error:" + str(ex)+".retrying...")
                        time.sleep(0.05)
                try:
                    shutil.copyfile(self.queueStatusPath,self.queueStatusPathMon)
                except:
                    pass
        except Exception as ex:
            self.logger.error("Unable to open/write " + self.queueStatusPath)
            self.logger.exception(ex)


class fileHandler(object):
    def __eq__(self,other):
        return self.filepath == other.filepath

    def __getattr__(self,name):
        if name not in self.__dict__:
            if name in ["dir","ext","basename","name"]: self.getFileInfo()
            elif name in ["filetype"]: self.filetype = self.getFiletype();
            elif name in ["run","ls","stream","index","pid"]: self.getFileHeaders()
            elif name in ["data"]: self.data = self.getData();
            elif name in ["definitions"]: self.getDefinitions()
            elif name in ["host"]: self.host = os.uname()[1];
        if name in ["ctime"]: self.ctime = self.getTime('c')
        if name in ["mtime"]: self.mtime = self.getTime('m')
        return self.__dict__[name]

    def __init__(self,filepath,filetype=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.filepath = filepath
        if filetype: self.filetype = filetype
        self.outDir = self.dir
        self.mergeStage = 0
        self.inputs = []
        self.inputData = []

    def getTime(self,t):
        if self.exists():
            if t == 'c':
                dt=os.path.getctime(self.filepath)
            elif t == 'm':
                dt=os.path.getmtime(self.filepath)
            time = datetime.datetime.utcfromtimestamp(dt).isoformat()
            return time
        return None

    def getFileInfo(self):
        self.dir = os.path.dirname(self.filepath)
        self.basename = os.path.basename(self.filepath)
        self.name,self.ext = os.path.splitext(self.basename)

    def getFiletype(self,filepath = None):
        if not filepath: filepath = self.filepath
        filename = self.basename
        name,ext = self.name,self.ext
        if ext==TEMPEXT:return UNKNOWN
        name = name.upper()
        if "/mon" not in filepath:
            if ext == ".dat" and "_PID" not in name: return DAT
            if ext == ".dat" and "_PID" in name: return PDAT
            if ext == ".jsndata" and "_PID" in name: return PJSNDATA
            if ext == ".ini" and "_PID" in name: return INI
            if ext == ".jsd" and "OUTPUT_" in name: return OUTPUTJSD
            if ext == ".jsd" : return DEFINITION
            if ext == ".jsn":
                if STREAMERRORNAME.upper() in name: return STREAMERR
                elif "_STREAM" in name and "_PID" in name: return STREAM
                elif "_INDEX" in name and  "_PID" in name: return INDEX
                elif "_CRASH" in name and "_PID" in name: return CRASH
                elif "_EOLS" in name: return EOLS
                elif "_BOLS" in name: return BOLS
                elif "_EOR" in name: return EOR
        if ext==".jsn":
            if "_STREAM" in name and "_PID" not in name: return OUTPUT
            if name.startswith("QUEUE_STATUS"): return QSTATUS
            if name.startswith("SLOWMONI"): return SLOW
        if ext==".pb":
            #if "_PID" not in name: return PB
            if "_PID" not in name: return UNKNOWN
            else: return PIDPB
        if ext == ".ini" and "/mon" in filepath: return INI
        if name.endswith("COMPLETE"): return COMPLETE
        if ext == ".fast" in filename: return FAST
        if name.startswith("MICROSTATELEGEND"): return MODULELEGEND
        if name.startswith("PATHLEGEND"): return PATHLEGEND
        if "boxes" in filepath : return BOX
        if filename == 'flush': return FLUSH
        if filename == 'processing': return PROCESSING
        return UNKNOWN


    def getFileHeaders(self):
        filetype = self.filetype
        name,ext = self.name,self.ext
        splitname = name.split("_")
        if filetype in [STREAM,INI,PDAT,PJSNDATA,PIDPB,CRASH]: self.run,self.ls,self.stream,self.pid = splitname
        elif filetype == SLOW: slowname,self.ls,self.pid = splitname
        elif filetype == FAST: self.run,self.pid = splitname
        elif filetype in [DAT,PB,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT]: self.run,self.ls,self.stream,self.host = splitname
        elif filetype == INDEX: self.run,self.ls,self.index,self.pid = splitname
        elif filetype in [EOLS,BOLS]: self.run,self.ls,self.eols = splitname
        else:
            self.logger.warning("Bad filetype: %s" %self.filepath)
            self.run,self.ls,self.stream = [None]*3

    def getData(self):
        if self.ext == '.jsn': return self.getJsonData()
        elif self.filetype == BOX: return self.getBoxData()
        return None

    def getBoxData(self,filepath = None):
        if not filepath: filepath = self.filepath
        try:
            with open(filepath,'r') as fi:
                data = json.load(fi)
        except IOError,e:
            data = {}
        except StandardError,e:
        #    self.logger.exception(e)
            self.logger.warning('Box parse error:'+str(e))
            data = {}
        return data

        #get data from json file
    def getJsonData(self,filepath = None):
        if not filepath: filepath = self.filepath
        try:
            with open(filepath) as fi:
                data = json.load(fi)
        except json.scanner.JSONDecodeError,e:
            self.logger.exception(e)
            data = {}
        except StandardError,e:
            self.logger.exception(e)
            data = {}
        return data

    def setJsdfile(self,jsdfile):
        self.jsdfile = jsdfile
        if self.filetype in [OUTPUT,STREAMDQMHISTOUTPUT,CRASH,STREAMERR]: self.initData()

    def initData(self):
        defs = self.definitions
        self.data = {}
        if defs:
            self.data["data"] = [self.nullValue(f["type"]) for f in defs]

    def nullValue(self,ftype):
        if ftype == "integer": return "0"
        elif ftype  == "string": return ""
        else:
            self.logger.warning("bad field type %r" %(ftype))
            return "ERR"

    def checkSources(self):
        data,defs = self.data,self.definitions
        for item in defs:
            fieldName = item["name"]
            index = defs.index(item)
            if "source" in item:
                source = item["source"]
                sIndex,ftype = self.getFieldIndex(fieldName) #TODO:pyflakes gives warning..
                data[index] = data[sIndex]

    def getFieldIndex(self,field):
        defs = self.definitions
        if defs:
            index = next((defs.index(item) for item in defs if item["name"] == field),-1)
            ftype = defs[index]["type"]
            return index,ftype


    def getFieldByName(self,field):
        index,ftype = self.getFieldIndex(field)
        data = self.data["data"]
        if index > -1:
            value = int(data[index]) if ftype == "integer" else str(data[index])
            return value
        else:
            self.logger.warning("bad field request %r in %r" %(field,self.definitions))
            return None

    def setFieldByName(self,field,value,warning=True):
        index,ftype = self.getFieldIndex(field)
        data = self.data["data"]
        if index > -1:
            data[index] = value
            return True
        else:
            if warning==True:
                self.logger.warning("bad field request %r in %r" %(field,self.definitions))
            return False

        #get definitions from jsd file
    def getDefinitions(self):
        if self.filetype in [STREAM]:
            #try:
            self.jsdfile = self.data["definition"]
            #except:
            #    self.logger.error("no definition field in "+str(self.filepath))
            #   self.definitions = {}
            #   return False
        elif not self.jsdfile:
            self.logger.warning("jsd file not set")
            self.definitions = []
            return False
        if self.jsdfile not in jsdCache.keys():
            jsdCache[self.jsdfile] = self.getJsonData(self.jsdfile)
        self.definitions = jsdCache[self.jsdfile]["data"]
        #self.definitions = self.getJsonData(self.jsdfile)["data"]
        return True


    def deleteFile(self,silent=False):
        #return True
        filepath = self.filepath
        if silent==False:
            self.logger.info(filepath)
        if os.path.isfile(filepath):
            try:
                os.remove(filepath)
            except Exception,e:
                self.logger.exception(e)
                return False
        return True

    def moveFile(self,newpath,copy = False,adler32=False,silent=False, createDestinationDir=True, missingDirAlert=True, updateFileInfo=True):
        checksum=1
        if not self.exists(): return True,checksum
        oldpath = self.filepath
        newdir = os.path.dirname(newpath)

        if not os.path.exists(oldpath):
            self.logger.error("Source path does not exist: " + oldpath)
            return False,checksum

        self.logger.info("%s -> %s" %(oldpath,newpath))
        retries = 5
        #temp name with temporary host name included to avoid conflict between multiple hosts copying at the same time
        newpath_tmp = newpath+'_'+THISHOST+TEMPEXT
        while True:
            try:
                #few attempts at creating destination directory 
                dir_missing_attempts=5
                while not os.path.isdir(newdir):
                    dir_missing_attempts-=1
                    if dir_missing_attempts>=0 and createDestinationDir==False:
                        continue
                    if createDestinationDir==False:
                        if silent==False and missingDirAlert==True:
                            self.logger.error("Unable to transport file "+str(oldpath)+". Destination directory does not exist: " + str(newdir))
                        return False,checksum
                    elif dir_missing_attempts<0:
                            self.logger.error("Unable to make directory "+str(newdir))
                            return False,checksum
                    try:
                        os.makedirs(newdir)
                    except:
                        #repeated check if dir was created in the meantime
                        if not os.path.isdir(newdir):
                            os.makedirs(newdir)

                if adler32:checksum=self.moveFileAdler32(oldpath,newpath_tmp,copy)
                else:
                    if copy: shutil.copy(oldpath,newpath_tmp)
                    else:
                        shutil.move(oldpath,newpath_tmp)
                break

            except (OSError,IOError),e:
                if silent==False:
                    if isinstance(e, IOError) and e.errno==2:
                        self.logger.warning("Error in attempt to copy/move file to destination " + newpath + ":" + str(e))
                    else:
                        self.logger.exception(e)
                retries-=1
                if retries == 0:
                    if silent==False:
                        #do not print this warning if directory was removed
                        if os.path.isdir(newdir):
                            self.logger.error("Failure to move file "+str(oldpath)+" to "+str(newpath_tmp))
                        else:
                            self.logger.warning("Failure to move file "+str(oldpath)+" to "+str(newpath_tmp)+'.Target directory is gone')
                    return False,checksum
                else:
                    time.sleep(0.5)
            except Exception, e:
                self.logger.exception(e)
                raise e
        #renaming
        retries = 5
        while True:
            try:
                os.rename(newpath_tmp,newpath)
                break
            except (OSError,IOError),e:
                if silent==False:
                    if isinstance(e, IOError) and e.errno==2:
                        self.logger.warning("Error encountered in attempt to copy/move file to destination " + newpath + ":" + str(e))
                    elif isinstance(e, OSError) and e.errno==18:
                        self.logger.warning("failed attempt to rename " + newpath_tmp + " to " + newpath + " error: "+ str(e))
                    else:
                        self.logger.exception(e)
                retries-=1
                if retries == 0:
                    if silent==False:
                        #do not print this warning if directory was deleted
                        if os.path.isdir(newdir):
                            self.logger.error("Failure to rename temporary file "+str(newpath_tmp)+" to "+str(newpath))
                        else:
                            self.logger.warning("Failure to rename temporary file "+str(newpath_tmp)+" to "+str(newpath)+'.Target directory is gone')
                    return False,checksum
                else:
                    time.sleep(0.5)
            except Exception, e:
                self.logger.exception(e)
                raise e

        if updateFileInfo:
            self.filepath = newpath
            self.getFileInfo()
        return True,checksum

    #move file (works only on src as file, not directory)
    def moveFileAdler32(self,src,dst,copy):
        global bw_cnt
        if os.path.isdir(src):
            raise shutil.Error("source `%s` is a directory")

        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))

        try:
            if os.path.samefile(src, dst):
                raise shutil.Error("`%s` and `%s` are the same file" % (src, dst))
        except OSError:
            pass

        #initial adler32 value
        adler32c=1
        #calculate checksum on the fly
        with open(src, 'rb') as fsrc:
            with open(dst, 'wb') as fdst:

                length=16*1024
                while 1:
                    buf = fsrc.read(length)
                    if not buf:
                        break
                    adler32c=zlib.adler32(buf,adler32c)
                    fdst.write(buf)
                    bw_cnt+=len(buf)

        #copy mode bits on the destionation file
        st = os.stat(src)
        mode = stat.S_IMODE(st.st_mode)
        os.chmod(dst, mode)

        if copy==False:os.unlink(src)
        return adler32c


    def mergeDatInputs(self,destinationpath,doChecksum):
        global bw_cnt
        dirname = os.path.dirname(self.filepath)
        ccomb=1
        dst = None
        adler32accum=1
        json_size=0
        copy_size=0
        for input in self.inputs:
            nproc = int(input.getFieldByName('Processed'))
            nerr = int(input.getFieldByName('ErrorEvents'))
            ifile = input.getFieldByName('Filelist')
            ifilecksum = int(input.getFieldByName('FileAdler32'))
            ifilesize = int(input.getFieldByName('Filesize'))

            #no file to merge if n processed = 0
            if nproc==0:
                continue

            #if any of 'proper' files has checksum set to -1, disable the check and substitute -1 in output json
            if ifilecksum == -1:
                ccomb = -1
                doChecksum = False
            if doChecksum:
                ccomb = zlibextras.adler32_combine(ccomb,ifilecksum,ifilesize)

            json_size+=ifilesize

            #if going to merge, open input file
            if dst == None:
                dst = open(destinationpath,'wb')

            length=16*1024
            adler32c=1
            file_size=0
            with open(os.path.join(dirname,ifile), 'rb') as fsrc:
                while 1:
                    buf = fsrc.read(length)
                    if not buf:
                        break
                    read_len=len(buf)
                    file_size+=read_len
                    if doChecksum:
                        adler32c=zlib.adler32(buf,adler32c)
                    dst.write(buf)
                    bw_cnt+=read_len
            copy_size += file_size
            #adler32c = adler32 & 0xffffffff
            if doChecksum and ifilecksum != (adler32c & 0xffffffff):
                self.logger.fatal("Checksum mismatch detected while reading file " + ifile + ". expected:"+str(ifilecksum)+" obtained:"+str(adler32c&0xffffffff))
            if file_size!=ifilesize:
                self.logger.fatal("Size mismatch is detected while reading file " + ifile + ". expected:"+str(ifilesize)+" obtained:"+str(file_size))
            if doChecksum:
                adler32accum = zlibextras.adler32_combine(adler32accum,adler32c,ifilesize) #& 0xffffffff

        if dst:
            dst.close()

        #delete input files
        for input in self.inputs:
            ifile = input.getFieldByName('Filelist')
            if not ifile:
              continue
            try:os.remove(os.path.join(dirname,ifile))
            except Exception as ex:
              self.logger.exception(ex)

        self.setFieldByName("Filesize",json_size)
        if doChecksum:
            self.setFieldByName("FileAdler32",ccomb & 0xffffffff)
        else:
            self.setFieldByName("FileAdler32",-1)
        checks_pass = (ccomb == adler32accum or not doChecksum ) and (copy_size == json_size)
        self.logger.info(str(checks_pass))
        return checks_pass

    def exists(self):
        return os.path.exists(self.filepath)

        #write self.outputData in json self.filepath
    def writeout(self,empty=False,verbose=True):
        filepath = self.filepath
        outputData = self.data
        self.logger.info(filepath)

        try:
            with open(filepath,"w") as fi:
                if empty==False:
                    json.dump(outputData,fi)
        except Exception,e:
            if verbose:
                self.logger.exception(e)
            else:
                self.logger.warning('unable to writeout ' + filepath)
            return False
        return True

    #TODO:make sure that the file is copied only once
    def esCopy(self, keepmtime=True):
        if not self.exists(): return
        if self.filetype in TO_ELASTICIZE:
            esDir = os.path.join(self.dir,ES_DIR_NAME)
            if os.path.isdir(esDir):
                newpathTemp = os.path.join(esDir,self.basename+TEMPEXT)
                newpath = os.path.join(esDir,self.basename)
                retries = 5
                while True:
                    try:
                        if keepmtime:
                            shutil.copy2(self.filepath,newpathTemp)
                        else:
                            shutil.copy(self.filepath,newpathTemp)
                        break
                    except (OSError,IOError),e:
                        retries-=1
                        if retries == 0:
                            self.logger.exception(e)
                            return
                            #raise e #non-critical exception
                        else:
                            time.sleep(0.5)
                retries = 5
                while True:
                    try:
                        os.rename(newpathTemp,newpath)
                        break
                    except (OSError,IOError),e:
                        retries-=1
                        if retries == 0:
                            self.logger.exception(e)
                            return
                            #raise e #non-critical exception
                        else:
                            time.sleep(0.5)


    def merge(self,infile):
        defs,oldData = self.definitions,self.data["data"][:]           #TODO: check infile definitions
        jsdfile = infile.jsdfile
        host = infile.host
        newData = infile.data["data"][:]

        self.logger.debug("old: %r with new: %r" %(oldData,newData))
        result=Aggregator(defs,oldData,newData).output()
        self.logger.debug("result: %r" %result)
        self.data["data"] = result
        self.data["definition"] = jsdfile
        self.data["source"] = host

        self.inputs.append(infile)
        pbOutput = False
        if self.filetype!=STREAMDQMHISTOUTPUT:
            #append list of files if this is json metadata stream
            try:
                findex,ftype = self.getFieldIndex("Filelist")
                flist = newData[findex].split(',')
                for l in flist:
                    if l.endswith('.jsndata'):
                        if (l.startswith('/')==False):
                            self.inputData.append(os.path.join(self.dir,l))
                        else:
                            self.inputData.append(l)
                    elif l.endswith('.pb'):
                      pbOutput=True
            except Exception as ex:
                self.logger.exception(ex)
                pass
            self.writeout()

        #detect streams with pb files as DQM Histogram streams and change outfile type
        if pbOutput:
          self.filetype = STREAMDQMHISTOUTPUT

    def updateData(self,infile):
        self.data["data"]=infile.data["data"][:]

    def isJsonDataStream(self):
        if len(self.inputData)>0:return True
        return False

    def mergeAndMoveJsnDataMaybe(self,outDir, removeInput=True):
        if len(self.inputData):
            try:
                outfile = os.path.join(self.dir,self.name+'.jsndata')
                command_args = ["jsonMerger",outfile]
                for fid in self.inputData:
                    command_args.append(fid)
                p = subprocess.Popen(command_args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                p.wait()
                if p.returncode!=0:
                    self.logger.error('jsonMerger returned with exit code '+str(p.returncode)+' and response: ' + str(p.communicate()) + '. Merging parameters given:'+str(command_args))
                    return False
            except Exception as ex:
                self.logger.exception(ex)
                return False
            if removeInput:
                for f in self.inputData:
                    try:
                        os.remove(f)
                    except:
                        pass
                try:
                    self.setFieldByName("Filesize",str(os.stat(outfile).st_size))
                    self.setFieldByName("FileAdler32","-1")
                    self.writeout()
                    jsndatFile = fileHandler(outfile)
                    result,cs = jsndatFile.moveFile(os.path.join(outDir, os.path.basename(outfile)),adler32=False,createDestinationDir=False)
                    if not result: return False
                except Exception as ex:
                    self.logger.error("Unable to copy jsonStream data file "+str(outfile)+" to output.")
                    self.logger.exception(ex)
                    return False
        return True


    def mergeDQM(self,outDir):
        outputName,outputExt = os.path.splitext(self.basename)
        outputName+='.pb'
        fullOutputPath = os.path.join(outDir,outputName)
        command_args = ["fastHadd","add","-o",fullOutputPath]

        totalEvents = self.getFieldByName("Processed")+self.getFieldByName("ErrorEvents")

        processedEvents = 0
        acceptedEvents = 0
        errorEvents = 0

        numFiles=0
        inFileSizes=[]
        for f in self.inputs:
#           try:
            fname = f.getFieldByName('Filelist')
            fullpath = os.path.join(outDir,fname)
            try:
                proc = f.getFieldByName('Processed')
                acc = f.getFieldByName('Accepted')
                err = f.getFieldByName('ErrorEvents')
                #self.logger.info('merging file : ' + str(fname) + ' counts:'+str(proc) + ' ' + str(acc) + ' ' + str(err))
                if fname:
                    pbfsize = os.stat(fullpath).st_size
                    inFileSizes.append(pbfsize)
                    command_args.append(fullpath)
                    numFiles+=1
                    processedEvents+= proc
                    acceptedEvents+= acc
                    errorEvents+= err
                else:
                    if proc>0:
                        self.logger.info('no histograms pb file : '+ str(fullpath))
                    errorEvents+= proc+err


            except OSError as ex:
                #file missing?
                errorEvents+= f.getFieldByName('Processed') + f.getFieldByName('ErrorEvents')
                self.logger.error('fastHadd pb file is missing? : '+ fullpath)
                self.logger.exception(ex)

        filesize=0
        hasError=False
        if numFiles>0:
            time_start = time.time()
            p = subprocess.Popen(command_args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            p.wait()
            time_delta = time.time()-time_start
            if p.returncode!=0:
                self.logger.error('fastHadd returned with exit code '+str(p.returncode)+' and response: ' + str(p.communicate()) + '. Merging parameters given:'+str(command_args) +' ,file sizes(B):'+str(inFileSizes))
                #DQM more verbose debugging
                try:
                    filesize = os.stat(fullOutputPath).st_size
                    self.logger.error('fastHadd reported to fail at merging, while output pb file exists! '+ fullOutputPath + ' with size(B): '+str(filesize))
                except:
                    pass
                self.setFieldByName('ReturnCodeMask', str(p.returncode))
                hasError=True
            else:
                self.logger.info('fastHadd merging of ' + str(len(inFileSizes)) + ' files took ' + str(time_delta) + ' seconds')

            for f in command_args[4:]:
                try:
                    if hasError==False:os.remove(f)
                except OSError as ex:
                    self.logger.warning('exception removing file '+f+' : '+str(ex))
        else:
            hasError=True

        if hasError:
            errorEvents+=processedEvents
            processedEvents=0
            acceptedEvents=0
            outputName=""
            filesize=0

        #correct for the missing event count in input file (when we have a crash)
        if totalEvents>processedEvents+errorEvents: errorEvents += totalEvents - processedEvents - errorEvents

        self.setFieldByName('Processed',str(processedEvents))
        self.setFieldByName('Accepted',str(acceptedEvents))
        self.setFieldByName('ErrorEvents',str(errorEvents))
        self.setFieldByName('Filelist',outputName)
        self.setFieldByName('Filesize',str(filesize))
        #self.esCopy() #happens after move to output
        self.writeout()
        return outputName


class Aggregator(object):
    def __init__(self,definitions,newData,oldData):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.definitions = definitions
        self.newData = newData
        self.oldData = oldData

    def output(self):
        self.result = map(self.action,self.definitions,self.newData,self.oldData)
        return self.result

    def action(self,definition,data1,data2=None):
        actionName = "action_"+definition["operation"]
        if hasattr(self,actionName):
            try:
                return getattr(self,actionName)(data1,data2)
            except AttributeError,e:
                self.logger.exception(e)
                return None
        else:
            self.logger.warning("bad operation: %r" %actionName)
            return None

    def action_binaryOr(self,data1,data2):
        try:
            res =  int(data1) | int(data2)
        except TypeError,e:
            self.logger.exception(e)
            res = 0
        return str(res)

    def action_merge(self,data1,data2):
        if not data2: return data1
        file1 = fileHandler(data1)

        file2 = fileHandler(data2)
        newfilename = "_".join([file2.run,file2.ls,file2.stream,file2.host])+file2.ext
        file2 = fileHandler(newfilename)

        if not file1 == file2:
            if data1: self.logger.warning("found different files: %r,%r" %(file1.filepath,file2.filepath))
            return file2.basename
        return file1.basename

    def action_sum(self,data1,data2):
        try:
            res =  int(data1) + int(data2)
        except TypeError,e:
            self.logger.exception(e)
            res = 0
        return str(res)

    def action_same(self,data1,data2):
        if str(data1)=='' or str(data1)=='0' or str(data1)=='N/A':
            return str(data2)
        if str(data2)=='' or str(data2)=='0' or str(data2)=='N/A':
            return str(data1)
        if str(data1) == str(data2):
            return str(data1)
        else:
            return "N/A"

    def action_cat(self,data1,data2):
        if data2 and data1: return str(data1)+","+str(data2)
        elif data1: return str(data1)
        elif data2: return str(data2)
        else: return ""

    def action_adler32(self,data1,data2):
        return "-1"
