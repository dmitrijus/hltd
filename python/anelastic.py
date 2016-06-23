#!/bin/env python

import sys,traceback
import os
import time
import shutil
import subprocess

import filecmp
import _inotify as inotify
import threading
import Queue
import simplejson as json
import logging


from hltdconf import hltdConf,initConf
from aUtils import *
from daemon2 import stdOutLog,stdErrorLog

host = os.uname()[1]

class LumiSectionRanger:
    def __init__(self,mr,tempdir,outdir,run_number):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dqmHandler = None
        self.dqmQueue = Queue.Queue()
        self.mr = mr
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()
        self.errIniFile = threading.Event()
        self.receivedFirstLumiFiles = threading.Event()
        self.initBuffer = []
        self.LSHandlerList = {}  # {(run,ls): LumiSectionHandler()}
        self.ClosedEmptyLSList = set() #empty LS set with metadata fully copied to output
        self.activeStreams = [] # updated by the ini files
        self.streamCounters = {} # extended by ini files, updated by the lumi handlers
        self.source = None
        self.eventtype = None
        self.infile = None
        self.EOR = None  #EORfile Object
        self.complete = None  #complete file Object
        self.run_number = run_number
        self.outdir = outdir
        self.tempdir = tempdir
        self.jsdfile = None
        self.useTimeout=60
        self.maxQueuedLumi=0
        self.maxReceivedEoLS=0
        self.maxClosedLumi=0
        self.iniReceived=False
        self.indexReceived=False
        self.flush = None
        self.allowEmptyLs=False
        self.logged_early_crash_warning=False
        self.EOLS_list = []

    #def join(self, stop=False, timeout=None):
    #    if stop: self.stop()
    #    super(LumiSectionRanger, self).join(timeout)

        #remove for threading
    def start(self):
        self.run()

    def stop(self,timeout=60):
        self.useTimeout=timeout
        self.stoprequest.set()

    def setSource(self,source):
        self.source = source

    def run(self):
        self.logger.info("Start main loop, watching: "+watchDir)
        open(os.path.join(watchDir,'flush'),'w').close()
        self.flush = fileHandler(os.path.join(watchDir,'flush'))
        endTimeout=-1
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet() and self.checkClosure()):
            if self.source:
                try:
                    event = self.source.get(True,0.5) #blocking with timeout
                    self.eventtype = event.mask
                    if isinstance(event,FileEvent):
                        self.infile = fileHandler(event.fullpath,self.eventtype)
                    else:
                        self.infile = fileHandler(event.fullpath)
                    self.emptyQueue.clear()
                    self.process()
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set()
                except Exception as ex:
                    self.logger.exception(ex)
                    self.logger.fatal("Exiting on unhandled exception")
                    os._exit(1)
            else:
                time.sleep(0.5)

            #allow timeout in case 'complete' file is received and lumi is not closed
            if self.stoprequest.isSet() and self.emptyQueue.isSet() and self.checkClosure()==False:
                if endTimeout<=-1: endTimeout=self.useTimeout*2
                elif endTimeout<=self.useTimeout:
                  #at half of the period tell DQM thread to quit merging and assign error events to unmerged LS-es in histo stream
                  self.dqmHandler.setSkipAll()
                if endTimeout==0: break
                endTimeout-=1

            if self.iniReceived==False:
                try:
                    os.stat(rawinputdir)
                except OSError as ex:
                    try:
                        time.sleep(.5)
                        os.stat(rawinputdir)
                    except:
                        #no such file or directory
                        if ex.errno==2:
                            self.logger.warning('Starting run with input directory missing. anelastic script will die.')
                            self.writeElasticMarker()
                            os._exit(1)
                except:
                    pass

        self.logger.info("joining DQM merger thread")
        try:
            #allow 10 min timeout in case of failure (however
            self.dqmHandler.waitFinish(10)
        except:
            pass
        try:
            if self.dqmHandler.isAlive():
                self.logger.warning("DQM thread has not terminated after waiting for 10 seconds")
        except:
            pass

        self.logger.info("Stopping main loop, watching: "+watchDir)


        if self.checkClosure(checkEmpty=False)==False:
            self.logger.error('not all lumisections were closed on exit!')
            try:
                self.logger.error('open lumisections are: '+str(self.getOpenLumis()))
            except:
                pass

        self.complete.esCopy()
        #generate and move EoR completition file
        self.createOutputEoR()

        self.logger.info("Stopping main loop, watching: "+watchDir)

        #send the fileEvent to the proper LShandlerand remove closed LSs, or process INI and EOR files

    def flushBuffer(self):
        self.receivedFirstLumiFiles.set()
        for self.infile in self.initBuffer:
            self.process()
        self.initBuffer=[]

    def process(self):

        filetype = self.infile.filetype

        #temporary way to detect that CMSSW will output empty LS json files
        if filetype == PROCESSING:
            self.allowEmptyLs=True

        if not self.receivedFirstLumiFiles.isSet():
            if filetype == CRASH:
                    #handle condition where crash happens immediately after opening data
                if self.indexReceived:
                    self.logger.warning("detected crash on first data - flushing buffers so that error event accounting can be done properly")
                    self.initBuffer.append(self.infile)
                    self.flushBuffer()
                    return
                elif not self.logged_early_crash_warning:
                    self.logged_early_crash_warning=True
                    self.logger.fatal("Detected cmsRun job crash before event processing was started!")

            if filetype == INDEX:
                self.logger.info('buffering index file '+self.infile.filepath)
                self.initBuffer.append(self.infile)
                self.indexReceived=True
                return
            elif filetype in [EOLS,EOR,COMPLETE,PROCESSING]:
                self.logger.info("PROCESSING")
                self.initBuffer.append(self.infile)
                #flush and when first processing file flag, EoL, EoR or completion arrive
                self.flushBuffer()
                return

        #this should not happen unless there is neither index not EoLS file present for the lumisection
        if not self.receivedFirstLumiFiles.isSet() and filetype in [STREAM,STREAMDQMHISTOUTPUT,DAT,PB]:
            self.logger.fatal("received output file earlier than expected. This should never happen!")
            return

        if filetype in [STREAM,STREAMDQMHISTOUTPUT,INDEX,EOLS,DAT,PB]:
            run,ls = (self.infile.run,self.infile.ls)
            key = (run,ls)
            ls_num=int(ls[2:])
            if filetype==EOLS:
                if ls_num in self.EOLS_list:
                    self.logger.warning("EoLS file for this lumisection has already been received before")
                    self.mr.notifyLumi(ls_num,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
                    return
                self.EOLS_list.append(ls_num)
                if self.maxReceivedEoLS<ls_num:
                    self.maxReceivedEoLS=ls_num
                self.mr.notifyLumi(ls_num,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())

                for lskey in self.LSHandlerList:
                    #if any previous open lumisection still didn't get EoLS notification, assume missing due to crashes and create EoLS to allow error event accounting
                    if self.LSHandlerList[lskey].ls_num < ls_num and not self.LSHandlerList[lskey].EOLS:
                        self.makeEoLSFile(self.LSHandlerList[lskey].ls)

            if key not in self.LSHandlerList:
                if filetype == INDEX:
                    madeBoLS=False
                    for lskey in self.LSHandlerList:
                    #same as when receiving EoLS type (and index file from a new LS), check if earlier lumisections can be closed
                        if self.LSHandlerList[lskey].ls_num < ls_num and not self.LSHandlerList[lskey].EOLS:
                            self.makeEoLSFile(self.LSHandlerList[lskey].ls)
                            if not madeBoLS:
                                self.makeBoLSFile(ls_num)
                                madeBoLS=True

                if ls_num in self.ClosedEmptyLSList:
                    if filetype in [STREAM]:
                        self.cleanStreamFiles()
                    return
                isEmptyLS = True if filetype not in [INDEX] else False
                if isEmptyLS and not self.allowEmptyLs:
                    #detected CMSSW version which writes out empty lumisections
                    return
                self.LSHandlerList[key] = LumiSectionHandler(self,run,ls,self.activeStreams,self.streamCounters,self.tempdir,self.outdir,self.jsdfile,isEmptyLS)
                if filetype not in [INDEX]:
                    self.LSHandlerList[key].emptyLS=True
                else:
                    self.checkDestinationDir()

                self.mr.notifyLumi(None,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
            lsHandler = self.LSHandlerList[key]
            lsHandler.processFile(self.infile)
            if lsHandler.closed.isSet():
                if self.maxClosedLumi<ls_num:
                    self.maxClosedLumi=ls_num
                    self.mr.notifyLumi(None,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
                #keep history of closed lumisections
                self.ClosedEmptyLSList.add(ls_num)
                self.LSHandlerList.pop(key,None)
        elif filetype == DEFINITION:
            self.processDefinitionFile()
        elif filetype == OUTPUTJSD:
            if not self.jsdfile:self.jsdfile=self.infile.filepath
        elif filetype == COMPLETE:
            self.processCompleteFile()
        elif filetype == INI:
            self.processINIfile()
            self.iniReceived=True
        elif filetype == CRASH:
            self.processCRASHfile()
        elif filetype == EOR:
            self.processEORFile()
        #elif filetype == PROCESSING:
        #    self.processProcessingFile()

    def processCRASHfile(self):

        #send CRASHfile to every LSHandler
        lsList = self.LSHandlerList
        basename = self.infile.basename
        errCode = self.infile.data["errorCode"]
        self.logger.info("%r with errcode: %r" %(basename,errCode))
        for item in lsList.values():
            item.processFile(self.infile)

    def createOutputDirs(self,remotefiledir):
        try:
            os.mkdir(remotefiledir)
            os.mkdir(os.path.join(remotefiledir,'jsns'))
            os.mkdir(os.path.join(remotefiledir,'data'))
            return True
        except OSError as ex:
            if ex.errno==17:
              return True
              pass
            elif ex.errno==2:
              self.logger.warning('Caught OSError '+str(ex.errno)+' creating directory: ' + str(ex))
              return True
            else:
              self.logger.warning('Caught OSError '+str(ex.errno)+' creating directory: ' + str(ex))
              return False
        except Exception as ex:
            self.logger.error('Caught exception creating directory: ' + str(ex))
        return False
 
    def createErrIniFile(self):

        if self.errIniFile.isSet(): return

        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        ls ="ls0000"
        stream = STREAMERRORNAME
        ext = ".ini"

        filename = "_".join([runname,ls,stream,host])+ext
        filepath = os.path.join(self.outdir,runname,stream,filename)
        filedir = os.path.join(self.outdir,runname,stream)
        filedir = os.path.join(self.outdir,runname,stream)

        #create stream subdirectory in output if not there
        create_attempts=5
        while not self.createOutputDirs(filedir) and create_attempts>0:
            time.sleep(.1)
            #terminate script if there is no condition
            create_attempts-=1
            if create_attempts==0: self.logger.fatal('Unable to create destination directories for INI file ' + self.infile.basename)

        infile = fileHandler(filepath)
        infile.data = ""
        if infile.writeout(empty=True,verbose=False):
            self.errIniFile.set()

        #'touch' empty Error INI file stream for monitoring
        localmonfilepath = os.path.join(self.tempdir,'mon',filename)
        if not os.path.exists(localmonfilepath):
            try:
                with open(localmonfilepath,'w') as fp:
                    pass
            except:
                self.logger.warning('could not create '+localmonfilepath)

        self.logger.info("created error ini file")


    def processINIfile(self):

        self.logger.info(self.infile.basename)
        infile = self.infile

        localdir,name,ext,filepath = infile.dir,infile.name,infile.ext,infile.filepath
        run,ls,stream = infile.run,infile.ls,infile.stream

        #calc generic local ini path
        filename = "_".join([run,ls,stream,host])+ext
        localfilepath = os.path.join(localdir,filename)
        localmonfilepath = os.path.join(localdir,'mon',filename)
        remotefiledir = os.path.join(self.outdir,run,stream)
        remotefilepath = os.path.join(self.outdir,run,stream,'data',filename)

        #create stream subdirectory in output if not there
        create_attempts=5
        while not self.createOutputDirs(remotefiledir) and create_attempts>0:
            time.sleep(.1+(5-create_attempts)/50.)
            #terminate script if there is no condition
            create_attempts-=1
            if create_attempts==0: self.logger.fatal('Unable to create destination directories for INI file ' + self.infile.basename)

        if not os.path.exists(localmonfilepath):
            try:
                with open(localmonfilepath,'w') as fp:
                    pass
            except:
                self.logger.warning('could not create '+localmonfilepath)

            #check and move/delete ini file
        if not os.path.exists(localfilepath):
            if stream not in self.activeStreams:
                self.activeStreams.append(stream)
                self.streamCounters[stream]=0
            self.infile.moveFile(newpath=localfilepath)
            self.infile.moveFile(newpath=remotefilepath,copy=True,createDestinationDir=False,missingDirAlert=False)
        else:
            self.logger.debug("compare %s , %s " %(localfilepath,filepath))
            if not filecmp.cmp(localfilepath,filepath,False):
                        # Where shall this exception be handled?
                self.logger.warning("Found a bad ini file %s" %filepath)
            else:
                self.infile.deleteFile(silent=True)

        self.createErrIniFile()

    def processDefinitionFile(self):
        run = 'run'+self.run_number.zfill(conf.run_number_padding)
        if "_pid" in self.infile.name:
            #name with pid: copy to local name without pid
            newpath = os.path.join(self.infile.dir,self.infile.name[:self.infile.name.rfind('_pid')])+self.infile.ext
            oldpath = self.infile.filepath
            try:
                os.stat(newpath)
                #skip further action if destination exists
                return
            except:
                pass
            stream = None
            for token in self.infile.name.split('_'):
                if token.startswith('stream'):
                    stream=token
            #find stream token
            #copy to output with rename
            if not stream:
                self.infile.moveFile(os.path.join(outputDir,run,os.path.basename(newpath)),copy=True,adler32=False,
                                   silent=True,createDestinationDir=False,missingDirAlert=False)
            else:
                remotefiledir = os.path.join(outputDir,run,stream)
                #create stream subdirectory in output if not there
                create_attempts=5
                while not self.createOutputDirs(remotefiledir) and create_attempts>0:
                    time.sleep(.1+(5-create_attempts)/50.)
                    #terminate script if there is no condition
                    create_attempts-=1
                    if create_attempts==0: self.logger.fatal('Unable to create destination directories for DEF file ' + self.infile.basename)

                fn = os.path.join(remotefiledir,'data',os.path.basename(newpath))
                self.infile.moveFile(fn, copy=True, adler32=False, silent=True, createDestinationDir=False, missingDirAlert=False)
            #local copy
            self.infile.moveFile(newpath, copy=True, adler32=False, silent=True, createDestinationDir=False, missingDirAlert=False)

            #delete as we will use the one without pid
            try:os.unlink(oldpath)
            except:pass
        else:
            pass

    def processEORFile(self):
        self.logger.info(self.infile.basename)
        self.EOR = self.infile
        self.EOR.esCopy()

    def processCompleteFile(self):
        self.logger.info("received run complete file")
        self.complete = self.infile
        if "abortcomplete" in self.infile.filepath:
            self.stop(timeout=60)
        else:
            self.checkOpenLumis()
            self.stop(timeout=5)

    def checkClosure(self,checkEmpty=True):
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                if not checkEmpty and self.LSHandlerList[key].emptyLS:continue
                return False
        return True

    def getOpenLumis(self):
        openLumis=[]
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                openLumis.append(key)
        return openLumis

    def getNumOpenLumis(self):
        openLumis=0
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                openLumis+=1
        return openLumis

    def checkDestinationDir(self):
        if not os.path.exists(os.path.join(outputDir,'run'+self.run_number.zfill(conf.run_number_padding))):
            self.logger.fatal("Can not find output (destination) directory. Anelastic script will die.")
            self.writeElasticMarker()
            os._exit(1)

    def createOutputEoR(self):

        #make json and moveFile
        totalCount=-1
        #namePrefix = "/run"+str(self.run_number).zfill(conf.run_number_padding)+"_ls0000_"
        eorname = 'run'+self.run_number.zfill(conf.run_number_padding)+"_ls0000_EoR_"+os.uname()[1]+".jsn"
        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        srcName = os.path.join(conf.watch_directory,runname,eorname)
        destName = os.path.join(outputDir,runname,eorname)
        document = {"data":[str(0)]}

        for stream in self.streamCounters.keys():
            document = {"data":[str(self.streamCounters[stream])]}
            break
        try:
            with open(srcName,"w") as fi:
                json.dump(document,fi)
        except: logging.exception("unable to create %r" %srcName)

        f = fileHandler(srcName)
        f.moveFile(destName,createDestinationDir=False,missingDirAlert=False)
        self.logger.info('created local EoR files for output')

    def createBoLS(self,run,ls,stream):
        #create BoLS file in output dir
        bols_file = run+"_"+ls+"_"+stream+"_BoLS.jsn"
        bols_path =  os.path.join(self.outdir,run,stream,bols_file)
        try:
            open(bols_path,'a').close()
        except:
            time.sleep(0.1)
            try:open(bols_path,'a').close()
            except:
                self.logger.warning('unable to create BoLS file for '+ str(ls))
        logger.info("bols file "+ str(bols_path) + " is created in the output")

    def cleanStreamFiles(self):

        if not self.infile.data:
            return False
        try:
            files = self.infile.getFieldByName("Filelist").split(',')
            if len(files) and len(files[0]):
                os.unlink(os.path.join(self.tempdir,os.path.basename(files[0])))
        except:
            pass
        os.remove(self.infile.filepath)
        return True

    def writeElasticMarker(self):
        logger.info('writing abort marker for the elastic script')
        abortpath = os.path.join(self.tempdir,ES_DIR_NAME,'ABORTCOMPLETE')
        try:
            if not os.path.exists(abortpath):
                with open(abortpath,'w') as abortfp:
                    pass
        except Exception as ex:
            logger.warning(str(ex))

    def makeEoLSFile(self, ls):
        thisrun = "run"+self.run_number.zfill(conf.run_number_padding)
        eols_file = thisrun + "_" + ls + "_EoLS.jsn"
        eols_path =  os.path.join(self.tempdir,eols_file)
        with open(eols_path,"w") as fi:
            self.logger.info("Created missing EoLS file "+eols_path)

    def makeBoLSFile(self, ls):
        thisrun = "run"+self.run_number.zfill(conf.run_number_padding)
        bols_file = thisrun + "_ls" + str(ls).zfill(4) + "_BoLS.jsn"
        bols_path =  os.path.join(self.tempdir,bols_file)
        with open(bols_path,"w") as fi:
            self.logger.info("Created missing BoLS file "+bols_path)

    def startDQMHandlerMaybe(self):
        if self.dqmHandler is None:
            self.logger.info('DQM histogram ini file: starting DQM merger...')
            self.dqmHandler = DQMMerger(self.dqmQueue,self.tempdir,self.source)
            if not self.dqmHandler.active:
                self.dqmHandler = None
                self.logger.error('Failed to start DQM merging thread. Histogram stream will be ignored in this run.')
                return False
        return True


    def checkOpenLumis(self):
       
        #check all non-closed lumisections and see if EoLS file needs to be generated
        #all CMSSW processes are terminated at this point because complete file was received previously
        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        lsList = self.LSHandlerList
        eolsCreated=False
        inputpath=None
        for item in lsList.values():
            if not item.EOLS:
                #construct ramdisk path for this
                self.logger.info("checking if EoLS file exists in input for LS "+str(item.ls_num))
                try:
                    eols_file = runname + "_ls" + str(item.ls_num).zfill(4)+"_EoLS.jsn"
                    if os.path.exists(os.path.join(rawinputdir,eols_file)) and not os.path.exists(os.path.join(self.tempdir,eols_file)):
                        self.logger.info("creating EoLS file for LS " + str(item.ls_num) + " to close it on this host if event counts match")
                        with open(os.path.join(self.tempdir,eols_file),'w'):
                            eolsCreated=True
                except Exception as ex:
                    self.logger.warning("Exception checking EoLS file in ramdisk: " + str(ex))
        if eolsCreated:
            #sleep before stopping so that EoLS file is picked up by inotify
            time.sleep(5)



class LumiSectionHandler():
    def __init__(self,parent,run,ls,activeStreams,streamCounters,tempdir,outdir,jsdfile,isEmptyLS):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(ls)
        self.parent=parent
        self.activeStreams = activeStreams
        self.streamCounters = streamCounters
        self.ls = ls
        self.ls_num = int(ls[2:])
        self.run = run
        self.outdir = outdir
        self.tempdir = tempdir
        self.jsdfile = jsdfile

        self.outfileList = []
        self.streamErrorFile = ""
        self.datfileList = []
        self.indexfileList = []
        self.pidList = {}           # {"pid":{"numEvents":num,"streamList":[streamA,streamB]}
        self.EOLS = None               #EOLS file
        self.closed = threading.Event() #True if all files are closed/moved
        self.totalEvent = 0
        self.totalFiles = 0
        self.emptyLumiStreams = None
        self.emptyLS = isEmptyLS
        self.data_size = 0

        if not self.emptyLS:
            self.initOutFiles()
        self.initErrFiles()

    def initOutFiles(self):
        activeStreams,run,ls,tempdir = self.activeStreams,self.run,self.ls,self.tempdir
        ext = ".jsn"
        if not os.path.exists(self.jsdfile):
            self.logger.error("JSD file not found %r" %self.jsdfile)
            return False

        for stream in self.activeStreams:
            outfilename = "_".join([run,ls,stream,host])+ext
            outfilepath = os.path.join(tempdir,outfilename)
            outfile = fileHandler(outfilepath)
            outfile.setJsdfile(self.jsdfile)
            self.outfileList.append(outfile)

    def initErrFiles(self):
        run,ls,tempdir = self.run,self.ls,self.tempdir
        ext = ".jsn"
        if not os.path.exists(self.jsdfile):
            self.logger.error("JSD file not found %r" %self.jsdfile)
            return False

        stream = STREAMERRORNAME
        errfilename = "_".join([run,ls,stream,host])+ext
        errfilepath = os.path.join(tempdir,errfilename)
        errfile = fileHandler(errfilepath)
        errfile.setJsdfile(self.jsdfile)
        self.streamErrorFile = errfile

    def processFile(self,infile):
        self.infile = infile
        filetype = self.infile.filetype

        if filetype == STREAM:
            proc_status = self.processStreamFile()
            if proc_status==False: return
        elif filetype == INDEX: self.processIndexFile()
        elif filetype == EOLS: self.processEOLSFile()
        elif filetype == DAT:
            self.processDATFile()
            return
        elif filetype == PB:
            self.processDATFile()
            return
        elif filetype == CRASH: self.processCRASHFile()
        elif filetype == STREAMDQMHISTOUTPUT: self.processStreamDQMHistoOutput()

        self.checkClosure()

    def processStreamFile(self):

        infile = self.infile
        ls,stream,pid = infile.ls,infile.stream,infile.pid
        outdir = self.outdir

        if pid not in self.pidList:
            processed = infile.getFieldByName("Processed")
            if processed!=0:
                self.logger.critical("Received stream output file with processed events and no seen indices by this process, pid "+str(pid))
                return False
            elif not self.emptyLS:
                #self.logger.info('Empty output in non-empty LS. Deleting output files for stream '+stream)
                if not infile.data:
                    return False
                try:
                    files = infile.getFieldByName("Filelist").split(',')
                    if len(files) and len(files[0]):
                        os.unlink(os.path.join(self.tempdir,os.path.basename(files[0])))
                except:pass
                os.remove(infile.filepath)
                return False

            if not self.emptyLumiStreams:
                self.emptyLumiStreams = ['Error']
                #self.logger.info('empty lumisection detected for pid '+str(pid))
                if self.EOLS:
                    self.outputErrorStream()
                else:
                    self.logger.info('Error stream output not created for empty lumi (' + str(ls) + ') streamError because no EoLS file was present')

            if not infile.data:
                return False
            files = infile.getFieldByName("Filelist").split(',')
            localPidDataPath=None
            if len(files) and len(files[0]):
                #this is not used, streams with no processed events don't have data file
                pidDataName = os.path.basename(files[0])
                localPidDataPath = os.path.join(self.tempdir,pidDataName)

            if stream not in self.emptyLumiStreams:
                self.logger.info("forwarding empty LS and stream:" + self.infile.basename)
                #rename from pid to hostname convention
                outfilename = "_".join([self.run,self.ls,stream,host])+'.jsn'
                outfilepath = os.path.join(self.tempdir,outfilename)
                outfile = fileHandler(outfilepath)
                outfile.setJsdfile(self.jsdfile)
                #copy entries from intput json file
                outfile.data = self.infile.data

                if self.EOLS:
                    self.outputBoLSFile(stream)
                if localPidDataPath:
                    #this is not used, streams with no processed events don't have data file
                    datastem,dataext = os.path.splitext(pidDataName)
                    datafilename = "_".join([self.run,self.ls,stream,host])+dataext
                    outfile.setFieldByName("Filelist",datafilename)
                    localDataPath = os.path.join(self.tempdir,datafilename)
                    self.logger.debug('renaming '+ str(localPidDataPath)+' --> ' + str(localDataPath))
                    os.rename(localPidDataPath,localDataPath)
                    dataFile = fileHandler(localDataPath)
                    remoteDataPath = os.path.join(outdir,outfile.run,outfile.stream,'data',datafilename)
                    if self.EOLS:
                        dataFile.moveFile(remoteDataPath, createDestinationDir=False, missingDirAlert=True)
                else:
                    outfile.setFieldByName("Filelist","")
                remotePath = os.path.join(outdir,outfile.run,outfile.stream,'jsns',outfilename)
                outfile.writeout()
                #remove input file
                os.remove(infile.filepath)
                #only copy data and json file in case EOLS file has been produced for this LS
                #(otherwise this EoR of LS that doesn't exist on BU)
                if self.EOLS:
                    #move empty stream JSON to BU
                    outfile.moveFile(remotePath, createDestinationDir=False,updateFileInfo=False,copy=True)
                    self.parent.mr.notifyMaxLsWithOutput(self.ls_num) #feedback for backpressure control
                    outfile.esCopy(keepmtime=False)
                    outfile.deleteFile(silent=True)

                self.emptyLumiStreams.append(stream)
                if len(self.emptyLumiStreams)==len(self.activeStreams)+1:
                    self.closed.set()
                    if self.EOLS:
                        self.EOLS.esCopy()
            else:
                if localPidDataPath:
                    os.remove(localPidDataPath)
                os.remove(infile.filepath)
            return False

        self.logger.info(self.infile.basename)
        self.infile.checkSources()

        #if self.closed.isSet(): self.closed.clear()
        if infile.data:
            #update pidlist
            try:
                if stream not in self.pidList[pid]["streamList"]: self.pidList[pid]["streamList"].append(stream)
            except KeyError as ex:
                self.logger.exception(ex)
                raise ex

            #update output files
            outfile = next((outfile for outfile in self.outfileList if outfile.stream == stream),False)
            if outfile:
                outfile.merge(infile)
                processed = outfile.getFieldByName("Processed")
                self.logger.info("ls,stream: %r,%r - events %r / %r " %(ls,stream,processed,self.totalEvent))
                infile.esCopy()
                infile.deleteFile(silent=True)
        return True

    def processIndexFile(self):
        self.logger.info(self.infile.basename)
        infile = self.infile
        ls,pid = infile.ls,infile.pid

        #if self.closed.isSet(): self.closed.clear()
        if infile.data:
            numEvents = int(infile.data["data"][0])
            self.totalEvent+=numEvents
            self.totalFiles+=1

            #update pidlist
            if pid not in self.pidList:
                self.pidList[pid] = {"numEvents": 0, "streamList": [], "indexFileList" : []}
            self.pidList[pid]["numEvents"]+=numEvents
            self.pidList[pid]["indexFileList"].append(infile)

            if infile not in self.indexfileList:
                self.indexfileList.append(infile)
                infile.esCopy()
            return True
        #else: TODO:delete raw file in ramdisk if we receive malformed index (process probably crashed while writing it)
        return False

    def processCRASHFile(self):
        #self.logger.info("LS: " + self.ls + " CHECKING ...... ")
        if self.infile.pid not in self.pidList: return True


        #self.logger.info("LS: "+self.ls+" pid: " + self.infile.pid)
        infile = self.infile
        pid = infile.pid
        data  = infile.data.copy()
        numEvents = self.pidList[pid]["numEvents"]
        errCode = data["errorCode"]

        file2merge = fileHandler(infile.filepath)
        file2merge.setJsdfile(self.jsdfile)
        file2merge.setFieldByName("ErrorEvents",numEvents)
        file2merge.setFieldByName("ReturnCodeMask",errCode)
        file2merge.setFieldByName("HLTErrorEvents",numEvents,warning=False)
        #if file2merge.getFieldIndex("transferDestination")>-1:
        #    file2merge.setFieldByName("transferDestination","ErrorArea")

        streamDiff = list(set(self.activeStreams)-set(self.pidList[pid]["streamList"]))
        for outfile in self.outfileList:
            if outfile.stream in streamDiff:
                outfile.merge(file2merge)

        #add crash infos to the streamError output file (only if no streams are merged yet for lumi)
        if len(streamDiff)==len(self.activeStreams):
            inputFileList = [item.name[:item.name.find('_pid')]+".raw" for item in self.pidList[pid]["indexFileList"]]
            inputFileEvents = [int(item.data["data"][0]) for item in self.pidList[pid]["indexFileList"]]
            errorRawFiles=[]
            rawErrorEvents=0
            for index,rawFile in enumerate(inputFileList):
                try:
                    rawname = os.path.join(rawinputdir,rawFile)
                    #test if original file is provided
                    os.stat(rawname)
                    rawFileNew = os.path.splitext(rawFile)[0]+'_'+host+'_'+str(pid)+'.raw'
                    rawnameNew = os.path.join(rawinputdir,rawFileNew)
                    #rename to a unique name containing FU name and CMSSW PID (usable also for tracking other information)
                    os.rename(rawname,rawnameNew)
                    errorRawFiles.append(rawFileNew)
                    rawErrorEvents+=inputFileEvents[index]
                except OSError:
                    self.logger.info('error stream input file '+rawFile+' is gone, possibly already deleted by the process')
                    pass
            file2merge.setFieldByName("ErrorEvents",rawErrorEvents)
            file2merge.setFieldByName("HLTErrorEvents",rawErrorEvents,warning=False)
            inputFileList = ",".join(errorRawFiles)
            self.logger.info("inputFileList: " + inputFileList)
            file2merge.setFieldByName("InputFiles",inputFileList)
            self.streamErrorFile.merge(file2merge)

    def processStreamDQMHistoOutput(self):
        if self.emptyLS:return
        self.logger.info(self.infile.basename)
        stream = self.infile.stream
        outfile = next((outfile for outfile in self.outfileList if outfile.stream == stream),False)
        if outfile and outfile.mergeStage==1: outfile.mergeStage+=1

    def processDATFile(self):
        self.logger.info(self.infile.basename)
        stream = self.infile.stream
        if self.infile not in self.datfileList:
            self.datfileList.append(self.infile)
        try:
          self.data_size+=os.stat(self.infile.filepath).st_size
        except:
          pass

    def processEOLSFile(self):
        self.logger.info(self.infile.basename)
        ls = self.infile.ls
#        try:
#            if os.stat(infile.filepath).st_size>0:
#                #self-triggered inotify event, this lumihandler should be deleted
#                self.closed.set()
#                return False
#        except:
#            pass
        if self.EOLS:
            self.logger.warning("LS %s already closed" %repr(ls))
            return False
        self.EOLS = self.infile
        #copy 'flush' notifier to elastic script area
        #self.logger.debug('FLUSH esCopy')
        self.parent.flush.esCopy()
        return True

    def checkClosure(self):
        if not self.EOLS or self.emptyLS: return False
        outfilelist = self.outfileList[:]
        for outfile in outfilelist:
            stream = outfile.stream
            errEntry = outfile.getFieldByName("ErrorEvents")
            processed = outfile.getFieldByName("Processed")+errEntry
            if processed == self.totalEvent:

                if outfile.filetype==STREAMDQMHISTOUTPUT:
                    if outfile.mergeStage==0:
                        #give to the merging thread
                        outfile.mergeStage+=1
                        outfile.lsHandler = self
                        if self.parent.startDQMHandlerMaybe():
                          self.parent.dqmQueue.put(outfile)
                        continue
                    elif outfile.mergeStage==1:
                        #still merging
                        continue

                self.streamCounters[stream]+=processed
                self.logger.info("%r,%r complete" %(self.ls,outfile.stream))

                #create BoLS file in output dir
                self.outputBoLSFile(stream)

                #move all dat files in rundir
                datfilelist = self.datfileList[:]

                def writeoutError(fobj,targetpath,removeOutput=True):
                        procevts = int(fobj.getFieldByName("Processed"))
                        errevts = int(fobj.getFieldByName("ErrorEvents"))
                        fobj.setFieldByName("Processed","0")
                        fobj.setFieldByName("Accepted","0")
                        fobj.setFieldByName("ErrorEvents",str(procevts+errevts))
                        fobj.setFieldByName("Filelist","")
                        fobj.setFieldByName("Filesize","0")
                        fobj.setFieldByName("FileAdler32","-1")
                        fobj.writeout()
                        try:os.remove(targetpath)
                        except:pass

                #no dat files in case of json data stream'
                if outfile.isJsonDataStream()==False:
                    foundDat=False

                    for datfile in datfilelist:
                        if datfile.stream == stream:
                            foundDat=True
                            newfilepath = os.path.join(self.outdir,datfile.run,datfile.stream,'data',datfile.basename)
                            (filestem,ext)=os.path.splitext(datfile.filepath)
                            checksum_file = filestem+'.checksum'
                            doChecksum=conf.output_adler32
                            checksum_cmssw=None
                            try:
                                with open(checksum_file,"r") as fi:
                                    checksum_cmssw = int(fi.read())
                            except:
                                doChecksum=False
                                pass
                            #test
                            if processed==errEntry:
                                self.logger.info("all events are error events for stream "+stream+":"+str(errEntry))
                                try:os.remove(datfile.filepath)
                                except:pass
                                doChecksum=False
                            else:
                                (status,checksum)=datfile.moveFile(newfilepath,adler32=doChecksum,createDestinationDir=False,missingDirAlert=True)
                            checksum_success=True
                            if doChecksum and status:
                                if checksum_cmssw!=checksum&0xffffffff:
                                    checksum_success=False
                                    try:
                                        self.logger.fatal("checksum mismatch for "+ datfile.filepath  \
                                                          +" expected adler32:" + str(checksum_cmssw) + ' size:'+ outfile.getFieldByName("Filesize") \
                                                          + " , got adler32:" + str(checksum)+' size:'+os.stat(datfile.filepath).st_size)
                                    except:
                                        self.logger.fatal("checksum mismatch for "+ datfile.filepath)
                                    #failed checksum, assign everything to error events and try to delete the file
                                    writeoutError(outfile,newfilepath)
                            if checksum_cmssw!=None and checksum_success==True:
                                outfile.setFieldByName("FileAdler32",str(checksum_cmssw&0xffffffff))
                                outfile.writeout()
                            try:
                                os.unlink(filestem+'.checksum')
                            except:pass
                            self.datfileList.remove(datfile)
                    if not foundDat and errEntry<self.totalEvent:
                        success = False
                        destinationpath = os.path.join(self.outdir,outfile.run,outfile.stream,"data",outfile.name+".dat")
                        #destinationpath = os.path.join(self.outdir,outfile.run,outfile.stream,outfile.name+".dat")
                        try:
                            try:
                                os.stat(os.path.join(self.tempdir,outfile.run,outfile.name+".dat")).st_size
                                self.logger.warning('file exists, but not previously detected? '+os.path.join(self.tempdir,outfile.run,outfile.name+".dat"))
                            except:
                                pass
                            success,copy_size = outfile.mergeDatInputs(destinationpath,conf.output_adler32)
                            self.data_size +=copy_size
                            #reset expiration timestamp as we don't want to set lumi_bw to zero if files are still being copied
                            self.parent.mr.data_size_last_update = time.time()
			    #os.rename(destinationpath_tmp,destinationpath)
                            outfile.writeout()
                            #test
                            os.stat(destinationpath).st_size
                        except Exception as ex:
                            self.logger.fatal("Failed micro-merge: "+destinationpath)
                            self.logger.exception(ex)
                            success=False
                        if not success:
                            writeoutError(outfile,destinationpath)

                #move output json file in rundir
                newfilepath = os.path.join(self.outdir,outfile.run,outfile.stream,'jsns',outfile.basename)

                #fill JSON with error event info if merging is failing for special streams
                if not outfile.mergeAndMoveJsnDataMaybe(os.path.join(self.outdir,outfile.run,outfile.stream,'data')):
                    self.logger.error('jsndata stream merging failed. Assigning as error events')
                    writeoutError(outfile,'',False)

                result,checksum=outfile.moveFile(newfilepath,copy=True,createDestinationDir=False,updateFileInfo=False)
                if not result:
                    writeoutError(outfile,'',False)
                self.outfileList.remove(outfile) #is completed
                self.parent.mr.notifyMaxLsWithOutput(self.ls_num) #feedback for backpressure control
                outfile.esCopy(keepmtime=False)
                outfile.deleteFile(silent=True)


        if not self.outfileList and not self.closed.isSet():

            #update queue status statistics
            if self.parent.mr.data_size_ls_num<self.ls_num:
                self.parent.mr.data_size_ls_num = self.ls_num
                self.parent.mr.data_size_val = self.data_size
                self.parent.mr.data_size_last_update = time.time()

            #self.EOLS.deleteFile()

            #delete all index files
            for item in self.indexfileList:
                item.deleteFile(silent=True)

            self.outputErrorStream()

            #close lumisection if all streams are closed
            self.logger.info("closing %r" %self.ls)
            if self.EOLS:
                self.EOLS.esCopy()
            else:
                self.logger.error("EOLS file missing for lumisection "+str(self.ls))
            #self.writeLumiInfo()
            self.closed.set()
            #update EOLS file with event processing statistics

    def outputErrorStream(self):
        #moving streamError file
        self.logger.info("Writing streamError file ")
        errfile = self.streamErrorFile
        #create BoLS file in output dir
        self.outputBoLSFile(errfile.stream)

        numErr = errfile.getFieldByName("ErrorEvents") or 0
        total = self.totalEvent
        errfile.setFieldByName("Processed", str(total - numErr))
        errfile.setFieldByName("FileAdler32", "-1", warning=False)
        errfile.setFieldByName("TransferDestination","ErrorArea",warning=False)
        try:errfile.setFieldByName("MergeType","RAW",warning=False)
        except:pass
        errfile.writeout()
        newfilepath = os.path.join(self.outdir,errfile.run,errfile.stream,'jsns',errfile.basename)
        #store in ES if there were any errors
        result,ch=errfile.moveFile(newfilepath,createDestinationDir=False,copy=True,updateFileInfo=False)
        if not result:
            errfile.setFieldByName("Processed", str(0))
            errfile.setFieldByName("ErrorEvents", str(total))
            errfile.writeout()
        errfile.esCopy(keepmtime=False)
        errfile.deleteFile(silent=True)


    def outputBoLSFile(self,stream):
        #create BoLS file in output dir
        bols_file = str(self.run)+"_"+self.ls+"_"+stream+"_BoLS.jsn"#use join
        bols_path =  os.path.join(self.outdir,self.run,stream,'jsns',bols_file)
        try:
            open(bols_path,'a').close()
        except:
            time.sleep(0.1)
            try:open(bols_path,'a').close()
            except:
                self.logger.warning('unable to create BoLS file for '+ self.ls + ' and stream '+ stream + ' in the output')
                return
        logger.info("bols file "+ str(bols_path) + " is created in the output")


    def writeLumiInfo(self):
        #populating EoL information back into empty EoLS file (disabled)
        document = {'data':[str(self.totalEvent),str(self.totalFiles),str(self.totalEvent)],
                    'definition':'',
                    'source':os.uname()[1]}
        try:
            if os.stat(self.EOLS.filepath).st_size==0:
                with open(self.EOLS.filepath,"w+") as fi:
                    json.dump(document,fi,sort_keys=True)
        except: logging.exception("unable to write to " %self.EOLS.filepath)


class FileEvent:
    def __init__(self,fullpath,eventtype):
        self.mask = eventtype
        self.fullpath = fullpath

class DQMMerger(threading.Thread):

    def __init__(self,queue,outDir,source):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dqmQueue = queue
        self.outDir = outDir
        self.abort = False
        self.active=False
        self.finish=False
        self.skipAll=False
        self.source=source
        try:
            mergeEnabled = True
            p = subprocess.Popen('fastHadd',shell=True,stdout=subprocess.PIPE)
            p.wait()
            if p.returncode!=1 and p.returncode!=0:
                self.logger.error('fastHadd exit code:'+str(p.returncode))
                mergeEnabled=False
        except Exception as ex:
            mergeEnabled = False
            self.logger.error('fastHadd test failed!')
            self.logger.exception(ex)
        if not mergeEnabled:
            return
        else:
            self.logger.info('fastHadd binary tested successfully')
        self.start()
        self.active=True
 
    def run(self):
        while self.abort == False:
            try:
                dqmJson = self.dqmQueue.get(True,0.5)
                outpbname = dqmJson.mergeDQM(self.outDir,setAsError=self.skipAll)
                try:
                  if len(outpbname):
                    outpbpath = os.path.join(self.outDir,outpbname)
                    os.stat(outpbpath)
                    self.source.put(FileEvent(outpbpath,PB))
                except OSError as ex:
                  self.logger.warning('pb file check: '+str(ex))
                #inject into main queue so that it gets closed (todo: think of special type that will
                self.source.put(FileEvent(dqmJson.filepath,STREAMDQMHISTOUTPUT))
            except Queue.Empty as e:
                if self.finish:break
                continue
            except KeyboardInterrupt as e:
                break

    def setSkipAll(self):
        if self.skipAll:return
        self.skipAll=True
        self.logger.info('Parent thread has request to skip merging remaining Histograms')
    def waitFinish(self,time=None):
        self.finish=True
        if time:
            self.join(time)
        else:
            self.join()

    def abortMerging(self):
        self.abort = True
        self.join()


if __name__ == "__main__":

    import procname
    procname.setprocname('anelastic')

    conf=initConf()

    try:run_str = ' - RUN:'+sys.argv[1].zfill(conf.run_number_padding)
    except:run_str = ''

    logging.basicConfig(filename=os.path.join(conf.log_dir,"anelastic.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(funcName)s'+run_str+' - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()

    try: 
      run_number = sys.argv[1]
      dirname = sys.argv[2]
      rawinputdir = sys.argv[3]
    except Exception as ex:
      logging.exception(ex)
      os._exit(1)

    dirname = os.path.basename(os.path.normpath(dirname))
    watchDir = os.path.join(conf.watch_directory,dirname)
    outputDir = sys.argv[4]
    outputRunDir = os.path.join(outputDir,'run'+run_number.zfill(conf.run_number_padding))

    mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO  # watched events
    logger.info("starting anelastic for "+dirname)
    mr = None

    es_dir_name = os.path.join(watchDir,ES_DIR_NAME)
    #make temp dir if we are here before elastic.py
    try:
        os.makedirs(es_dir_name)
    except OSError:
        pass

    try:
        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        mr.setQueueStatusPath(os.path.join(watchDir,"open","queue_status.jsn"),os.path.join(watchDir,"mon","queue_status.jsn"))
        mr.startStatsCollector()
        mr.register_inotify_path(watchDir,mask)
        mr.start_inotify()

        #ensuring that output run dir gets created (after inotify init)
        attempts = 3
        while attempts>0:
            attempts-=1
            try:
                os.makedirs(outputRunDir)
                logger.info("created "+outputRunDir)
                break
            except OSError as ex:
                if ex.errno == 17: break
                logging.exception(ex)
                time.sleep(.5)
                continue
            except Exception as ex:
                logging.exception(ex)
                time.sleep(.5)
                continue

        #starting lsRanger thread
        ls = LumiSectionRanger(mr,watchDir,outputDir,run_number)
        ls.setSource(eventQueue)
        ls.start()

        logging.info("Closing notifier")
        mr.stop_inotify()

    except Exception,e:
        logger.exception(e)
        logging.info("Closing notifier")
        if mr is not None:
            mr.stop_inotify()
        os._exit(1)

    logging.info("Quit")
    sys.exit(0)

