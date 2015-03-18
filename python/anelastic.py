#!/bin/env python

import sys,traceback
import os
import time
import shutil
import subprocess

import filecmp
from inotifywrapper import InotifyWrapper
import _inotify as inotify
import threading
import Queue
import simplejson as json
import logging


from hltdconf import *
from aUtils import *


class LumiSectionRanger():
    host = os.uname()[1]        
    def __init__(self,mr,tempdir,outdir,run_number):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dqmHandler = None
        self.dqmQueue = Queue.Queue()
        self.mr = mr
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()  
        self.errIniFile = threading.Event()  
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
        self.flush = None

    def join(self, stop=False, timeout=None):
        if stop: self.stop()
        super(LumiSectionRanger, self).join(timeout)

        #remove for threading
    def start(self):
        self.run()

    def stop(self,timeout=60):
        self.useTimeout=timeout
        self.stoprequest.set()

    def setSource(self,source):
        self.source = source

    def run(self):
        self.logger.info("Start main loop")
        open(os.path.join(watchDir,'flush'),'w').close()
        self.flush = fileHandler(os.path.join(watchDir,'flush'))
        endTimeout=-1
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet() and self.checkClosure()):
            if self.source:
                try:
                    event = self.source.get(True,0.5) #blocking with timeout
                    self.eventtype = event.mask
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
                            self.logger.error('Starting run with input directory missing. anelastic script will die.')
                            os._exit(1)
                except:
                    pass

        if self.checkClosure()==False:
            self.logger.error('not all lumisections were closed on exit!')
            try:
                self.logger.error('open lumisections are: '+str(self.getOpenLumis()))
            except:
                pass

        self.complete.esCopy()
        #generate and move EoR completition file
        self.createOutputEoR()

        self.logger.info("joining DQM merger thread")
        self.dqmHandler.waitFinish()
        self.logger.info("Stop main loop")

        #send the fileEvent to the proper LShandlerand remove closed LSs, or process INI and EOR files
    
    def process(self):
        
        filetype = self.infile.filetype
        if filetype in [STREAM,STREAMDQMHISTOUTPUT,INDEX,EOLS,DAT,PB]:
            run,ls = (self.infile.run,self.infile.ls)
            key = (run,ls)
            ls_num=int(ls[2:])
            if filetype == EOLS :
                if self.maxReceivedEoLS<ls_num:
                    self.maxReceivedEoLS=ls_num
                self.mr.notifyLumi(ls_num,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
            if key not in self.LSHandlerList:
                if ls_num in self.ClosedEmptyLSList:
                    if filetype in [STREAM]:
                        self.cleanStreamFiles()
                    return
                isEmptyLS = True if filetype not in [INDEX] else False
                self.LSHandlerList[key] = LumiSectionHandler(self,run,ls,self.activeStreams,self.streamCounters,self.tempdir,self.outdir,self.jsdfile,isEmptyLS)
                if filetype not in [INDEX]:
                    self.LSHandlerList[key].emptyLS=True
                self.mr.notifyLumi(None,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
            lsHandler = self.LSHandlerList[key]
            lsHandler.processFile(self.infile)
            if lsHandler.closed.isSet():
                if self.maxClosedLumi<ls_num:
                    self.maxClosedLumi=ls_num
                    self.mr.notifyLumi(None,self.maxReceivedEoLS,self.maxClosedLumi,self.getNumOpenLumis())
                #keep history of closed empty lumisections
                if lsHandler.emptyLS:
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

    def processCRASHfile(self):

        #send CRASHfile to every LSHandler
        lsList = self.LSHandlerList
        basename = self.infile.basename
        errCode = self.infile.data["errorCode"]
        self.logger.info("%r with errcode: %r" %(basename,errCode))
        for item in lsList.values():
            item.processFile(self.infile)
    
    def createErrIniFile(self):

        if self.errIniFile.isSet(): return 

        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        ls = ZEROLS
        stream = STREAMERRORNAME
        ext = ".ini"

        filename = "_".join([runname,ls,stream,self.host])+ext
        filepath = os.path.join(self.outdir,runname,filename)
        infile = fileHandler(filepath)
        infile.data = ""
        infile.writeout(True)
        self.errIniFile.set()

        self.logger.info("created error ini file")


    def processINIfile(self):

        self.logger.info(self.infile.basename)
        infile = self.infile 

        localdir,name,ext,filepath = infile.dir,infile.name,infile.ext,infile.filepath
        run,ls,stream = infile.run,infile.ls,infile.stream

        #start DQM merger thread
        if STREAMDQMHISTNAME.upper() in stream.upper():
            if self.dqmHandler is None:
                self.logger.info('DQM histogram ini file: starting DQM merger...')
                self.dqmHandler = DQMMerger(self.dqmQueue)
                if not self.dqmHandler.active:
                    self.dqmHandler = None
                    self.logger.error('Failed to start DQM merging thread. Histogram stream will be ignored in this run.')
                    return

        #calc generic local ini path
        filename = "_".join([run,ls,stream,self.host])+ext
        localfilepath = os.path.join(localdir,filename)
        remotefilepath = os.path.join(self.outdir,run,filename)


            #check and move/delete ini file
        if not os.path.exists(localfilepath):
            if stream not in self.activeStreams:
                self.activeStreams.append(stream)
                self.streamCounters[stream]=0
            self.infile.moveFile(newpath = localfilepath)
            self.infile.moveFile(newpath = remotefilepath,copy = True,createDestinationDir=False,missingDirAlert=False)
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
          self.infile.moveFile(newpath,copy = True,adler32=False,silent=True,createDestinationDir=False,missingDirAlert=False)
          #delete as we will use the one without pid
          try:os.unlink(oldpath)
          except:pass
        else:
          #name with pid: copy to output
          self.infile.moveFile(os.path.join(outputDir,run,self.infile.basename),copy = True,adler32=False,
                               silent=True,createDestinationDir=False,missingDirAlert=False)
          
    #def createEOLSFile(self,ls):
    #    eolname = os.path.join(self.tempdir,'run'+self.run_number.zfill(conf.run_number_padding)+"_"+ls+"_EoLS.jsn")
    #    try:
    #        os.stat(eolname)
    #    except OSError:
    #        try:
    #            with open(eolname,"w") as fi:
    #                self.logger.warning("EOLS file "+eolname+" was not present. Creating it by hltd.")
    #        except:pass
            

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
            self.stop(timeout=5)

    def checkClosure(self):
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
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
        bols_path =  os.path.join(self.outdir,run,bols_file)
        try:
           open(bols_path,'a').close()
        except:
           time.sleep(0.1)
           try:open(bols_path,'a').close()
           except:
               self.logger.warning('unable to create BoLS file for ls ', ls)
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


class LumiSectionHandler():
    host = os.uname()[1]
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
            outfilename = "_".join([run,ls,stream,self.host])+ext
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
        errfilename = "_".join([run,ls,stream,self.host])+ext
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
        self.logger.info(self.infile.basename)

        infile = self.infile
        ls,stream,pid = infile.ls,infile.stream,infile.pid
        outdir = self.outdir
 
        #fastHadd was not detected, delete files from histogram stream
        if stream=="streamDQMHistograms" and self.parent.dqmHandler==None:
            try:
                (filestem,ext)=os.path.splitext(infile.filepath)
                os.remove(filestem + '.pb')
                infile.deleteFile(silent=True)
            except:pass
            return True
        
        if pid not in self.pidList:
            processed = infile.getFieldByName("Processed")
            if processed != 0 :
                self.logger.critical("Received stream output file with processed events and no seen indices by this process, pid "+str(self.infile.pid))
                return False
            elif not self.emptyLS:
                self.logger.info('Empty output in non-empty LS. Deleting output files for stream '+stream)
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
                self.logger.info("pid %r not in pidlist as expected for ls %r. Empty lumisection. ")
                self.outputErrorStream()

            if not infile.data:
                return False
            files = infile.getFieldByName("Filelist").split(',')
            localPidDataPath=None
            if len(files) and len(files[0]):
                pidDataName = os.path.basename(files[0])
                localPidDataPath = os.path.join(self.tempdir,pidDataName)
 
            if stream not in self.emptyLumiStreams:
                #rename from pid to hostname convention
                outfilename = "_".join([self.run,self.ls,stream,self.host])+'.jsn'
                outfilepath = os.path.join(self.tempdir,outfilename)
                outfile = fileHandler(outfilepath)
                outfile.setJsdfile(self.jsdfile)
                #copy entries from intput json file
                outfile.data = self.infile.data 

                if localPidDataPath:
                    datastem,dataext = os.path.splitext(pidDataName)
                    datafilename = "_".join([self.run,self.ls,stream,self.host])+dataext
                    outfile.setFieldByName("Filelist",datafilename)
                    localDataPath = os.path.join(self.tempdir,datafilename)
                    self.logger.debug('renaming '+ str(localPidDataPath)+' --> ' + str(localDataPath))
                    os.rename(localPidDataPath,localDataPath)
                    dataFile = fileHandler(localDataPath)
                    remoteDataPath = os.path.join(outdir,datafilename)
                    dataFile.moveFile(remoteDataPath, createDestinationDir=True, missingDirAlert=True)
                else:
                    outfile.setFieldByName("Filelist","")

                remotePath = os.path.join(outdir,infile.basename)
                outfile.writeout()
                #remove intfile
                os.remove(infile.filepath)
                self.outputBoLSFile(stream)
                #TODO:esCopy file after copy to output and before delete (do in moveFile)
                outfile.esCopy()
                outfile.moveFile(remotePath, createDestinationDir=True, missingDirAlert=True)
                self.emptyLumiStreams.append(stream)
                if len(self.emptyLumiStreams)==len(self.activeStreams):
                  self.closed.set()
                  self.EOLS.esCopy()
            else:
                if localPidDataPath:
                    os.remove(localPidDataPath)
                os.remove(infile.filepath)
            return False
        
        self.infile.checkSources()

        #if self.closed.isSet(): self.closed.clear()
        if infile.data:
            #update pidlist
            try:
                if stream not in self.pidList[pid]["streamList"]: self.pidList[pid]["streamList"].append(stream)
            except KeyError as ex:
                #this case can be ignored for DQM stream (missing fastHadd)
                if STREAMDQMHISTNAME.upper() not in stream.upper():
                    self.logger.exception(ex)
                    raise(ex)
                return True

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
                 os.stat(os.path.join(rawinputdir,rawFile))
                 errorRawFiles.append(rawFile)
                 rawErrorEvents+=inputFileEvents[index]
               except OSError:
                 self.logger.info('error stream input file '+rawFile+' is gone, possibly already deleted by the process')
                 pass
            file2merge.setFieldByName("ErrorEvents",rawErrorEvents)
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
                #no dat files in case of json data stream'
                if outfile.isJsonDataStream()==False:
                  for datfile in datfilelist:
                    if datfile.stream == stream:
                        newfilepath = os.path.join(self.outdir,datfile.run,datfile.basename)
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
                                procevts = int(outfile.getFieldByName("Processed"))
                                errevts = int(outfile.getFieldByName("ErrorEvents"))
                                outfile.setFieldByName("Processed","0")
                                outfile.setFieldByName("Accepted","0")
                                outfile.setFieldByName("ErrorEvents",str(procevts+errevts))
                                outfile.setFieldByName("Filelist","")
                                outfile.setFieldByName("Filesize","0")
                                outfile.setFieldByName("FileAdler32","-1")
                                outfile.writeout()
                                try:os.remove(newfilepath)
                                except:pass
                        if checksum_cmssw!=None and checksum_success==True:
                                outfile.setFieldByName("FileAdler32",str(checksum_cmssw&0xffffffff))
                                outfile.writeout()
                        try:
                            os.unlink(filestem+'.checksum')
                        except:pass
                        self.datfileList.remove(datfile)

                #move output json file in rundir
                newfilepath = os.path.join(self.outdir,outfile.run,outfile.basename)

                #do not copy data if this is jsn data stream and json merging fails
                if outfile.mergeAndMoveJsnDataMaybe(os.path.join(self.outdir,outfile.run))==False:return

                outfile.esCopy()
                result,checksum=outfile.moveFile(newfilepath,createDestinationDir=False)
                if result:
                  self.outfileList.remove(outfile)
 
                
        if not self.outfileList and not self.closed.isSet():
            #self.EOLS.deleteFile()

            #delete all index files
            for item in self.indexfileList:
                item.deleteFile(silent=True)

            self.outputErrorStream()

            #close lumisection if all streams are closed
            self.logger.info("closing %r" %self.ls)
            self.EOLS.esCopy()
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
            errfile.setFieldByName("Processed", str(total - numErr) )
            errfile.setFieldByName("FileAdler32", "-1", warning=False)
            errfile.setFieldByName("TransferDestination","ErrorArea",warning=False)
            errfile.writeout()
            newfilepath = os.path.join(self.outdir,errfile.run,errfile.basename)
            errfile.moveFile(newfilepath,createDestinationDir=False)

    def outputBoLSFile(self,stream):
        
            #create BoLS file in output dir
            bols_file = str(self.run)+"_"+self.ls+"_"+stream+"_BoLS.jsn"#use join
            bols_path =  os.path.join(self.outdir,self.run,bols_file)
            try:
                open(bols_path,'a').close()
            except:
                time.sleep(0.1)
                try:open(bols_path,'a').close()
                except:
                    self.logger.warning('unable to create BoLS file for ls ', self.ls)
            logger.info("bols file "+ str(bols_path) + " is created in the output")



    def writeLumiInfo(self):
        #populating EoL information back into empty EoLS file (disabled)
        document = { 'data':[str(self.totalEvent),str(self.totalFiles),str(self.totalEvent)],
                     'definition':'',
                     'source':os.uname()[1] }
        try:
            if os.stat(self.EOLS.filepath).st_size==0:
                with open(self.EOLS.filepath,"w+") as fi:
                    json.dump(document,fi,sort_keys=True)
        except: logging.exception("unable to write to " %self.EOLS.filepath)


class DQMMerger(threading.Thread):

    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dqmQueue = queue
        self.command = 'fastHadd'
        self.threadEvent = threading.Event()
        self.abort = False
        self.active=False
        self.finish=False
        try:
            mergeEnabled = True
            p = subprocess.Popen(self.command,shell=True,stdout=subprocess.PIPE)
            p.wait()
            if p.returncode!=1 and p.returncode!=0:
                self.logger.error('fastHadd exit code:'+str(p.returncode))
                mergeEnabled=False
        except Exception as ex:
            mergeEnabled = False
            self.logger.error('fastHadd not present or not working:')
            self.logger.exception(ex)
        if mergeEnabled == False:return
        else: self.logger.info('fastHadd binary tested successfully')
        self.start()
        self.active=True

    def run(self):
        while self.abort == False:
            try:
                dqmJson = self.dqmQueue.get(True,0.5) #blocking with timeout
                #self.emptyQueue.clear()
                self.process(dqmJson) 
            except Queue.Empty as e:
                if self.finish==True:break
                continue
            except KeyboardInterrupt as e:
                break

    def process(self,outfile):
       outputName,outputExt = os.path.splitext(outfile.basename)
       outputName+='.pb'
       fullOutputPath = os.path.join(watchDir,outputName)
       command_args = [self.command,"add","-o",fullOutputPath]

       totalEvents = outfile.getFieldByName("Processed")+outfile.getFieldByName("ErrorEvents")

       processedEvents = 0
       acceptedEvents = 0
       errorEvents = 0

       numFiles=0
       inFileSizes=[]
       for f in outfile.inputs:
#           try:
           fname = f.getFieldByName('Filelist')
           fullpath = os.path.join(watchDir,fname)
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
       exitCodes =  outfile.getFieldByName('ReturnCodeMask')
       if numFiles>=0:
           p = subprocess.Popen(command_args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
           p.wait()
           if p.returncode!=0:
               self.logger.error('fastHadd returned with exit code '+str(p.returncode)+' and response: ' + str(p.communicate()) + '. Merging parameters given:'+str(command_args) +' ,file sizes(B):'+str(inFileSizes))
               #DQM more verbose debugging
               try:
                   filesize = os.stat(fullOutputPath).st_size
                   self.logger.error('fastHadd reported to fail at merging, while output pb file exists! '+ fullOutputPath + ' with size(B): '+str(filesize))
               except:
                   pass
               outfile.setFieldByName('ReturnCodeMask', str(p.returncode))
               hasError=True

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
           fullOutputPath=""
           filesize=0

       #correct for the missing event count in input file (when we have a crash)
       if totalEvents>processedEvents+errorEvents: errorEvents += totalEvents - processedEvents - errorEvents

       outfile.setFieldByName('Processed',str(processedEvents))
       outfile.setFieldByName('Accepted',str(acceptedEvents))
       outfile.setFieldByName('ErrorEvents',str(errorEvents))
       outfile.setFieldByName('Filelist',outputName)
       outfile.setFieldByName('Filesize',str(filesize))
       outfile.esCopy()
       outfile.writeout()
 

    def waitCompletition(self):
        self.join()

    def waitFinish(self):
        self.finish=True
        self.join()

    def abortMerging(self):
        self.abort = True
        self.join()
    
        


if __name__ == "__main__":

    import procname
    procname.setprocname('anelastic')

    conf=initConf()

    logging.basicConfig(filename=os.path.join(conf.log_dir,"anelastic.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()
    
    dirname = sys.argv[1]
    run_number = sys.argv[2]
    rawinputdir = sys.argv[3]
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


    

    
