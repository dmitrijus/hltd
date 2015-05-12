#!/bin/env python

import sys,traceback
import os,signal

import logging
import pyinotify
import threading
import Queue

import elasticBand
import hltdconf

from anelastic import *
from aUtils import *




class killer():
    stoprequest = threading.Event()
    emptyQueue = threading.Event()
    source = False
    infile = False
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
        self.logger.info("Start main loop") 
        while not self.stoprequest.isSet() :
            if self.source:
                try:
                    event = self.source.get(True,0.5) #blocking with timeout
                    self.eventtype = event.maskname
                    self.infile = fileHandler(event.pathname)
                    self.emptyQueue.clear()
                    self.process() 
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set() 
            else:
                time.sleep(0.5)

        self.logger.info("Stop main loop")


    def setSource(self,source):
        self.source = source

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        
        if self.infile.filetype == INDEX and self.eventtype == "IN_CLOSE_WRITE" and self.infile.ls == "ls0002" :
            time.sleep(0.5)            
            pid = int(self.infile.pid[3:])
            os.kill(pid,signal.SIGKILL)
            print "killed %r for %r " %(pid,self.infile.filepath)
            self.stop()

        


             


if __name__ == "__main__":
    logging.basicConfig(filename="/tmp/elastic.log",
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s-%(name)s.%(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    #sys.stderr = stdErrorLog()
    #sys.stdout = stdOutLog()


    #signal.signal(signal.SIGINT, signalHandler)
    
    eventQueue = Queue.Queue()

    conf=hltdconf.hltdConf('/etc/hltd.conf')
    dirname = sys.argv[1]
    dirname = os.path.basename(os.path.normpath(dirname))
    watchDir = os.path.join(conf.watch_directory,dirname)
    #outputDir = conf.micromerge_output

    

    mask = pyinotify.IN_CLOSE_WRITE
    logger.info("starting elastic for "+dirname)
    try:
        #starting inotify thread
        wm = pyinotify.WatchManager()
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        notifier = pyinotify.ThreadedNotifier(wm, mr)
        notifier.start()
        wdd = wm.add_watch(watchDir, mask, rec=False, auto_add =True)

        

        #starting elasticCollector thread
        k = killer()
        k.setSource(eventQueue)
        k.start()

    except KeyboardInterrupt as e:
        logger.exception(e)
        print traceback.format_exc()
        logger.error("when processing files from directory "+dirname)

    print "Closing notifier"
    notifier.stop()

    print "Quit"
    sys.exit(0)
