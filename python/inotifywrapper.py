#
# wrapper around the python-inotify library
#

import logging
import threading
import watcher
import _inotify as inotify
#import select
#import sys
#from time import sleep

class InotifyWrapper(threading.Thread):

    def __init__(self, parent, recursive = False, logger = None):
        threading.Thread.__init__(self)
        self.logger = logger or logging.getLogger(__name__)

        self.parent_ = parent
        self.w = None
        self.quit = False

        self.threshold_ = None
        self.timeout_ = None
        self.delay_ = None

        if recursive == True:
            self.logger.debug("RECURSIVE_MODE")
            self.w = watcher.AutoWatcher()
        else:
            self.logger.debug("NON-RECURSIVE_MODE")
            self.w = watcher.Watcher()

        self.recursiveMode = recursive

    def setThreshold(self,threshold,timeout,delay):
        #currently ignored
        self.threshold_ = threshold
        self.timeout_ = timeout
        self.delay_ = delay

    def registerPath(self,path,mask):
        try:
            if self.recursiveMode == True:
                self.w.add_all(path,mask,reportError)
            else:
                self.w.add(path,mask)
        except OSError, err:
            self.logger.error('inotify wrapper exception: ' + err.strerror)
            raise err

    def reportError(self,err):
        self.logger.error('error registering inotify path ' + str(err) +', ignoring')

    def run(self):
        while self.quit == False:
            try:
                for event in self.w.read():
                    if self.quit == True:
                        break
                    #note:add more events in case needed
                    try:
                        if event.mask & inotify.IN_CREATE:
                            self.parent_.process_IN_CREATE(event)
                        elif event.mask & inotify.IN_MODIFY:
                            self.parent_.process_IN_MODIFY(event)
                        elif event.mask & inotify.IN_MOVED_TO:
                            self.parent_.process_IN_MOVED_TO(event)
                        elif event.mask & inotify.IN_DELETE:
                            self.parent_.process_IN_DELETE(event)
                        elif event.mask & inotify.IN_CLOSE_WRITE:
                            self.parent_.process_IN_CLOSE_WRITE(event)
                        else:
                            self.parent_.process_default(event)
                    except AttributeError, err:
                        #parent does not implement the function
                        self.parent_.process_default(event)
            except Exception, ex:
                if self.quit == False: self.logger.exception("exception in inotify run thread:")

    def stop(self):
        self.quit = True
        try:
            for wd in self.w._wds:
                inotify.remove_watch(self.w.fd, wd)
        except Exception, ex:
            pass
        self.w.close()
        self.logger.debug('closed inotify fd')

#more advanced use
#    def run(self):
#        poll = select.poll()
#        poll.register(w, select.POLLIN)
#        thresholdObj = watcher.Threshold(w, threshold_)
#
#        while quit == False:
#            events = poll.poll(timeout)
#            sleep(0.01)#wait for events to accumulate
#            nread = 0
#            if thresholdObj() or not events:
#                #print 'reading,', threshold.readable(), 'bytes available'
#                for event in w.read(0):
#                     if event.type == xy:
#                         parent.process_IN_MOVE_TO(event)
