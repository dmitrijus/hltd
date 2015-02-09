import sys, os, time, atexit
import subprocess
from signal import SIGINT
from aUtils import * #for stdout and stderr redirection
import ConfigParser
import re

class Daemon2:
    """
    A generic daemon class.

    Usage: subclass the Daemon2 class and override the run() method

    reference:
    http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

    attn: May change in the near future to use PEP daemon
    """

    def __init__(self, processname, instance, confname=None, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                      self.stdin = stdin
                      self.stdout = stdout
                      self.stderr = stderr
                      self.processname = processname
                      self.instance = instance
                      if confname==None:confname=processname
                      if instance=="main": 
                          instsuffix=""
                          self.instancemsg=""
                      else:
                          instsuffix="-"+instance
                          self.instancemsg=" instance"+instance

                      self.pidfile = "/var/run/" + processname + instsuffix + ".pid"
                      self.conffile = "/etc/" + confname + instsuffix + ".conf"
                      self.lockfile = '/var/lock/subsys/'+processname + instsuffix



    def daemonize(self):

        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                return -1
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors


        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        sys.stderr = stdErrorLog()
        sys.stdout = stdOutLog()

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
        return 0

    def delpid(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)
    def start(self):
        """
        Start the daemon
        """
        if not os.path.exists(self.conffile): 
            print "Missing "+self.conffile+" - can not start instance"
            #raise Exception("Missing "+self.conffile)
            sys.exit(4)
        # Check for a pidfile to see if the daemon already runs

        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exists. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(3)
        # Start the daemon
        ret = self.daemonize()
        if ret == 0:
           self.run()
           ret = 0
        return ret

    def status(self):
        """
        Get the daemon status from the pid file and ps
        """
        retval = False
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = self.processname + self.instancemsg +" not running, no pidfile %s\n"
        else:
            try:
                os.kill(pid,0)
                message = self.processname + self.instancemsg + " is running with pidfile %s\n"
                retval = True
            except OSError as ex:
                if ex.errno==1:
                    message = self.processname + self.instancemsg + " is running with pidfile %s\n"
                else:
                    message = self.processname + self.instancemsg + " pid exist in %s but process is not running\n"
            except:
                message = self.processname + self.instancemsg + " pid exist in %s but process is not running\n"
                #should return true for puppet to detect service crash (also when stopped)

        sys.stdout.write(message % self.pidfile)
        return retval

    def silentStatus(self):
        """
        Get the daemon status from the pid file and ps
        """
        retval = False
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = self.processname + self.instancemsg +" not running, no pidfile %s\n"
        else:
            try:
                os.kill(pid,0)
                retval = True
            except:
                pass

        return retval

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = " not running, no pidfile %s\n"
            sys.stdout.write(message % self.pidfile)
            sys.stdout.flush()
            return # not an error in a restart

        # Try killing the daemon process
        processPresent=False
        try:
            #check is process is alive
            os.kill(pid,0)
            processPresent=True
            sys.stdout.flush()
            # signal the daemon to stop
            timeout = 5.0 #kill timeout
            os.kill(pid, SIGINT)
            #Q: how is the while loop exited ???
            #A: os.kill throws an exception of type OSError
            #   when pid does not exist
            #C: not very elegant but it works
            while 1:
                if timeout <=0.:
                  sys.stdout.write("\nterminating with -9...")
                  os.kill(pid,9)
                  sys.stdout.write("\nterminated after 5 seconds\n")
                  #let system time to kill the process tree
                  time.sleep(0.5)
                  self.emergencyUmount()
                  time.sleep(0.5)
                os.kill(pid,0)
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(0.5)
                timeout-=0.5
        except OSError, err:
            time.sleep(.1)
            err = str(err)
            if err.find("No such process") > 0:
                #this handles the successful stopping of the daemon...
                if os.path.exists(self.pidfile):
                    if processPresent==False:
                        sys.stdout.write(" process "+str(pid)+" is dead. Removing pidfile" + self.pidfile+ " pid:" + str(pid))
                    try:
                        os.remove(self.pidfile)
                    except Exception as ex:
                        sys.stdout.write(' [  \033[1;31mFAILED\033[0;39m  ]\n')
                        sys.stderr.write(str(ex)+'\n')
                        sys.exit(1)
                elif not os.path.exists(self.pidfile):
                    if processPresent==False:
                        sys.stdout.write(' service is not running')
            else:
                sys.stdout.write(' [  \033[1;31mFAILED\033[0;39m  ]\n')
                sys.stderr.write(str(err)+'\n')
                sys.exit(1)

        if (self.processname!="hltd"):sys.stdout.write("\t\t")
        sys.stdout.write('\t\t\t [  \033[1;32mOK\033[0;39m  ]\n')
        sys.stdout.flush()

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        return self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon2. It will be called after the process has been
        daemonized by start() or restart().
        """

    def emergencyUmount(self):

        cfg = ConfigParser.SafeConfigParser()
        cfg.read(self.conffile)

        bu_base_dir=None#/fff/BU0?
        ramdisk_subdirectory = 'ramdisk'
        output_subdirectory = 'output'
       
        for sec in cfg.sections():
            for item,value in cfg.items(sec):
                if item=='ramdisk_subdiretory':ramdisk_subdirectory=value
                if item=='output_subdirectory':output_subdirectory=value
                if item=='bu_base_dir':bu_base_dir=value



        process = subprocess.Popen(['mount'],stdout=subprocess.PIPE)
        out = process.communicate()[0]
        mounts = re.findall('/'+bu_base_dir+'[0-9]+',out)
        mounts = sorted(list(set(mounts)))
        for point in mounts:
            sys.stdout.write("trying emergency umount of "+point+"\n")
            try:
                subprocess.check_call(['umount','/'+point])
            except subprocess.CalledProcessError, err1:
                pass
            except Exception as ex:
                #ok(legacy mountpoint)
                pass
            try:
                subprocess.check_call(['umount',os.path.join('/'+point,ramdisk_subdirectory)])
            except subprocess.CalledProcessError, err1:
                sys.stdout.write("Error calling umount in cleanup_mountpoints\n")
                sys.stdout.write(str(err1.returncode)+"\n")
            except Exception as ex:
                sys.stdout.write(ex.args[0]+"\n")
            try:
                subprocess.check_call(['umount',os.path.join('/'+point,output_subdirectory)])
            except subprocess.CalledProcessError, err1:
                sys.stdout.write("Error calling umount in cleanup_mountpoints\n")
                sys.stdout.write(str(err1.returncode)+"\n")
            except Exception as ex:
                sys.stdout.write(ex.args[0]+"\n")


    def touchLockFile(self):
        try:
            with open(self.lockfile,"w+") as fi:
                pass
        except:
            pass

    def removeLockFile(self):
        try:
            os.unlink(self.lockfile)
        except:
            pass


 
