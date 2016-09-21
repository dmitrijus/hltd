import os, sys
import subprocess
import logging
import re
import time
import demote
import prctl
from signal import SIGKILL
import datetime

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

class MountManager:

    def __init__(self,confClass):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conf = confClass
        #for preexec_func
        global conf
        conf = confClass
        self.reset()

    def reset(self):
        self.bu_disk_list_ramdisk = []
        self.bu_disk_list_output = []
        self.bu_disk_list_ramdisk_instance = []
        self.bu_disk_list_output_instance = []
        self.bu_disk_ramdisk_CI=None
        self.bu_disk_ramdisk_CI_instance=None
        self.buBootId=None
        self.stale_handle_remount_required=False

    def umount_helper(self,point,nsslock,attemptsLeft=3,initial=True):

      if initial:
        try:
            self.logger.info('calling umount of '+point)
            p = subprocess.Popen(["umount",point], shell=False, stdout=subprocess.PIPE)
	    p.wait()
            #subprocess.check_call(['umount',point])
	    code = p.returncode
        except subprocess.CalledProcessError, err:
	    code = err.returncode
	except Exception as ex:
            code = -1
	    raise ex
	finally:
            if code<2:return True
            if attemptsLeft<=0:
                self.logger.error('Failed to perform umount of '+point+'. returncode:'+str(code))
                return False
            self.logger.warning("umount failed, trying to kill users of mountpoint "+point)
            try:
                if nsslock:nsslock.acquire()
                #try to kill all unpriviledged child processes using the mount point
                f_user = subprocess.Popen(['fuser','-km',os.path.join('/'+point,self.conf.ramdisk_subdirectory)],shell=False,preexec_fn=preexec_function,close_fds=True)
                if nsslock:nsslock.release()
                f_user.wait()
            except:
                if nsslock:
                    try:nsslock.release()
                    except:pass
            return self.umount_helper(point,nsslock,attemptsLeft-1,initial=False)
      else:
        attemptsLeft-=1
        time.sleep(.5)
        try:
            self.logger.info("trying umount -f of "+point)
            p = subprocess.Popen(["umount","-f",point], shell=False, stdout=subprocess.PIPE)
	    p.wait()
            #subprocess.check_call(['umount','-f',point])
	    code = p.returncode
        except subprocess.CalledProcessError, err:
	    code = err.returncode
	except Exception as ex:
            code = -1
	    raise ex
	finally:
            if code<2:return True
            if attemptsLeft<=0:
                self.logger.error('Failed to perform umount -f of '+point+'. returncode:'+str(code))
                return False
            return self.umount_helper(point,nsslock,attemptsLeft,initial=False)
      return True

    def cleanup_mountpoints(self,nsslock,remount=True):

      #reset vars
      self.reset()
      if self.conf.bu_base_dir[0] == '/':
        self.bu_disk_list_ramdisk = [os.path.join(self.conf.bu_base_dir,self.conf.ramdisk_subdirectory)]
        self.bu_disk_list_output = [os.path.join(self.conf.bu_base_dir,self.conf.output_subdirectory)]
        if self.conf.instance=="main":
            self.bu_disk_list_ramdisk_instance = self.bu_disk_list_ramdisk
            self.bu_disk_list_output_instance = self.bu_disk_list_output
        else:
            self.bu_disk_list_ramdisk_instance = [os.path.join(self.bu_disk_list_ramdisk[0],self.conf.instance)]
            self.bu_disk_list_output_instance = [os.path.join(self.bu_disk_list_output[0],self.conf.instance)]

        #make subdirectories if necessary and return
        if remount==True:
            try:
                #there is no BU mount, so create subdirectory structure on FU
                os.makedirs(os.path.join(self.conf.bu_base_dir,self.conf.ramdisk_subdirectory,'appliance','boxes'))
            except OSError:
                pass
            try:
                os.makedirs(os.path.join(self.conf.bu_base_dir,self.conf.output_subdirectory))
            except OSError:
                pass
        return True
      try:
        process = subprocess.Popen(['mount'],stdout=subprocess.PIPE)
        out = process.communicate()[0]
        mounts = re.findall('/'+self.conf.bu_base_dir+'[0-9]+',out) + re.findall('/'+self.conf.bu_base_dir+'-CI/',out)

        mounts = sorted(list(set(mounts)))
        self.logger.info("cleanup_mountpoints: found following mount points: ")
        self.logger.info(mounts)
        umount_failure=False
        for mpoint in mounts:
            self.logger.info('running umount loop for '+str(mpoint))
            point = mpoint.rstrip('/')
            umount_failure = self.umount_helper(os.path.join('/'+point,self.conf.ramdisk_subdirectory),nsslock)==False

            #only attempt this if first umount was successful
            if umount_failure==False and not point.rstrip('/').endswith("-CI"):
                umount_failure = self.umount_helper(os.path.join('/'+point,self.conf.output_subdirectory),nsslock)==False

            #this will remove directories only if they are empty (as unmounted mount point should be)
            try:
                if os.path.join('/'+point,self.conf.ramdisk_subdirectory)!='/':
                    os.rmdir(os.path.join('/'+point,self.conf.ramdisk_subdirectory))
            except Exception as ex:
                self.logger.exception(ex)
            try:
                if os.path.join('/'+point,self.conf.output_subdirectory)!='/':
                    if not point.rstrip('/').endswith("-CI"):
                        os.rmdir(os.path.join('/'+point,self.conf.output_subdirectory))
            except Exception as ex:
                self.logger.exception(ex)
        if remount==False:
            if umount_failure:
              self.logger.info('finishing mount cleanup with mount failure')
            else
              self.logger.info('finishing mount cleanup with mount success')
            if umount_failure:return False
            return True

        i = 0
        bus_config = os.path.join(os.path.dirname(self.conf.resource_base.rstrip(os.path.sep)),'bus.config')
        busconfig_age = os.path.getmtime(bus_config)
        if os.path.exists(bus_config):
            lines = []
            with open(bus_config) as fp:
                lines = fp.readlines()

            if len(lines)==0:
                #exception if invalid bus.config file
                raise Exception("Missing BU address in bus.config file")

            if self.conf.mount_control_path and len(lines):

                try:
                    os.makedirs(os.path.join('/'+self.conf.bu_base_dir+'-CI',self.conf.ramdisk_subdirectory))
                except OSError:
                    pass
                try:
                    mountaddr = lines[0].split('.')[0]+'.cms'
                    #VM fallback
                    if lines[0].endswith('.cern.ch'): mountaddr = lines[0]
                    self.logger.info("found BU to mount (CI) at " + mountaddr)
                except Exception as ex:
                    self.logger.fatal('Unable to parse bus.config file')
                    self.logger.exception(ex)
                    sys.exit(1)
                attemptsLeft = 8
                while attemptsLeft>0:
                    #by default ping waits 10 seconds
                    p_begin = datetime.datetime.now()
                    if os.system("ping -c 1 "+mountaddr)==0:
                        break
                    else:
                        p_end = datetime.datetime.now()
                        self.logger.warning('unable to ping '+mountaddr)
                        dt = p_end - p_begin
                        if dt.seconds < 10:
                            time.sleep(10-dt.seconds)
                    attemptsLeft-=1
                    if attemptsLeft==0:
                        self.logger.fatal('hltd was unable to ping BU '+mountaddr)
                        #check if bus.config has been updated
                        if (os.path.getmtime(bus_config) - busconfig_age)>1:
                            return self.cleanup_mountpoints(nsslock,remount)
                        attemptsLeft=8
                        #sys.exit(1)
                if True:
                    self.logger.info("trying to mount (CI) "+mountaddr+':/fff/'+self.conf.ramdisk_subdirectory+' '+os.path.join('/'+self.conf.bu_base_dir+'-CI',self.conf.ramdisk_subdirectory))
                    try:
                        subprocess.check_call(
                            [self.conf.mount_command,
                             '-t',
                             self.conf.mount_type,
                             '-o',
                             self.conf.mount_options_ramdisk,
                             mountaddr+':/fff/'+self.conf.ramdisk_subdirectory,
                             os.path.join('/'+self.conf.bu_base_dir+'-CI',self.conf.ramdisk_subdirectory)]
                            )
                        toappend = os.path.join('/'+self.conf.bu_base_dir+'-CI',self.conf.ramdisk_subdirectory)
                        self.bu_disk_ramdisk_CI=toappend
                        if self.conf.instance=="main":
                            self.bu_disk_ramdisk_CI_instance = toappend
                        else:
                            self.bu_disk_ramdisk_CI_instance = os.path.join(toappend,self.conf.instance)
                    except subprocess.CalledProcessError, err2:
                        self.logger.exception(err2)
                        self.logger.fatal("Unable to mount ramdisk - exiting.")
                        sys.exit(1)




            busconfig_age = os.path.getmtime(bus_config)
            for line in lines:
                self.logger.info("found BU to mount at "+line.strip())
                try:
                    os.makedirs(os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.ramdisk_subdirectory))
                except OSError:
                    pass
                try:
                    os.makedirs(os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.output_subdirectory))
                except OSError:
                    pass

                attemptsLeft = 8
                while attemptsLeft>0:
                    #by default ping waits 10 seconds
                    p_begin = datetime.datetime.now()
                    if os.system("ping -c 1 "+line.strip())==0:
                        break
                    else:
                        p_end = datetime.datetime.now()
                        self.logger.warning('unable to ping '+line.strip())
                        dt = p_end - p_begin
                        if dt.seconds < 10:
                            time.sleep(10-dt.seconds)
                    attemptsLeft-=1
                    if attemptsLeft==0:
                        self.logger.fatal('hltd was unable to ping BU '+line.strip())
                        #check if bus.config has been updated
                        if (os.path.getmtime(bus_config) - busconfig_age)>1:
                            return self.cleanup_mountpoints(nsslock,remount)
                        attemptsLeft=8
                        #sys.exit(1)
                if True:
                    self.logger.info("trying to mount "+line.strip()+':/fff/'+self.conf.ramdisk_subdirectory+' '+os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.ramdisk_subdirectory))
                    try:
                        subprocess.check_call(
                            [self.conf.mount_command,
                             '-t',
                             self.conf.mount_type,
                             '-o',
                             self.conf.mount_options_ramdisk,
                             line.strip()+':/fff/'+self.conf.ramdisk_subdirectory,
                             os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.ramdisk_subdirectory)]
                            )
                        toappend = os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.ramdisk_subdirectory)
                        self.bu_disk_list_ramdisk.append(toappend)
                        if self.conf.instance=="main":
                            self.bu_disk_list_ramdisk_instance.append(toappend)
                        else:
                            self.bu_disk_list_ramdisk_instance.append(os.path.join(toappend,self.conf.instance))
                    except subprocess.CalledProcessError, err2:
                        self.logger.exception(err2)
                        self.logger.fatal("Unable to mount ramdisk - exiting.")
                        sys.exit(1)

                    self.logger.info("trying to mount "+line.strip()+':/fff/'+self.conf.output_subdirectory+' '+os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.output_subdirectory))
                    try:
                        subprocess.check_call(
                            [self.conf.mount_command,
                             '-t',
                             self.conf.mount_type,
                             '-o',
                             self.conf.mount_options_output,
                             line.strip()+':/fff/'+self.conf.output_subdirectory,
                             os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.output_subdirectory)]
                            )
                        toappend = os.path.join('/'+self.conf.bu_base_dir+str(i),self.conf.output_subdirectory)
                        self.bu_disk_list_output.append(toappend)
                        #if self.conf.instance=="main" or self.conf.instance_same_destination:
                        #    self.bu_disk_list_output_instance.append(toappend)
                        #else:
                        #    self.bu_disk_list_output_instance.append(os.path.join(toappend,self.conf.instance))
                        self.bu_disk_list_output_instance.append(toappend)

                    except subprocess.CalledProcessError, err2:
                        self.logger.exception(err2)
                        self.logger.fatal("Unable to mount output - exiting.")
                        sys.exit(1)

                i+=1
        else:
            self.logger.warning('starting hltd without bus.config file!')
            return False
        #clean up suspended state
        try:
            if remount==True:os.popen('rm -rf '+self.conf.watch_directory+'/suspend*')
        except:pass
        return True
      except Exception as ex:
        self.logger.error("Exception in cleanup_mountpoints")
        self.logger.exception(ex)
        if remount==True:
            self.logger.fatal("Unable to handle (un)mounting")
            return False
        else:return False

    def submount_size(self,basedir):
      loop_size=0
      try:
        p = subprocess.Popen("mount", shell=False, stdout=subprocess.PIPE)
        p.wait()
        std_out=p.stdout.read().split("\n")
        for l in std_out:
            try:
                ls = l.strip()
                toks = l.split()
                if toks[0].startswith(basedir) and toks[2].startswith(basedir) and 'loop' in toks[5]:
                    imgstat = os.stat(toks[0])
                    imgsize = imgstat.st_size
                    loop_size+=imgsize
            except:pass
      except:pass
      self.ramdisk_submount_size = loop_size
      return loop_size

    def cleanup_bu_disks(self,run=None,cleanRamdisk=True,cleanOutput=True):
      if cleanRamdisk:
        if self.conf.watch_directory.startswith('/fff') and self.conf.ramdisk_subdirectory in self.conf.watch_directory:
            self.logger.info('cleanup BU disks: deleting runs in ramdisk ...')
            tries = 10
            while tries > 0:
                tries-=1
                if run==None:
                    p = subprocess.Popen("rm -rf " + self.conf.watch_directory+'/run*',shell=True)
                else:
                    p = subprocess.Popen("rm -rf " + self.conf.watch_directory+'/run'+str(run),shell=True)
                p.wait()
                if p.returncode==0:
                    self.logger.info('Ramdisk cleanup performed')
                    break
                else:
                    self.logger.info('Failed ramdisk cleanup (return code:'+str(p.returncode)+') in attempt'+str(10-tries))

      if cleanOutput:
        outdirPath = self.conf.watch_directory[:self.conf.watch_directory.find(self.conf.ramdisk_subdirectory)]+self.conf.output_subdirectory
        self.logger.info('outdirPath:'+ outdirPath + ' '+self.conf.output_subdirectory)

        if outdirPath.startswith('/fff') and self.conf.output_subdirectory in outdirPath:
            self.logger.info('cleanup BU disks: deleting runs in output disk ...')
            tries = 10
            while tries > 0:
                tries-=1
                if run==None:
                    p = subprocess.Popen("rm -rf " + outdirPath+'/run*',shell=True)
                else:
                    p = subprocess.Popen("rm -rf " + outdirPath+'/run'+str(run),shell=True)
                p.wait()
                if p.returncode==0:
                    self.logger.info('Output cleanup performed')
                    break
                else:
                    self.logger.info('Failed output disk cleanup (return code:'+str(p.returncode)+') in attempt '+str(10-tries))


