#!/bin/env python

import os
import shutil
import hltdconf
import time

def clearDir(dir):
  try:
    files = os.listdir(dir)
    for file in files:
      try:
        os.unlink(os.path.join(dir,file))
      except:
        pass
  except:
    pass

conf=hltdconf.hltdConf('/etc/hltd.conf')

role=None

if conf.role==None:
    if 'bu' in os.uname()[1]: role='bu'
    elif 'fu' in os.uname()[1]: role='fu'
else: role = conf.role

if role=='fu' and not conf.dqm_machine:

    clearDir(conf.resource_base+'/idle')
    clearDir(conf.resource_base+'/online')
    clearDir(conf.resource_base+'/except')
    clearDir(conf.resource_base+'/quarantined')
    clearDir(conf.resource_base+'/cloud')
 
    fp=open('/proc/cpuinfo','r')
    resource_count = 0
    for line in fp:
        if line.startswith('processor'):
            open(conf.resource_base+'/idle/core'+str(resource_count),'a').close()
            resource_count+=1

    try:
        os.umask(0)
        os.makedirs(conf.watch_directory)
    except OSError:
        try: 
            os.chmod(conf.watch_directory,0777)
        except:
            pass

elif role=='bu':

    try:
        os.umask(0)
        os.makedirs(conf.watch_directory+'/appliance')
    except OSError:
        try:
            os.chmod(conf.watch_directory+'/appliance',0777)
        except:
            pass

