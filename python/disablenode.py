#!/bin/env python

import os,sys,socket
import shutil
import json
import subprocess
import shutil

import time

#sys.path.append('/opt/hltd/python')
#from fillresources import *

hltdconf = '/etc/hltd.conf'
busconfig = '/etc/appliance/bus.config'
elasticconf = '/etc/elasticsearch/elasticsearch.yml'

def getTimeString():
    tzones = time.tzname
    if len(tzones)>1:zone=str(tzones[1])
    else:zone=str(tzones[0])
    return str(time.strftime("%H:%M:%S"))+" "+time.strftime("%d-%b-%Y")+" "+zone


class FileManager:
    def __init__(self,file,sep,edited,os1='',os2='',recreate=False):
        self.name = file
        if recreate==False:
            f = open(file,'r')
            self.lines = f.readlines()
            f.close()
        else:
            self.lines=[]
        self.sep = sep
        self.regs = []
        self.remove = []
        self.edited = edited
        #for style
        self.os1=os1
        self.os2=os2

    def reg(self,key,val,section=None):
        self.regs.append([key,val,False,section])

    def removeEntry(self,key):
        self.remove.append(key)

    def commit(self):
        out = []
        #if self.edited  == False:
        out.append('#edited by fff meta rpm at '+getTimeString()+'\n')

        #first removing elements
        for rm in self.remove:
            for i,l in enumerate(self.lines):
                if l.strip().startswith(rm):
                    del self.lines[i]
                    break

        for i,l in enumerate(self.lines):
            lstrip = l.strip()
            if lstrip.startswith('#'):
                continue
                   
            try:
                key = lstrip.split(self.sep)[0].strip()
                for r in self.regs:
                    if r[0] == key:
                        self.lines[i] = r[0].strip()+self.os1+self.sep+self.os2+r[1].strip()+'\n'
                        r[2]= True
                        break
            except:
                continue
        for r in self.regs:
            if r[2] == False:
                toAdd = r[0]+self.os1+self.sep+self.os2+r[1]+'\n'
                insertionDone = False
                if r[3] is not None:
                    for idx,l in enumerate(self.lines):
                        if l.strip().startswith(r[3]):
                            try:
                                self.lines.insert(idx+1,toAdd)
                                insertionDone = True
                            except:
                                pass
                            break
                if insertionDone == False:
                    self.lines.append(toAdd)
        for l in self.lines:
            #already written
            if l.startswith("#edited by fff meta rpm"):continue
            out.append(l)
        #print "file ",self.name,"\n\n"
        #for o in out: print o
        f = open(self.name,'w+')
        f.writelines(out)
        f.close()


#main function
if __name__ == "__main__":
    argvc = 1
    if not sys.argv[1]:
        print "selection of packages to set up (hltd and/or elastic) missing"
        sys.exit(1)
    selection = sys.argv[1]
    #print selection

    clusterName='appliance_disabled'
    if 'elasticsearch' in selection:

        print "disabling elasticsearch"
        escfg = FileManager(elasticconf,':',True,'',' ',recreate=True)
        escfg.reg('cluster.name',clusterName)
        escfg.reg('node.name',os.uname()[1])
        escfg.reg('discovery.zen.ping.multicast.enabled','false')
        #escfg.reg('network.publish_host',es_publish_host)
        escfg.reg('transport.tcp.compress','true')
        #escfg.reg('discovery.zen.ping.unicast.hosts',"[\"" + buName + ".cms" + "\"]")

        if type == 'fu':
            escfg.reg('node.master','false')
            escfg.reg('node.data','true')
        if type == 'bu':
            escfg.reg('node.master','true')
            escfg.reg('node.data','false')
        escfg.commit()
#        subprocess.check_call(['/sbin/chkconfig elasticsearch off'])
#        subprocess.check_call(['/sbin/service elasticsearch stop'])

    if "hltd" in selection:

        print "disabling hltd"

        hltdcfg = FileManager(hltdconf,'=',True,' ',' ')
        hltdcfg.reg('enabled','False','[General]')
        hltdcfg.reg('elastic_cluster',clusterName,'[Monitoring]')
        hltdcfg.reg('elastic_runindex_url','null','[Monitoring]')
        hltdcfg.reg('elastic_runindex_name','null','[Monitoring]')
        hltdcfg.commit()

#        subprocess.check_call(['/sbin/chkconfig hltd off'])
#        subprocess.check_call(['/sbin/service hltd stop'])
        try:os.remove(busconfig)
        except:pass


    print "Disabling fffmeta service (boot check)"
#    subprocess.check_call(['chkconfig fffmeta off'])

