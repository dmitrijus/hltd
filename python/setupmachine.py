#!/bin/env python

import os,sys,socket
import shutil
import json
import subprocess
import shutil

import time

sys.path.append('/opt/hltd/python')
#from fillresources import *

#for testing enviroment
try:
    import cx_Oracle
except ImportError:
    pass
try:
    import MySQLdb
except ImportError:
    pass


backup_dir = '/opt/fff/backup'
try:
    os.makedirs(backup_dir)
except:pass

hltdconf = '/etc/hltd.conf'
busconfig = '/etc/appliance/bus.config'
elasticsysconf = '/etc/sysconfig/elasticsearch'
elasticconf = '/etc/elasticsearch/elasticsearch.yml'

dbhost = 'empty'
dbsid = 'empty'
dblogin = 'empty'
dbpwd = 'empty'
equipmentSet = 'latest'
minidaq_list = ["bu-c2f13-21-01","bu-c2f13-23-01","bu-c2f13-25-01","bu-c2f13-27-01",
                "fu-c2f13-17-01","fu-c2f13-17-02","fu-c2f13-17-03","fu-c2f13-17-04"
                "fu-c2f13-19-01","fu-c2f13-19-02","fu-c2f13-19-03","fu-c2f13-19-04"]
dqm_list = ["bu-c2f13-31-01","fu-c2f13-39-01","fu-c2f13-39-02",
            "fu-c2f13-39-03","fu-c2f13-39-04"]
ed_list = ["bu-c2f13-29-01","fu-c2f13-41-01","fu-c2f13-41-02",
           "fu-c2f13-41-03","fu-c2f13-41-04"]

#es_cdaq_list = ["srv-c2a11-07-01","srv-c2a11-08-01","srv-c2a11-09-01","srv-c2a11-10-01",
#                "srv-c2a11-11-01","srv-c2a11-14-01","srv-c2a11-15-01","srv-c2a11-16-01",
#                "srv-c2a11-17-01","srv-c2a11-18-01","srv-c2a11-19-01","srv-c2a11-20-01",
#                "srv-c2a11-21-01","srv-c2a11-22-01","srv-c2a11-23-01","srv-c2a11-26-01",
#                "srv-c2a11-27-01","srv-c2a11-28-01","srv-c2a11-29-01","srv-c2a11-30-01"]
#
#es_tribe_list = ["srv-c2a11-31-01","srv-c2a11-32-01","srv-c2a11-33-01","srv-c2a11-34-01",
#                "srv-c2a11-35-01","srv-c2a11-38-01","srv-c2a11-39-01","srv-c2a11-40-01",
#                "srv-c2a11-41-01","srv-c2a11-42-01"]

tribe_ignore_list = ['bu-c2f13-29-01','bu-c2f13-31-01']

myhost = os.uname()[1]

#testing dual mount point
vm_override_buHNs = {
                     "fu-vm-01-01.cern.ch":["bu-vm-01-01","bu-vm-01-01"],
                     "fu-vm-01-02.cern.ch":["bu-vm-01-01"],
                     "fu-vm-02-01.cern.ch":["bu-vm-01-01","bu-vm-01-01"],
                     "fu-vm-02-02.cern.ch":["bu-vm-01-01"]
                     }

def getmachinetype():

    #print "running on host ",myhost
    if   myhost.startswith('dvrubu-') or myhost.startswith('dvfu-') : return 'daq2val','fu'
    elif myhost.startswith('dvbu-') : return 'daq2val','bu'
    elif myhost.startswith('fu-') : return 'daq2','fu'
    elif myhost.startswith('bu-') : return 'daq2','bu'
    elif myhost.startswith('srv-') :
        try:
            es_cdaq_list = socket.gethostbyname_ex('es-cdaq')[2]
            es_tribe_list = socket.gethostbyname_ex('es-tribe')[2]
            myaddr = socket.gethostbyname(myhost)
            if myaddr in es_cdaq_list:
                return 'es','escdaq'
            elif myaddr in es_tribe_list:
                return 'es','tribe'
            else:
                return 'unknown','unknown'
        except socket.gaierror, ex:
            print 'dns lookup error ',str(ex)
            raise ex  
    else: 
       print "unknown machine type"
       return 'unknown','unknown'
    

def getIPs(hostname):
    try:
        ips = socket.gethostbyname_ex(hostname)
    except socket.gaierror, ex:
        print 'unable to get ',hostname,'IP address:',str(ex)
        raise ex
    return ips

def getTimeString():
    tzones = time.tzname
    if len(tzones)>1:zone=str(tzones[1])
    else:zone=str(tzones[0])
    return str(time.strftime("%H:%M:%S"))+" "+time.strftime("%d-%b-%Y")+" "+zone


def checkModifiedConfigInFile(file):

    f = open(file)
    lines = f.readlines(2)#read first 2
    f.close()
    tzones = time.tzname
    if len(tzones)>1:zone=tzones[1]
    else:zone=tzones[0]

    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False
    


def checkModifiedConfig(lines):
    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False


#alternates between two data inteface indices based on host naming convention
def name_identifier():
  try:
      nameParts = os.uname()[1].split('-')
      return (int(nameParts[-1]) * int(nameParts[-2]/2)) % 2
  except:
      return 0



def getBUAddr(parentTag,hostname):

    global equipmentSet
    #con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_W/'+dbpwd+'@'+dbhost+':10121/int2r_lb.cern.ch',

    if env == "vm":

        try:
            #cluster in openstack that is not (yet) in mysql
            retval = []
            for bu_hn in vm_override_buHNs[hostname]:
              retval.append(["myBU",bu_hn])
            return retval
        except:
            pass
        con = MySQLdb.connect( host= dbhost, user = dblogin, passwd = dbpwd, db = dbsid)
    else:
        if parentTag == 'daq2':
            if dbhost.strip()=='null':
                #con = cx_Oracle.connect('CMS_DAQ2_HW_CONF_W','pwd','cms_rcms',
                con = cx_Oracle.connect(dblogin,dbpwd,dbsid,
                          cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
            else:
                con = cx_Oracle.connect(dblogin+'/'+dbpwd+'@'+dbhost+':10121/'+dbsid,
                          cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
        else:
            con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_R/'+dbpwd+'@int2r2-v.cern.ch:10121/int2r_lb.cern.ch',
                          cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
    
    #print con.version

    cur = con.cursor()

    #IMPORTANT: first query requires uppercase parent eq, while the latter requires lowercase

    qstring=  "select attr_name, attr_value from \
                DAQ_EQCFG_HOST_ATTRIBUTE ha, \
                DAQ_EQCFG_HOST_NIC hn, \
                DAQ_EQCFG_DNSNAME d \
                where \
                ha.eqset_id=hn.eqset_id AND \
                hn.eqset_id=d.eqset_id AND \
                ha.host_id = hn.host_id AND \
                ha.attr_name like 'myBU%' AND \
                hn.nic_id = d.nic_id AND \
                d.dnsname = '" + hostname + "' \
                AND d.eqset_id = (select eqset_id from DAQ_EQCFG_EQSET \
                where tag='"+parentTag.upper()+"' AND \
                ctime = (SELECT MAX(CTIME) FROM DAQ_EQCFG_EQSET WHERE tag='"+parentTag.upper()+"')) order by attr_name"

    qstring2= "select attr_name, attr_value from \
                DAQ_EQCFG_HOST_ATTRIBUTE ha, \
                DAQ_EQCFG_HOST_NIC hn, \
                DAQ_EQCFG_DNSNAME d \
                where \
                ha.eqset_id=hn.eqset_id AND \
                hn.eqset_id=d.eqset_id AND \
                ha.host_id = hn.host_id AND \
                ha.attr_name like 'myBU%' AND \
                hn.nic_id = d.nic_id AND \
                d.dnsname = '" + hostname + "' \
                AND d.eqset_id = (select child.eqset_id from DAQ_EQCFG_EQSET child, DAQ_EQCFG_EQSET \
                parent WHERE child.parent_id = parent.eqset_id AND parent.cfgkey = '"+parentTag+"' and child.cfgkey = '"+ equipmentSet + "')"

    #NOTE: to query squid master for the FU, replace 'myBU%' with 'mySquidMaster%'

    if equipmentSet == 'latest':
      cur.execute(qstring)
    else:
      print "query equipment set",parentTag+'/'+equipmentSet
      cur.execute(qstring2)

    retval = []
    for res in cur:
        retval.append(res)
    cur.close()
    #print retval
    return retval

def getAllBU(requireFU=False):

    #setups = ['daq2','daq2val']
    parentTag = 'daq2'
    if True:
    #if parentTag == 'daq2':
        if dbhost.strip()=='null':
            #con = cx_Oracle.connect('CMS_DAQ2_HW_CONF_W','pwd','cms_rcms',
            con = cx_Oracle.connect(dblogin,dbpwd,dbsid,
                      cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
        else:
            con = cx_Oracle.connect(dblogin+'/'+dbpwd+'@'+dbhost+':10121/'+dbsid,
                      cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
    #else:
    #    con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_W/'+dbpwd+'@int2r2-v.cern.ch:10121/int2r_lb.cern.ch',
    #                  cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
 
    cur = con.cursor()
    retval = []
    if requireFU==False:
        qstring= "select dnsname from DAQ_EQCFG_DNSNAME where (dnsname like 'bu-%' OR dnsname like '__bu-%') \
                  AND eqset_id = (select eqset_id from DAQ_EQCFG_EQSET where tag='"+parentTag.upper()+"' AND \
                                  ctime = (SELECT MAX(CTIME) FROM DAQ_EQCFG_EQSET WHERE tag='"+parentTag.upper()+"'))"

    else:
        qstring = "select attr_value from \
	                DAQ_EQCFG_HOST_ATTRIBUTE ha,       \
	                DAQ_EQCFG_HOST_NIC hn,              \
	                DAQ_EQCFG_DNSNAME d                  \
	                where                                 \
	                ha.eqset_id=hn.eqset_id AND            \
			hn.eqset_id=d.eqset_id AND              \
			ha.host_id = hn.host_id AND              \
			ha.attr_name like 'myBU%' AND             \
			hn.nic_id = d.nic_id AND                   \
			d.dnsname like 'fu-%'                       \
			AND d.eqset_id = (select eqset_id from DAQ_EQCFG_EQSET \
			where tag='"+parentTag.upper()+"' AND                    \
			ctime = (SELECT MAX(CTIME) FROM DAQ_EQCFG_EQSET WHERE tag='"+parentTag.upper()+"'))"




    cur.execute(qstring)

    for res in cur:
        retval.append(res[0])
    cur.close()
    retval = sorted(list(set(map(lambda v: v.split('.')[0], retval))))
    print retval
    return retval


def getSelfDataAddr(parentTag):


    global equipmentSet
    #con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_W/'+dbpwd+'@'+dbhost+':10121/int2r_lb.cern.ch',

    con = cx_Oracle.connect(dblogin+'/'+dbpwd+'@'+dbhost+':10121/'+dbsid,
                        cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
    #print con.version

    cur = con.cursor()

    hostname = os.uname()[1]

    qstring1= "select dnsname from DAQ_EQCFG_DNSNAME where dnsname like '%"+os.uname()[1]+"%' \
                AND d.eqset_id = (select child.eqset_id from DAQ_EQCFG_EQSET child, DAQ_EQCFG_EQSET \
                parent WHERE child.parent_id = parent.eqset_id AND parent.cfgkey = '"+parentTag+"' and child.cfgkey = '"+ equipmentSet + "')"

    qstring2 = "select dnsname from DAQ_EQCFG_DNSNAME where dnsname like '%"+os.uname()[1]+"%' \
                AND eqset_id = (select child.eqset_id from DAQ_EQCFG_EQSET child, DAQ_EQCFG_EQSET parent \
                WHERE child.parent_id = parent.eqset_id AND parent.cfgkey = '"+parentTag+"' and child.cfgkey = '"+ equipmentSet + "')"


    if equipmentSet == 'latest':
        cur.execute(qstring1)
    else:
        print "query equipment set (data network name): ",parentTag+'/'+equipmentSet
        #print '\n',qstring2
        cur.execute(qstring2)

    retval = []
    for res in cur:
        if res[0] != os.uname()[1]+".cms": retval.append(res[0])
    cur.close()

    if len(retval)>1:
        for r in res:
            #prefer .daq2 network if available
            if r.startswith(os.uname()[1]+'.daq2'): return [r]

    return retval

def getInstances(hostname):
    #instance.input example:
    #{"cmsdaq-401b28.cern.ch":{"names":["main","ecal"],"sizes":[40,20]}} #size is in megabytes
    #BU can have multiple instances, FU should have only one specified. If none, any host is assumed to have only main instance
    try:
       with open('/opt/fff/instances.input','r') as fi:
           doc = json.load(fi)
           return doc[hostname]['names'],doc[hostname]['sizes']
    except:
        return ["main"],0


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
        if self.edited  == False:
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


def restoreFileMaybe(file):
    try:
        try:
            f = open(file,'r')
            lines = f.readlines()
            f.close()
            shouldCopy = checkModifiedConfig(lines)
        except:
            #backup also if file got deleted
            shouldCopy = True

        if shouldCopy:
            print "restoring ",file
            backuppath = os.path.join(backup_dir,os.path.basename(file))
            f = open(backuppath)
            blines = f.readlines()
            f.close()
            if  checkModifiedConfig(blines) == False and len(blines)>0:
                shutil.move(backuppath,file)
    except Exception, ex:
        print "restoring problem: " , ex
        pass

#main function
if __name__ == "__main__":
    argvc = 1
    if not sys.argv[argvc]:
        print "selection of packages to set up (hltd and/or elastic) missing"
        sys.exit(1)
    selection = sys.argv[argvc]
    #print selection

    if 'restore' in selection:
        if 'hltd' in selection:
            restoreFileMaybe(hltdconf)
        if 'elasticsearch' in selection:
            restoreFileMaybe(elasticsysconf)
            restoreFileMaybe(elasticconf)

        sys.exit(0)

    argvc += 1
    if not sys.argv[argvc]:
        print "Enviroment parameter missing"
        sys.exit(1)
    env = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "global elasticsearch URL name missing"
        sys.exit(1)
    elastic_host = sys.argv[argvc]
    #http prefix is required here
    if not elastic_host.strip().startswith('http://'):
        elastic_host = 'http://'+ elastic_host.strip()
        #add default port name for elasticsearch
    if len(elastic_host.split(':'))<3:
        elastic_host+=':9200'

    argvc += 1
    if not sys.argv[argvc]:
        print "CMSSW base missing"
        sys.exit(1)
    cmssw_base = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "DB connection hostname missing"
        sys.exit(1)
    dbhost = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "DB connection SID missing"
        sys.exit(1)
    dbsid = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "DB connection login missing"
        sys.exit(1)
    dblogin = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "DB connection password missing"
        sys.exit(1)
    dbpwd = sys.argv[argvc]

    argvc += 1
    if not sys.argv[argvc]:
        print "equipment set name missing"
        sys.exit(1)
    if sys.argv[argvc].strip() != '':
        equipmentSet = sys.argv[argvc].strip()

    argvc += 1
    if not sys.argv[argvc]:
        print "CMSSW job username parameter is missing"
        sys.exit(1)
    username = sys.argv[argvc]

    argvc+=1
    if not sys.argv[argvc]:
        print "CMSSW number of threads/process is missing"
    nthreads = sys.argv[argvc]


    argvc+=1
    if not sys.argv[argvc]:
        print "CMSSW number of framework streams/process is missing"
    nfwkstreams = sys.argv[argvc]



    argvc+=1
    if not sys.argv[argvc]:
        print "CMSSW log collection level is missing"
    cmsswloglevel =  sys.argv[argvc]

    cluster,type = getmachinetype()
    #override for daq2val!
    #if cluster == 'daq2val': cmsswloglevel =  'INFO'
    if env == "vm":
        cnhostname = os.uname()[1]
    else:
        cnhostname = os.uname()[1]+'.cms'
       
    use_elasticsearch = 'True'
    cmssw_version = 'CMSSW_7_1_4_patch1'
    dqmmachine = 'False'
    execdir = '/opt/hltd'
    resourcefract = '0.5'

    if cluster == 'daq2val':
        runindex_name = 'dv'
    elif cluster == 'daq2':
        runindex_name = 'cdaq'
        if myhost in minidaq_list:
            runindex_name = 'minidaq'
        if myhost in dqm_list or myhost in ed_list:

            use_elasticsearch = 'False'
            runindex_name = 'dqm'
            cmsswloglevel = 'DISABLED'
            dqmmachine = 'True'
            username = 'dqmpro'
            resourcefract = '1.0'
            cmssw_version = ''
            if type == 'fu':
                cmsswloglevel = 'ERROR'
                cmssw_base = '/home/dqmprolocal'
                execdir = '/home/dqmprolocal/output' ##not yet
        if myhost in ed_list:
            runindex_name = 'ed'
            username = 'dqmdev'
            if type == 'fu':
                cmsswloglevel = 'ERROR'
                cmssw_base = '/home/dqmdevlocal'
                execdir = '/home/dqmdevlocal/output' ##not yet 

    buName = None
    buDataAddr=[]

    if type == 'fu':
      if cluster == 'daq2val' or cluster == 'daq2': 
        for addr in getBUAddr(cluster,cnhostname):
            if buName==None:
                buName = addr[1].split('.')[0]
            elif buName != addr[1].split('.')[0]:
                print "BU name not same for all interfaces:",buName,buNameCheck
                continue
            buDataAddr.append(addr[1])
            #if none are pingable, first one is picked
            if buName == None or len(buDataAddr)==0:
                print "no BU found for this FU in the dabatase"
                sys.exit(-1)
      else:
          print "FU configuration in cluster",cluster,"not supported yet !!"
          sys.exit(-2)
 
    elif type == 'bu':
        if env == "vm":
            buName = os.uname()[1].split(".")[0]
        else:
            buName = os.uname()[1]
    elif type == 'tribe':
        buDataAddr = getAllBU(requireFU=False)
        buName='es-tribe'

    print "running configuration for machine",cnhostname,"of type",type,"in cluster",cluster,"; appliance bu is:",buName

    clusterName='appliance_'+buName
    if 'elasticsearch' in selection:

        if env=="vm":
            es_publish_host=os.uname()[1]
        else:
            es_publish_host=os.uname()[1]+'.cms'

        #print "will modify sysconfig elasticsearch configuration"
        #maybe backup vanilla versions
        essysEdited =  checkModifiedConfigInFile(elasticsysconf)
        if essysEdited == False:
          #print "elasticsearch sysconfig configuration was not yet modified"
          shutil.copy(elasticsysconf,os.path.join(backup_dir,os.path.basename(elasticsysconf)))

        esEdited =  checkModifiedConfigInFile(elasticconf)
        if esEdited == False:
          shutil.copy(elasticconf,os.path.join(backup_dir,os.path.basename(elasticconf)))

        if type == 'fu' or type == 'bu':

            essyscfg = FileManager(elasticsysconf,'=',essysEdited)
            essyscfg.reg('ES_HEAP_SIZE','1G')
            essyscfg.commit()

            escfg = FileManager(elasticconf,':',esEdited,'',' ')
            escfg.reg('cluster.name',clusterName)
            escfg.reg('node.name',cnhostname)
            escfg.reg('discovery.zen.ping.multicast.enabled','false')
            escfg.reg('network.publish_host',es_publish_host)
            escfg.reg('transport.tcp.compress','true')

            if type == 'fu':
                if env=="vm":
                    escfg.reg('discovery.zen.ping.unicast.hosts',"[\"" + buName + "\"]")
                else:
                    escfg.reg('discovery.zen.ping.unicast.hosts',"[\"" + buName + ".cms" + "\"]")
                escfg.reg('indices.fielddata.cache.size', '50%')
                escfg.reg('node.master','false')
                escfg.reg('node.data','true')
            if type == 'bu':
                #escfg.reg('discovery.zen.ping.unicast.hosts','[ \"'+elastic_host2+'\" ]')
                escfg.reg('node.master','true')
                escfg.reg('node.data','false')
            escfg.commit()

        if type == 'tribe':
            essyscfg = FileManager(elasticsysconf,'=',essysEdited)
            essyscfg.reg('ES_HEAP_SIZE','12G')
            essyscfg.commit()

            escfg = FileManager(elasticconf,':',esEdited,'',' ',recreate=True)
            escfg.reg('cluster.name','es-tribe')
            escfg.reg('discovery.zen.ping.multicast.enabled','false')
            #escfg.reg('discovery.zen.ping.unicast.hosts','['+','.join(buDataAddr)+']')
            escfg.reg('transport.tcp.compress','true')
            bustring = "["
            for bu in buDataAddr:
                if bu in tribe_ignore_list:continue

                try:
                    socket.gethostbyname_ex(bu+'.cms')
                except:
                    print "skipping",bu," - unable to lookup IP address"
                    continue
                if bustring!="[":bustring+=','
                bustring+='"'+bu+'.cms'+'"'
            bustring += "]"
            escfg.reg('discovery.zen.ping.unicast.hosts',bustring)

            escfg.reg('tribe','')
            i=1;
            for bu in buDataAddr:
                if bu in tribe_ignore_list:continue

                try:
                    socket.gethostbyname_ex(bu+'.cms')
                except:
                #    print "skipping",bu," - unable to lookup IP address"
                    continue

                escfg.reg('    t'+str(i),'')
                #escfg.reg('         discovery.zen.ping.unicast.hosts', '["'+bu+'.cms"]')
                escfg.reg('         cluster.name', 'appliance_'+bu)
                i=i+1
            escfg.commit()

        if type == 'escdaq':
            essyscfg = FileManager(elasticsysconf,'=',essysEdited)
            essyscfg.reg('ES_HEAP_SIZE','10G')
            essyscfg.commit()

            escfg = FileManager(elasticconf,':',esEdited,'',' ',recreate=True)
            escfg.reg('cluster.name','es-cdaq')
            escfg.reg('discovery.zen.minimum_master_nodes','11')
            escfg.reg('index.mapper.dynamic','false')
            escfg.reg('action.auto_create_index','false')
            escfg.reg('transport.tcp.compress','true')
            escfg.reg('node.master','true')
            escfg.reg('node.data','true')
            escfg.commit()


    if "hltd" in selection:

      #first prepare bus.config file
      if type == 'fu':

        #permissive:try to remove old bus.config
        try:os.remove(os.path.join(backup_dir,os.path.basename(busconfig)))
        except:pass
        try:os.remove(busconfig)
        except:pass

      #write bu ip address
        f = open(busconfig,'w+')

        #swap entries based on name (only C6100 hosts with two data interfaces):
        if len(buDataAddr)>1 and name_identifier()==1:
            temp = buDataAddr[0]
            buDataAddr[0]=buDataAddr[1]
            buDataAddr[1]=temp

        newline=False
        for addr in buDataAddr:
            if newline:f.writelines('\n')
            newline=True
            f.writelines(getIPs(addr)[0])
            #break after writing first entry. it is not yet safe to use secondary interface
            break
        f.close()

      #FU should have one instance assigned, BUs can have multiple
      watch_dir_bu = '/fff/ramdisk'
      out_dir_bu = '/fff/output'
      log_dir_bu = '/var/log/hltd'

      instances,sizes=getInstances(os.uname()[1])
      if len(instances)==0: instances=['main']

      hltdEdited = checkModifiedConfigInFile(hltdconf)

      if hltdEdited == False:
        shutil.copy(hltdconf,os.path.join(backup_dir,os.path.basename(hltdconf)))

      if type=='bu':
        try:os.remove('/etc/hltd.instances')
        except:pass

        #do major ramdisk cleanup (unmount existing loop mount points, run directories and img files)
        try:
            subprocess.check_call(['/opt/hltd/scripts/unmountloopfs.sh','/fff/ramdisk'])
            #delete existing run directories to ensure there is space (if this machine has a non-main instance)
            if instances!=["main"]:
              os.popen('rm -rf /fff/ramdisk/run*')
        except subprocess.CalledProcessError, err1:
            print 'failed to cleanup ramdisk',err1
        except Exception as ex:
            print 'failed to cleanup ramdisk',ex
 
        cgibase=9000

        for idx,val in enumerate(instances):
          if idx!=0 and val=='main':
            instances[idx]=instances[0]
            instances[0]=val
            break
        for idx, instance in enumerate(instances):

          watch_dir_bu = '/fff/ramdisk'
          out_dir_bu = '/fff/output'
          log_dir_bu = '/var/log/hltd'

          cfile = hltdconf
          if instance != 'main':
            cfile = '/etc/hltd-'+instance+'.conf'
            shutil.copy(hltdconf,cfile)
            watch_dir_bu = os.path.join(watch_dir_bu,instance)
            out_dir_bu = os.path.join(out_dir_bu,instance)
            log_dir_bu = os.path.join(log_dir_bu,instance)

            #run loopback setup for non-main instances (is done on every boot since ramdisk is volatile)
            try:
                subprocess.check_call(['/opt/hltd/scripts/makeloopfs.sh','/fff/ramdisk',instance, str(sizes[idx])])
            except subprocess.CalledProcessError, err1:
                print 'failed to configure loopback device mount in ramdisk'

          soap2file_port='0'
 
          if myhost in dqm_list or myhost in ed_list or cluster == 'daq2val' or env=='vm':
              soap2file_port='8010'

          hltdcfg = FileManager(cfile,'=',hltdEdited,' ',' ')

          hltdcfg.reg('enabled','True','[General]')
          hltdcfg.reg('role','bu','[General]')
      
          hltdcfg.reg('user',username,'[General]')
          hltdcfg.reg('instance',instance,'[General]')

          #port for multiple instances
          hltdcfg.reg('cgi_port',str(cgibase+idx),'[Web]')
          hltdcfg.reg('cgi_instance_port_offset',str(idx),'[Web]')
          hltdcfg.reg('soap2file_port',soap2file_port,'[Web]')

          hltdcfg.reg('elastic_cluster',clusterName,'[Monitoring]')
          hltdcfg.reg('watch_directory',watch_dir_bu,'[General]')
          #hltdcfg.reg('micromerge_output',out_dir_bu,'[General]')
          hltdcfg.reg('elastic_runindex_url',elastic_host,'[Monitoring]')
          hltdcfg.reg('elastic_runindex_name',runindex_name,'[Monitoring]')
          hltdcfg.reg('use_elasticsearch',use_elasticsearch,'[Monitoring]')
          hltdcfg.reg('es_cmssw_log_level',cmsswloglevel,'[Monitoring]')
          hltdcfg.reg('dqm_machine',dqmmachine,'[DQM]')
          hltdcfg.reg('log_dir',log_dir_bu,'[Logs]')
          hltdcfg.commit()

        #write all instances in a file
        if 'main' not in instances or len(instances)>1:
          with open('/etc/hltd.instances',"w") as fi:
            for instance in instances: fi.write(instance+"\n")


      if type=='fu':
          hltdcfg = FileManager(hltdconf,'=',hltdEdited,' ',' ')

          hltdcfg.reg('enabled','True','[General]')
          hltdcfg.reg('role','fu','[General]')

          hltdcfg.reg('user',username,'[General]')
          #FU can only have one instance (so we take instance[0] and ignore others)
          hltdcfg.reg('instance',instances[0],'[General]')

          hltdcfg.reg('exec_directory',execdir,'[General]') 
          hltdcfg.reg('watch_directory','/fff/data','[General]')
          hltdcfg.reg('cgi_port','9000','[Web]')
          hltdcfg.reg('cgi_instance_port_offset',"0",'[Web]')
          hltdcfg.reg('soap2file_port','0','[Web]')
          hltdcfg.reg('elastic_cluster',clusterName,'[Monitoring]')
          hltdcfg.reg('es_cmssw_log_level',cmsswloglevel,'[Monitoring]')
          hltdcfg.reg('elastic_runindex_url',elastic_host,'[Monitoring]')
          hltdcfg.reg('elastic_runindex_name',runindex_name,'[Monitoring]')
          hltdcfg.reg('use_elasticsearch',use_elasticsearch,'[Monitoring]')
          hltdcfg.reg('dqm_machine',dqmmachine,'[DQM]')
          hltdcfg.reg('cmssw_base',cmssw_base,'[CMSSW]')
          hltdcfg.reg('cmssw_default_version',cmssw_version,'[CMSSW]')
          hltdcfg.reg('cmssw_threads',nthreads,'[CMSSW]')
          hltdcfg.reg('cmssw_streams',nfwkstreams,'[CMSSW]')
          hltdcfg.reg('resource_use_fraction',resourcefract,'[Resources]')
          hltdcfg.commit()
    if "web" in selection:
          try:os.rmdir('/var/www/html')
          except:
              try:os.unlink('/var/www/html')
              except:pass
          os.symlink('/es-web','/var/www/html')

