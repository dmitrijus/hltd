#!/bin/env python

"""
Set the database entries for the hltd bu-fu appliance.
Tables must be empty, to avoid this in future is possibile to read the last eqset_id from DAQ_EQCFG_EQSET
for start the eqset_id count from.

"""
#SETUP
EQUIPMENT = "test"
NETWORK = "test"
TAG = "daq2"
SETUP =   {     "bu-01.cern.ch" :["fu-01.cern.ch","fu-02.cern.ch"],
                "bu-02.cern.ch" :["fu-03.cern.ch","fu-04.cern.ch"]
        }

#MYSQL PARAMS
db_host = "localhost"
db_user = "rcms"
db_pass = "ominozzo2"
db_name = "fffsetup"

#SCRIPT BEGINS
import sys
import MySQLdb
import itertools as i
import time
from datetime import datetime


eqset_id = i.count(1)
host_id = i.count(1)
nic_id = i.count(1)
tag = "'{0}'".format(TAG)
ctime = "'{0}'".format(datetime.utcnow())
attr = "'{0}'".format("myBu%")
null = "NULL"
net = "'{0}'".format(NETWORK)

#DATA TABLES
#WARNING, TABLES MUST BE EMPTY!!!
ha = "DAQ_EQCFG_HOST_ATTRIBUTE"
hn = "DAQ_EQCFG_HOST_NIC"
d = "DAQ_EQCFG_DNSNAME"
e = "DAQ_EQCFG_EQSET"


#QUERY CREATIONS
sqlCmd = []
eqp = str(eqset_id.next())
eqc = str(eqset_id.next())
eqstr = "'{0}'".format(EQUIPMENT)
#parent
sqlCmd.append("INSERT INTO "+e+" (`eqset_id`, `cfgkey`, `description`, `parent_id`, `ctime`, `tag`, `is_directory`) VALUES ("+",".join([eqp,tag,null,null,ctime,null,null])+")")
#child
sqlCmd.append("INSERT INTO "+e+" (`eqset_id`, `cfgkey`, `description`, `parent_id`, `ctime`, `tag`, `is_directory`) VALUES ("+",".join([eqc,eqstr,null,eqp,ctime,null,null])+")")

for bu in SETUP.keys():
    bstr = "'{0}'".format(bu)
    for fu in SETUP[bu]:
        fstr = "'{0}'".format(fu)

        nic = str(nic_id.next())
        host = str(host_id.next())
        sqlCmd.append("INSERT INTO "+d+" (`eqset_id`, `dnsname`, `nic_id`, `network_name`)  VALUES (" +",".join([eqc,fstr,nic,net])+ ")")
        sqlCmd.append("INSERT INTO "+hn+" (`eqset_id`, `host_id`, `nic_id`) VALUES ("+",".join([eqc,host,nic])+")")
        sqlCmd.append("INSERT INTO "+ha+" (`eqset_id`, `host_id`, `attr_name`, `attr_value`) VALUES ("+",".join([eqc,host,attr,bstr])+")")


#for query in sqlCmd:
#    print query


#MYSQL
conn=MySQLdb.connect( host= db_host, user = db_user, passwd = db_pass, db = db_name)

cursor=conn.cursor()

for cmd in sqlCmd:
    print cmd
    #cursor.execute(cmd)
