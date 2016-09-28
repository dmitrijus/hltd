#!/bin/env python
import os
import sys
import json
import cx_Oracle

#TODO:avoid putting this into DB
#TODO: syslog, conf?
cred = {}
with open('/opt/fff/db.jsn','r') as fi:
  cred = json.load(fi)
con = cx_Oracle.connect(cred['login'],cred['password'],cred['sid'],
                        cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)

cur = con.cursor()
host = os.uname()[1]+".cms"

myDAQ_EQCFG_EQSET = 'DAQ_EQCFG_EQSET'

if host.startswith('dv'):
  tagdaq = 'DAQ2VAL'
  con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_R',cred['password'],'int2r_lb',
                        cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
  qstring = "select d.dnsname from \
	                DAQ_EQCFG_DNSNAME d,               \
	                DAQ_EQCFG_HOST_ATTRIBUTE ha,       \
	                DAQ_EQCFG_HOST_NIC hn              \
	                where                              \
			d.dnsname like 'fu-%'              \
	                AND ha.eqset_id=d.eqset_id         \
	                AND hn.eqset_id=d.eqset_id         \
	                AND hn.nic_id=d.nic_id             \
	                AND ha.host_id=hn.host_id          \
			AND ha.attr_name like 'myBU%' AND ha.attr_value='"+host+"' \
			AND d.eqset_id = (select eqset_id from DAQ_EQCFG_EQSET     \
			where tag='"+tagdaq+"' AND                                 \
			ctime = (SELECT MAX(CTIME) FROM DAQ_EQCFG_EQSET WHERE tag='"+tagdaq+"'))"
else:
  tagdaq = 'DAQ2'
  con = cx_Oracle.connect(cred['login'],cred['password'],cred['sid'],
                        cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
  qstring = "select d.dnsname from \
	                DAQ_EQCFG_DNSNAME d,               \
	                DAQ_EQCFG_HOST_ATTRIBUTE ha,       \
	                DAQ_EQCFG_HOST_NIC hn              \
	                where                              \
			d.dnsname like 'dvrubu-%'          \
	                AND ha.eqset_id=d.eqset_id         \
	                AND hn.eqset_id=d.eqset_id         \
	                AND hn.nic_id=d.nic_id             \
	                AND ha.host_id=hn.host_id          \
			AND ha.attr_name like 'myBU%' AND ha.attr_value like '"+host+".%' \
			AND d.eqset_id = (select eqset_id from DAQ_EQCFG_EQSET            \
			where tag='"+tagdaq+"' AND                                        \
			ctime = (SELECT MAX(CTIME) FROM DAQ_EQCFG_EQSET WHERE tag='"+tagdaq+"'))"

cur.execute(qstring)

fu_control_list = []
fu_data_interfaces = {}

for result in cur:
  fu = result[0].split('.')
  if len(fu)==2:
    #print fu[0]
    fu_control_list.append(fu[0])
    if fu[0]+'.cms' in fu_data_interfaces:
      fu_data_interfaces[fu[0]+'.cms'].append('control')
    else:
      fu_data_interfaces[fu[0]+'.cms']=['control']
  else:
    if fu[0]+'.cms' in fu_data_interfaces:
      fu_data_interfaces[fu[0]+'.cms'].append(fu[1])
    else:
      fu_data_interfaces[fu[0]+'.cms']=[fu[1]]

print ','.join(sorted(list(set(fu_control_list))))

cur.close()
con.close()
sys.exit(0)

#class Notificator(threading.Thread):
#
#    def __init__(self,host):
#        threading.Thread.__init__(self)
#        self.host=host
#
#    def run(self):
#
#            try:
#                #subtract cgi offset when connecting machine
#                connection = httplib.HTTPConnection(self.host,9000 if not len(sys.argv) else sys.argv[1],timeout=20)
#                connection.request("GET",'cgi-bin/rebirth_cgi.py')
#                response = connection.getresponse()
#                machinelist.append(self.host)
#            except:
#                print "Unable to contact machine",self.host
#
#
#
#fu_threads = []
#for fu in fu_control_list:
#  um = Notificator(fu)
#  fu_threads.append(um)
#  um.start()
#for fut in fu_threads:
#  fut.join()

#sys.exit(0)
