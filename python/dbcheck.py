import os,sys
from setupmachine import getBUAddr
dbhost__='null'

#dblogin__='CMS_DAQ2_HW_CONF_R'
#dbsid__='cms_rcms'
#dbpwd__='password'
#tag__='daq2'

dblogin__ = sys.argv[1]
dbpwd__ = sys.argv[2]
dbsid__ = sys.argv[3]

host__=os.uname()[1]+'.cms'
if host__.startswith('dv'):
  tag__='daq2val'
else:
  tag__='daq2'

env__='prod'
eqset__='latest'
print getBUAddr(tag__,host__,env__,eqset__,dbhost__,dblogin__,dbpwd__,dbsid__)
