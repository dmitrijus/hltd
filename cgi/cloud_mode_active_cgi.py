#!/usr/bin/env python2.6
import cgi
import os
print "Content-Type: text/html"     # HTML is following                                                  
print 

try:
    cloud = os.listdir('/etc/appliance/resources/cloud')
    print len(cloud)
except Exception as ex:
    print ex
