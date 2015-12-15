#!/usr/bin/env python2.6
import cgi
import os
print "Content-Type: text/html"     # HTML is following
print

retval=0
try:
    listOfRuns=map(lambda x:x,filter(lambda x:True if x.startswith('run') else False,os.listdir(os.getcwd())))
    for run in listOfRuns:
        retval = 1 if 'active' in os.listdir(run) else 0;
except Exception as ex:
    print ex
print retval;
