#!/usr/bin/env python2.6
import cgi
import os
import time

RUNNUMBER_PADDING=6

form = cgi.FieldStorage()
if "run" not in form:
    print "Status:400"
    print "Content-Type: text/html"     # HTML is following
    print "<H1>Error</H1>"
    print "Please fill in the run number "
else:
    filename='emu'+str(form["run"].value.zfill(RUNNUMBER_PADDING))
    runfilename='run'+str(form["run"].value.zfill(RUNNUMBER_PADDING))
    success = True
    if os.path.exists(runfilename):
        print "Status:409"
        success = False
    try:
        if os.path.exists(filename):
            os.remove(filename)
        fp = open(filename,'w+')
        fp.close()
    except Exception as ex:
        print "Status:410"
        success = False


    attempts=0

    if success:
        while not os.path.exists(runfilename):
            time.sleep(1.)
            attempts+=1
            if attempts > 5:
                print "Status:408"
                success = False
                break
    if success:
        print "Content-Type: text/html"     # HTML is following
        print
        print "<TITLE>CGI script output</TITLE>"
        print "<H1>emu run "+str(form["run"].value)+" created</H1>"
        print "in dir "+os.getcwd()
