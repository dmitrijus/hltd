#!/usr/bin/env python2.6
import cgi
import os
RUNNUMBER_PADDING=6
form = cgi.FieldStorage()
print "Content-Type: text/html"     # HTML is following
print
print "<TITLE>CGI script output</TITLE>"
if "run" not in form:
    print "<H1>Error</H1>"
    print "Please fill in the run number "
else:
    os.mkdir('run'+str(form["run"].value).zfill(RUNNUMBER_PADDING))
    print "<H1>run "+str(form["run"].value)+" created</H1>"
    print "in dir "+os.getcwd()
