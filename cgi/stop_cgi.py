#!/usr/bin/env python2.6
import cgi
import os
form = cgi.FieldStorage()
print "Content-Type: text/html"     # HTML is following
print
print "<TITLE>CGI script output</TITLE>"
if "run" not in form:
    print "<H1>Error</H1>"
    print "Please fill in the run number "
else:
    fp = open('end'+str(form["run"].value),'w+')
    fp.close()
    print "<H1>run "+str(form["run"].value)+" stopped</H1>"
    print "in dir "+os.getcwd()
