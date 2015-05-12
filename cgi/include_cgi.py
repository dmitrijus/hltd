#!/usr/bin/env python2.6
import cgi
import os
form = cgi.FieldStorage()
print "Content-Type: text/html"     # HTML is following
print            
print "<TITLE>CGI script exclude</TITLE>"

try:
    os.unlink('include')
except:
    pass
fp = open('include','w+')
fp.close()

