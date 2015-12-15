#!/usr/bin/env python2.6
import cgi
import time
import os
import subprocess

"""
problem: cgi scripts run as user 'nobody'
how can we handle signaling the daemon ?
"""

form = cgi.FieldStorage()
print "Content-Type: text/html"     # HTML is following
print
print "<TITLE>CGI script output</TITLE>"
print "Hey I'm still here !"

try:
    if os.path.exists('populationcontrol'):
        os.remove('populationcontrol')
    fp = open('populationcontrol','w+')
    fp.close()
except Exception as ex:
    print "exception encountered in operating hltd\n"
    print '<P>'
    print ex
    raise
