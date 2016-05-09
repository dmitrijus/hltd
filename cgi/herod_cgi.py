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
    command=form['command']
except:
    command='herod'

try:
    if os.path.exists(command):
        os.remove(command)
    fp = open(command,'w+')
    fp.close()
except Exception as ex:
    print "exception encountered in operating hltd (herod)\n"
    print '<P>'
    print ex
    raise
