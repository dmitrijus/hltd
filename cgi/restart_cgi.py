#!/usr/bin/env python2.6
import cgi
import time
import os
import subprocess

print "Content-Type: text/html"     # HTML is following
print
try:
    if os.path.exists('restart'):
        os.remove('restart')
    fp = open('restart','w+')
    fp.close()
except Exception as ex:
    print "exception encountered in operating hltd\n"
    print '<P>'
    print ex
    raise

print "Rebirth planning commenced."
