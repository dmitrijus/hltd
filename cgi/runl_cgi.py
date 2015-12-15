#!/usr/bin/env python2.6
import cgi
import os
print "Content-Type: text/html"     # HTML is following
print                               # blank line, end of headers
print map(lambda x:x[3:],filter(lambda x:True if x.startswith('run') else False,os.listdir(os.getcwd())))
