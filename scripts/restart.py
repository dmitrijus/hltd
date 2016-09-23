#!/bin/env python
import subprocess
import os
import time

#stop the process
a = subprocess.Popen(["/etc/init.d/hltd","stop"],close_fds=True)
a.wait()
#kill any remnants (possibly not needed)
b = subprocess.Popen(["killall -9 hltd"],shell=True)
b.wait()
time.sleep(0.1)
#(re)start
c = subprocess.Popen(["/etc/init.d/hltd","restart"],close_fds=True)
c.wait()
os._exit(0)
