#!/bin/env python
fp=open('/proc/cpuinfo','r')
resource_count = 0
for line in fp:
    if line.startswith('processor'):
        open(conf.resource_base+'/idle/core'+str(resource_count),'a').close()
        resource_count+=1
        
if not os.path.exists(conf.watch_directory): os.makedirs(conf.watch_directory)