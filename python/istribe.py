#!/bin/env python
import sys

def esClusterName():
    try:
      with open('/etc/elasticsearch/elasticsearch.yml') as fi:
        lines = fi.readlines()
        for line in lines:
          sline = line.strip()
          if line.startswith("cluster.name"):
            return line.split(':')[1].strip()
    except:
      pass
    return ""

esCluster = esClusterName()
print esCluster
sys.exit(0)
#if esCluster.startswith("es-tribe") or esCluster.startswith("es-vm-tribe"):
#  sys.exit(0)
#else:
#  sys.exit(1)
