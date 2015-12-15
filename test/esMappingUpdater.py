#!/bin/env python
sys.path.append('.')
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import sys
from hltdconf import initConf
import elasticbu

del sys.modules['mapping']
sys.modules['mapping'] = __import__('mapping2')

import mapping

print elasticbu.mapping

#conf=initConf('main')
#self.es = elasticBandBU(self.conf,0,'',False,None,None)



