import os
import json
import time

count_used=0
count_idle=0
count_cloud=0
count_total=0
count_bl=0

try:
  with open('/fff/ramdisk/appliance/blacklist','r') as fb:
    blist = json.load(fb)
except:
    blist=[]

files = os.listdir('/fff/ramdisk/appliance/boxes')
for f in files:
	if not f.startswith('fu-'):continue
	fpath = os.path.join('/fff/ramdisk/appliance/boxes',f)
        f_age = os.path.getmtime(fpath)
        age = time.time() - f_age
	if age<10:
          count_total+=1
	  with open(fpath,'r') as fp:
	    js = json.load(fp)
	    if f in blist:
              count_bl+=1
	      if js['used']>0 or js['cloud']>0:print "BL warning U:",js['used'],"C:",js['cloud']
	    else:
	      if js['used']>0:
                count_used+=1
	      if js['idles']>0:
	        count_idle+=1
	      if js['cloud']>0:
	        count_cloud+=1

print "stats T:",count_total,"U:",count_used,"I:",count_idle,"C:",count_cloud,"B:",count_bl
		    
	  
