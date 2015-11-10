#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/var/log/hltd/pid/hlt_run$4_pid$$.log"
lognamez="/var/log/hltd/pid/hlt_run$4_pid$$_gzip.log.gz"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startDqmRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 $8 >| $logname
export http_proxy="http://cmsproxy.cms:3128"
export https_proxy="https://cmsproxy.cms:3128/"
export NO_PROXY=".cms"
export SCRAM_ARCH=$2
cd $1
cd base
source cmsset_default.sh >> $logname
cd $1
# fully dereference 'current' symbolic link 
cd `readlink -f current`
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $3;
pwd >> $logname 2>&1
exec esMonitoring.py -z $lognamez cmsRun `readlink -f $6` runInputDir=$5 runNumber=$4 $7 $8 >> $logname 2>&1
