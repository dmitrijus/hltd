#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/var/log/hltd/pid/hlt_run$4_pid$$.log"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startDqmRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 $8 >| $logname
export SCRAM_ARCH=$2
cd $1
cd base
source cmsset_default.sh >> $logname
cd $1
cd current
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $3;
pwd >> $logname 2>&1
exec esMonitoring.py cmsRun `readlink $6` runInputDir=$5 runNumber=$4 $7 $8 >> $logname 2>&1
