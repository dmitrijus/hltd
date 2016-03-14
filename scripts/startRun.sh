#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/var/log/hltd/pid/hlt_run$8_pid$$.log"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12}>| $logname
dir=$1
export HOME=/tmp
export SCRAM_ARCH=$2
source $dir/cmsset_default.sh >> $logname
dir+=/$2/cms/${5}/$3/src
cd $dir;
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $4;
export FRONTIER_LOG_LEVEL="warning"
export FFF_EMPTYLSMODE="true"
export FFF_MICROMERGEDISABLED="true"
type -P cmsRun &>/dev/null || (sleep 2;exit 127)
exec cmsRun $6 "transferMode="$7 "runNumber="$8 "buBaseDir="$9 "dataDir"=${10} "numThreads="${11} "numFwkStreams"=${12} >> $logname 2>&1
