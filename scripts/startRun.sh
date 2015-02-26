#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/var/log/hltd/pid/hlt_run$6_pid$$.log"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12}>| $logname
dir=$1
export SCRAM_ARCH=$2
source $dir/cmsset_default.sh >> $logname
dir+=/$2/cms/${11}/$3/src
cd $dir;
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $4;
logname="/var/log/hltd/pid/hlt_run$6_pid$$.log"
type -P cmsRun &>/dev/null || (sleep 2;exit 127)
exec cmsRun $5 "transferMode="$6 "runNumber="$7 "buBaseDir="$8 "dataDir"=$9 "numThreads="$10 "numFwkStreams"=${11} >> $logname 2>&1
