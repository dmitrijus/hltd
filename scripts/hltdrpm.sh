#!/bin/bash -e
alias python=python2.6
# set the RPM build architecture
#BUILD_ARCH=$(uname -i)      # "i386" for SLC4, "x86_64" for SLC5
BUILD_ARCH=x86_64
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..

#run other script...
#$SCRIPTDIR/libshltdrpm.sh

BASEDIR=$PWD

# create a build area
echo "removing old build area"
rm -rf /tmp/hltd-build-tmp-area
echo "creating new build area"
mkdir  /tmp/hltd-build-tmp-area
cd     /tmp/hltd-build-tmp-area
TOPDIR=$PWD

echo "Moving files to their destination"
mkdir -p var/log/hltd
mkdir -p var/log/hltd/pid
mkdir -p opt/hltd
mkdir -p etc/init.d
mkdir -p etc/logrotate.d
mkdir -p etc/appliance/resources/idle
mkdir -p etc/appliance/resources/online
mkdir -p etc/appliance/resources/except
mkdir -p etc/appliance/resources/quarantined
mkdir -p etc/appliance/resources/cloud
ls
cp -r $BASEDIR/python/hltd $TOPDIR/etc/init.d/hltd
cp -r $BASEDIR/python/soap2file $TOPDIR/etc/init.d/soap2file
cp -r $BASEDIR/* $TOPDIR/opt/hltd
touch $TOPDIR/opt/hltd/scratch/new-version
rm -rf $TOPDIR/opt/hltd/python/hltd
rm -rf $TOPDIR/opt/hltd/python/soap2file
rm -rf $TOPDIR/opt/hltd/python/riverd
rm -rf $TOPDIR/opt/hltd/python/river-daemon.py
cp -r $BASEDIR/etc/hltd.conf $TOPDIR/etc/
cp -r $BASEDIR/etc/logrotate.d/hltd $TOPDIR/etc/logrotate.d/
echo "working in $PWD"
ls opt/hltd

echo "Creating DQM directories"
mkdir -p etc/appliance/dqm_resources/idle
mkdir -p etc/appliance/dqm_resources/online
mkdir -p etc/appliance/dqm_resources/except
mkdir -p etc/appliance/dqm_resources/quarantined
mkdir -p etc/appliance/dqm_resources/cloud

rm -rf $TOPDIR/opt/hltd/bin
rm -rf $TOPDIR/opt/hltd/rpm
rm -rf $TOPDIR/opt/hltd/lib
rm -rf $TOPDIR/opt/hltd/esplugins
rm -rf $TOPDIR/opt/hltd/scripts/paramcache*
rm -rf $TOPDIR/opt/hltd/scripts/*rpm.sh
rm -rf $TOPDIR/opt/hltd/scripts/*.php
rm -rf $TOPDIR/opt/hltd/scripts/fff-es

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > hltd.spec <<EOF
Name: hltd
Version: 1.9.9
Release: 5
Summary: hlt daemon
License: gpl
Group: DAQ
Packager: smorovic
Source: none
%define _tmppath $TOPDIR/hltd-build
BuildRoot: %{_tmppath}
BuildArch: $BUILD_ARCH
AutoReqProv: no
Provides:/opt/hltd
Provides:/etc/hltd.conf
Provides:/etc/logrotate.d/hltd
Provides:/etc/init.d/hltd
Provides:/etc/init.d/soap2file
Requires:hltd-libs >= 1.9.6,SOAPpy,python-simplejson >= 3.3.1,jsonMerger,python-psutil

%description
fff hlt daemon

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
%__install -d "%{buildroot}/var/log/hltd"
%__install -d "%{buildroot}/var/log/hltd/pid"
tar -C $TOPDIR -c opt/hltd | tar -xC \$RPM_BUILD_ROOT
tar -C $TOPDIR -c etc | tar -xC \$RPM_BUILD_ROOT
rm \$RPM_BUILD_ROOT/opt/hltd/python/setupmachine.py
rm \$RPM_BUILD_ROOT/opt/hltd/python/disablenode.py
rm \$RPM_BUILD_ROOT/opt/hltd/python/dbcheck.py
rm \$RPM_BUILD_ROOT/opt/hltd/TODO
%post
%files
%dir %attr(777, -, -) /var/log/hltd
%dir %attr(777, -, -) /var/log/hltd/pid
%defattr(-, root, root, -)
/opt/hltd/
/etc/hltd.conf
/etc/logrotate.d/hltd
/etc/init.d/hltd
/etc/init.d/soap2file
/etc/appliance
%preun
if [ \$1 == 0 ]; then
  /sbin/service hltd stop || true
  /sbin/service soap2file stop || true
fi
EOF
mkdir -p RPMBUILD/{RPMS/{noarch},SPECS,BUILD,SOURCES,SRPMS}
rpmbuild --define "_topdir `pwd`/RPMBUILD" -bb hltd.spec
#rm -rf patch-cmssw-tmp

