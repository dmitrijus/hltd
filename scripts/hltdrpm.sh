#!/bin/bash -e
# numArgs=$#
# if [ $numArgs -lt 4 ]; then
#     echo "Usage: patch-cmssw-build.sh CMSSW_X_Y_Z patchId {dev|pro|...} patchdir"
#     exit -1
# fi
# CMSSW_VERSION=$1            # the CMSSW version, as known to scram
# PATCH_ID=$2                 # an arbitrary tag which identifies the extra code (usually, "p1", "p2", ...)
# AREA=$3                     # "pro", "dev", etc...
# LOCAL_CODE_PATCHES_TOP=$4   # absolute path to the area where extra code to be compiled in can be found, equivalent to $CMSSW_BASE/src
alias python=python2.6
# set the RPM build architecture
#BUILD_ARCH=$(uname -i)      # "i386" for SLC4, "x86_64" for SLC5
BUILD_ARCH=x86_64
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

# create a build area

echo "removing old build area"
rm -rf /tmp/hltd-build-tmp
echo "creating new build area"
mkdir  /tmp/hltd-build-tmp
ls
cd     /tmp/hltd-build-tmp
TOPDIR=$PWD
ls


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
mkdir -p usr/lib64/python2.6/site-packages
mkdir -p usr/lib64/python2.6/site-packages/pyelasticsearch
ls
cp -r $BASEDIR/python/hltd $TOPDIR/etc/init.d/hltd
cp -r $BASEDIR/python/soap2file $TOPDIR/etc/init.d/soap2file
cp -r $BASEDIR/* $TOPDIR/opt/hltd
rm -rf $TOPDIR/opt/hltd/python/hltd
rm -rf $TOPDIR/opt/hltd/python/soap2file
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

cd $TOPDIR
#pyelasticsearch
cd opt/hltd/lib/pyelasticsearch-0.6/
./setup.py -q build
python - <<'EOF'
import py_compile
py_compile.compile("build/lib/pyelasticsearch/__init__.py")
py_compile.compile("build/lib/pyelasticsearch/client.py")
py_compile.compile("build/lib/pyelasticsearch/downtime.py")
py_compile.compile("build/lib/pyelasticsearch/exceptions.py")
EOF
python -O - <<'EOF'
import py_compile
py_compile.compile("build/lib/pyelasticsearch/__init__.py")
py_compile.compile("build/lib/pyelasticsearch/client.py")
py_compile.compile("build/lib/pyelasticsearch/downtime.py")
py_compile.compile("build/lib/pyelasticsearch/exceptions.py")
EOF
cp build/lib/pyelasticsearch/*.pyo $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp build/lib/pyelasticsearch/*.py $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp build/lib/pyelasticsearch/*.pyc $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp -r pyelasticsearch.egg-info/ $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch

cd $TOPDIR
#python-prctl
cd opt/hltd/lib/python-prctl/
./setup.py -q build
python - <<'EOF'
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/prctl.py")
EOF
python -O - <<'EOF'
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/prctl.py")
EOF
cp build/lib.linux-x86_64-2.6/prctl.pyo $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/prctl.py $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/prctl.pyc $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/_prctl.so $TOPDIR/usr/lib64/python2.6/site-packages
cat > $TOPDIR/usr/lib64/python2.6/site-packages/python_prctl-1.5.0-py2.6.egg-info <<EOF
Metadata-Version: 1.0
Name: python-prctl
Version: 1.5.0
Summary: Python(ic) interface to the linux prctl syscall
Home-page: http://github.com/seveas/python-prctl
Author: Dennis Kaarsemaker
Author-email: dennis@kaarsemaker.net
License: UNKNOWN
Description: UNKNOWN
Platform: UNKNOWN
Classifier: Development Status :: 5 - Production/Stable
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: GNU General Public License (GPL)
Classifier: Operating System :: POSIX :: Linux
Classifier: Programming Language :: C
Classifier: Programming Language :: Python
Classifier: Topic :: Security
EOF

cd $TOPDIR
cd opt/hltd/lib/python-inotify-0.5/
./setup.py -q build
cp build/lib.linux-x86_64-2.6/inotify/_inotify.so $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/inotify/watcher.py $TOPDIR/usr/lib64/python2.6/site-packages
python - <<'EOF'
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/inotify/watcher.py")
EOF
cp build/lib.linux-x86_64-2.6/inotify/watcher.pyc $TOPDIR/usr/lib64/python2.6/site-packages/
cat > $TOPDIR/usr/lib64/python2.6/site-packages/python_inotify-0.5.egg-info <<EOF
Metadata-Version: 1.0
Name: python-inotify
Version: 0.5
Summary: Interface to Linux inotify subsystem
Home-page: 'http://www.serpentine.com/
Author: Bryan O'Sullivan
Author-email: bos@serpentine.com
License: LGPL
Platform: Linux
Classifier: Development Status :: 5 - Production/Stable
Classifier: Environment :: Console
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: LGPL
Classifier: Natural Language :: English
Classifier: Operating System :: POSIX :: Linux
Classifier: Programming Language :: Python
Classifier: Programming Language :: Python :: 2.4
Classifier: Programming Language :: Python :: 2.5
Classifier: Programming Language :: Python :: 2.6
Classifier: Programming Language :: Python :: 2.7
Classifier: Topic :: Software Development :: Libraries :: Python Modules
Classifier: Topic :: System :: Filesystems
Classifier: Topic :: System :: Monitoring
EOF


cd $TOPDIR
cd opt/hltd/lib/python-procname/
./setup.py -q build
cp build/lib.linux-x86_64-2.6/procname.so $TOPDIR/usr/lib64/python2.6/site-packages

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > hltd.spec <<EOF
Name: hltd
Version: 1.6.0
Release: 1
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
Provides:/usr/lib64/python2.6/site-packages/prctl.pyc
Requires:python,libcap,python-six,python-requests,SOAPpy,python-simplejson >= 3.3.1,jsonMerger

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
tar -C $TOPDIR -c usr | tar -xC \$RPM_BUILD_ROOT
rm \$RPM_BUILD_ROOT/opt/hltd/python/setupmachine.py
rm \$RPM_BUILD_ROOT/opt/hltd/rpm/*.rpm
%post
#/opt/hltd/python/fillresources.py #--> in fffmeta
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
/usr/lib64/python2.6/site-packages/*prctl*
/usr/lib64/python2.6/site-packages/*watcher*
/usr/lib64/python2.6/site-packages/*_inotify.so*
/usr/lib64/python2.6/site-packages/*python_inotify*
/usr/lib64/python2.6/site-packages/pyelasticsearch
/usr/lib64/python2.6/site-packages/procname.so
%preun
if [ \$1 == 0 ]; then
  /sbin/service hltd stop || true
  /sbin/service soap2file stop || true
fi
EOF
mkdir -p RPMBUILD/{RPMS/{noarch},SPECS,BUILD,SOURCES,SRPMS}
rpmbuild --define "_topdir `pwd`/RPMBUILD" -bb hltd.spec
#rm -rf patch-cmssw-tmp

