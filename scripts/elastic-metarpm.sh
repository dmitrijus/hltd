#!/bin/bash -e
BUILD_ARCH=noarch
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

PARAMCACHE="paramcache"

if [ -n "$1" ]; then
  #PARAMCACHE=$1
  PARAMCACHE=${1##*/}
fi

echo "Using cache file $PARAMCACHE"

if [ -f $SCRIPTDIR/$PARAMCACHE ];
then
  readarray lines < $SCRIPTDIR/$PARAMCACHE
  for (( i=0; i < 12; i++ ))
  do
    lines[$i]=`echo -n ${lines[$i]} | tr -d "\n"`
  done
else
  for (( i=0; i < 12; i++ ))
  do
    lines[$i]=""
  done
fi

echo "Enviroment (prod,vm) (press enter for \"${lines[0]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[0]=$readin
fi

#PACKAGENAME="fffmeta-elastic"
if [ ${lines[0]} == "prod" ]; then
  PACKAGENAME="fffmeta-elastic"
elif [ ${lines[0]} == "vm" ]; then
  PACKAGENAME="fffmeta-elastic-vm"
else
  echo "Environment ${lines[0]} not supported. Available: prod or vm"
  exit 1
fi


nousevar=$readin
nousevar=$readin
lines[1]="null"
lines[2]="null"

echo "HWCFG DB server (press enter for \"${lines[3]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[3]=$readin
fi

echo "HWCFG DB SID (or db name in VM enviroment) (press enter for: \"${lines[4]}\"):"
echo "(SPECIFIES address in TNSNAMES.ORA file if DB server field was \"null\"!)"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[4]=$readin
fi

echo "HWCFG DB username (press enter for: \"${lines[5]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[5]=$readin
fi

echo "HWCFG DB password (press enter for: \"${lines[6]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[6]=$readin
fi

echo "Equipment set (press enter for: \"${lines[7]}\") - type 'latest' or enter a specific one:"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[7]=$readin
fi

lines[8]="null"
lines[9]="null"
lines[10]="null"
lines[11]="null"

params=""
for (( i=0; i < 12; i++ ))
do
  params="$params ${lines[i]}"
done

# create a build area

echo "removing old build area"
rm -rf /tmp/$PACKAGENAME-build-tmp
echo "creating new build area"
mkdir  /tmp/$PACKAGENAME-build-tmp
ls
cd     /tmp/$PACKAGENAME-build-tmp
mkdir BUILD
mkdir RPMS
TOPDIR=$PWD
echo "working in $PWD"
ls

pluginpath="/opt/fff/esplugins/"
pluginname1="bigdesk"
pluginfile1="bigdesk-505b32e-mod2.zip"
pluginname2="head"
pluginfile2="head-master.zip"
pluginname3="kopf"
pluginfile3="elasticsearch-kopf-2.1.1.zip"

#pluginname4="river-runriver"
#riverfile="river-runriver-1.4.0-jar-with-dependencies.jar"
#es 1.4 compatible:
riverfile="river-runriver-1.3.6-jar-with-dependencies.jar"

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > fffmeta-elastic.spec <<EOF
Name: $PACKAGENAME
Version: 1.8.0
Release: 1
Summary: hlt daemon
License: gpl
Group: DAQ
Packager: smorovic
Source: none
%define __jar_repack %{nil}
%define _topdir $TOPDIR
BuildArch: $BUILD_ARCH
AutoReqProv: no
Requires:elasticsearch => 1.4.5, cx_Oracle >= 5.1.2, java-1.8.0-oracle-headless >= 1.8.0.45 , php >= 5.3.3, php-oci8 >= 1.4.9 

Provides:/opt/fff/configurefff.sh
Provides:/opt/fff/setupmachine.py
Provides:/opt/fff/closeRunIndices.php
Provides:/etc/init.d/fffmeta
Provides:/etc/init.d/fff-es
Provides:/opt/fff/daemon2.py
Provides:/opt/fff/river-daemon.py
Provides:/opt/fff/istribe.py
Provides:/etc/init.d/riverd
Provides:/opt/ff/river.jar
Provides:/etc/rsyslog.d/48-river.conf

%description
fffmeta configuration setup package

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
%__install -d "%{buildroot}/opt/fff"
%__install -d "%{buildroot}/opt/fff/backup"
%__install -d "%{buildroot}/opt/fff/esplugins"
%__install -d "%{buildroot}/etc/init.d"
%__install -d "%{buildroot}/etc/rsyslog.d"

mkdir -p opt/fff/esplugins
mkdir -p opt/fff/backup
mkdir -p etc/init.d/
mkdir -p etc/rsyslog.d
cp $BASEDIR/etc/rsyslog.d/48-river.conf %{buildroot}/etc/rsyslog.d/48-river.conf
cp $BASEDIR/python/setupmachine.py %{buildroot}/opt/fff/setupmachine.py
cp $BASEDIR/python/istribe.py %{buildroot}/opt/fff/istribe.py
cp $BASEDIR/python/daemon2.py %{buildroot}/opt/fff/daemon2.py
cp $BASEDIR/python/river-daemon.py %{buildroot}/opt/fff/river-daemon.py
cp $BASEDIR/python/riverd %{buildroot}/etc/init.d/riverd
echo "#!/bin/bash" > %{buildroot}/opt/fff/configurefff.sh
echo python2.6 /opt/fff/setupmachine.py elasticsearch,web $params >> %{buildroot}/opt/fff/configurefff.sh 

cp $BASEDIR/esplugins/$riverfile %{buildroot}/opt/fff/river.jar
cp $BASEDIR/esplugins/$pluginfile1 %{buildroot}/opt/fff/esplugins/$pluginfile1
cp $BASEDIR/esplugins/$pluginfile2 %{buildroot}/opt/fff/esplugins/$pluginfile2
cp $BASEDIR/esplugins/$pluginfile3 %{buildroot}/opt/fff/esplugins/$pluginfile3
#cp $BASEDIR/esplugins/$pluginfile4 %{buildroot}/opt/fff/esplugins/$pluginfile4
cp $BASEDIR/esplugins/install.sh %{buildroot}/opt/fff/esplugins/install.sh
cp $BASEDIR/esplugins/uninstall.sh %{buildroot}/opt/fff/esplugins/uninstall.sh
cp $BASEDIR/scripts/fff-es %{buildroot}/etc/init.d/fff-es

cp $BASEDIR/scripts/closeRunIndices.php %{buildroot}/opt/fff/closeRunIndices.php

echo "#!/bin/bash"                       >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "# chkconfig:   2345 79 22"         >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"start\" ]; then"   >> %{buildroot}/etc/init.d/fffmeta
echo "  /opt/fff/configurefff.sh"  >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"restart\" ]; then" >> %{buildroot}/etc/init.d/fffmeta
echo "/opt/fff/configurefff.sh"    >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"status\" ]; then"  >> %{buildroot}/etc/init.d/fffmeta
echo "echo fffmeta does not have status" >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta


%files
%defattr(-, root, root, -)
#/opt/fff
%attr( 755 ,root, root) /opt/fff/setupmachine.py
%attr( 755 ,root, root) /opt/fff/setupmachine.pyc
%attr( 755 ,root, root) /opt/fff/setupmachine.pyo
%attr( 755 ,root, root) /opt/fff/istribe.py
%attr( 755 ,root, root) /opt/fff/istribe.pyc
%attr( 755 ,root, root) /opt/fff/istribe.pyo
%attr( 755 ,root, root) /opt/fff/daemon2.py
%attr( 755 ,root, root) /opt/fff/daemon2.pyc
%attr( 755 ,root, root) /opt/fff/daemon2.pyo
%attr( 755 ,root, root) /opt/fff/river-daemon.py
%attr( 755 ,root, root) /opt/fff/river-daemon.pyc
%attr( 755 ,root, root) /opt/fff/river-daemon.pyo
%attr( 700 ,root, root) /opt/fff/configurefff.sh
%attr( 755 ,root, root) /opt/fff/closeRunIndices.php
%attr( 755 ,root, root) /etc/init.d/fffmeta
%attr( 755 ,root, root) /etc/init.d/fff-es
%attr( 755 ,root, root) /etc/init.d/riverd
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile1
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile2
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile3
#%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile4
%attr( 755 ,root, root) /opt/fff/esplugins/install.sh
%attr( 755 ,root, root) /opt/fff/esplugins/uninstall.sh
%attr( 755 ,root, root) /opt/fff/river.jar
%attr( 444 ,root, root) /etc/rsyslog.d/48-river.conf

%post
#echo "post install trigger"
chkconfig --del fffmeta
chkconfig --add fffmeta
chkconfig --del riverd
chkconfig --add riverd
#disabled, can be run manually for now

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install"
#/sbin/service elasticsearch stop
python2.6 /opt/fff/setupmachine.py restore,elasticsearch
python2.6 /opt/fff/setupmachine.py elasticsearch,web $params
#update permissions in case new rpm changed uid/guid
chown -R elasticsearch:elasticsearch /var/log/elasticsearch
chown -R elasticsearch:elasticsearch /var/lib/elasticsearch
chmod a+r -R /etc/elasticsearch
tr=\$(python2.6 /opt/fff/istribe.py);
if [ \$tr == "es-tribe" ]; then
  echo "Tribe machine: not installing plugins" #some seem to break tribe
elif [ \$tr == "es-vm-tribe" ]; then
  echo "Tribe VM machine: not installing plugins" #some seem to break tribe
else
  echo "Installing plugins..."
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 > /dev/null
  /opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile1 $pluginname1

  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 > /dev/null
  /opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile2 $pluginname2

  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 > /dev/null
  /opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile3 $pluginname3

  #/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 > /dev/null
  #/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile4 $pluginname4
fi

chkconfig --del elasticsearch
chkconfig --add elasticsearch
#restart (should be re-enabled)
if [ -d /elasticsearch ]
then
  if [ ! -d /elasticsearch/lib/elasticsearch ]
  then
      mkdir -p /elasticsearch/lib/elasticsearch
      chown -R elasticsearch:elasticsearch /elasticsearch/lib/elasticsearch
  fi
fi
        
#/sbin/service elasticsearch start
/etc/init.d/riverd restart

%preun

if [ \$1 == 0 ]; then 

  chkconfig --del fffmeta
  chkconfig --del riverd
  chkconfig --del elasticsearch
  /sbin/service riverd stop || true

  #/sbin/service elasticsearch stop || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 || true


  python2.6 /opt/fff/setupmachine.py restore,elasticsearch
fi

#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta-elastic.spec

