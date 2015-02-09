#!/bin/bash -e
BUILD_ARCH=noarch
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

PACKAGENAME="fffmeta-tribe"

PARAMCACHE="paramcache"

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
rm -rf /tmp/fffmeta-tribe-build-tmp
echo "creating new build area"
mkdir  /tmp/fffmeta-tribe-build-tmp
ls
cd     /tmp/fffmeta-tribe-build-tmp
mkdir BUILD
mkdir RPMS
TOPDIR=$PWD
echo "working in $PWD"
ls

pluginpath="/opt/fff/esplugins/"
pluginname1="bigdesk"
pluginfile1="lukas-vlcek-bigdesk-v2.4.0-2-g9807b92-mod.zip"
pluginname2="head"
pluginfile2="head-master.zip"
pluginname3="HQ"
pluginfile3="hq-master.zip"
pluginname4="paramedic"
pluginfile4="paramedic-master.zip"

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > fffmeta-tribe.spec <<EOF
Name: $PACKAGENAME
Version: 1.6.0
Release: 0
Summary: hlt daemon
License: gpl
Group: DAQ
Packager: smorovic
Source: none
%define _topdir $TOPDIR
BuildArch: $BUILD_ARCH
AutoReqProv: no
Requires:elasticsearch >= 1.4.2, cx_Oracle >= 5.1.2, java-1.7.0-openjdk, httpd >= 2.2.15, php >= 5.3.3, php-oci8 >= 1.4.9 

Provides:/opt/fff/configurefff.sh
Provides:/opt/fff/setupmachine.py
Provides:/etc/init.d/fffmeta

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

mkdir -p opt/fff/esplugins
mkdir -p opt/fff/backup
mkdir -p etc/init.d/
cp $BASEDIR/python/setupmachine.py %{buildroot}/opt/fff/setupmachine.py
echo "#!/bin/bash" > %{buildroot}/opt/fff/configurefff.sh
echo python2.6 /opt/fff/setupmachine.py elasticsearch,web $params >> %{buildroot}/opt/fff/configurefff.sh 

cp $BASEDIR/esplugins/$pluginfile1 %{buildroot}/opt/fff/esplugins/$pluginfile1
cp $BASEDIR/esplugins/$pluginfile2 %{buildroot}/opt/fff/esplugins/$pluginfile2
cp $BASEDIR/esplugins/$pluginfile3 %{buildroot}/opt/fff/esplugins/$pluginfile3
cp $BASEDIR/esplugins/$pluginfile4 %{buildroot}/opt/fff/esplugins/$pluginfile4
cp $BASEDIR/esplugins/install.sh %{buildroot}/opt/fff/esplugins/install.sh
cp $BASEDIR/esplugins/uninstall.sh %{buildroot}/opt/fff/esplugins/uninstall.sh

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
%attr( 700 ,root, root) /opt/fff/configurefff.sh
%attr( 755 ,root, root) /etc/init.d/fffmeta
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile1
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile2
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile3
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile4
%attr( 755 ,root, root) /opt/fff/esplugins/install.sh
%attr( 755 ,root, root) /opt/fff/esplugins/uninstall.sh

%post
#echo "post install trigger"
chkconfig --del fffmeta
chkconfig --add fffmeta
#disabled, can be run manually for now

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install"
/sbin/service elasticsearch stop
python2.6 /opt/fff/setupmachine.py restore,elasticsearch
python2.6 /opt/fff/setupmachine.py elasticsearch,web $params
#update permissions in case new rpm changed uid/guid
chown -R elasticsearch:elasticsearch /var/log/elasticsearch
chown -R elasticsearch:elasticsearch /var/lib/elasticsearch

/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 > /dev/null
/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile1 $pluginname1

/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 > /dev/null
/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile2 $pluginname2

/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 > /dev/null
/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile3 $pluginname3

/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 > /dev/null
/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile4 $pluginname4

chkconfig --del elasticsearch
chkconfig --add elasticsearch
chkconfig --add httpd
#todo:kill java process if running to have clean restart
/sbin/service elasticsearch start
/sbin/service httpd restart || true

%preun

if [ \$1 == 0 ]; then 

  chkconfig --del fffmeta
  chkconfig --del elasticsearch
  chkconfig --del httpd

  /sbin/service elasticsearch stop || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 || true
  /sbin/service httpd stop || true


  python2.6 /opt/fff/setupmachine.py restore,elasticsearch
fi

#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta-tribe.spec

