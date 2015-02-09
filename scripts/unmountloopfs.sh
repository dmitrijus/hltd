#!/bin/bash
if [ -n "$1" ]; then
  if [ -d $1 ]; then

    basedir=`readlink -e $1`
    umask 0
    points=`mount | grep $basedir/ | grep /dev/loop | awk '{print $3}'`
    imgs=`mount | grep $basedir/ | grep /dev/loop | awk '{print $1}'`
    pointarr=( $points )
    imgarr=( $imgs )

    len=${#pointarr[@]}
    len2=${#imgarr[@]}
    if [[ $len == 0 ]]; then
      exit 0
    fi
    max=$((len))

    for i in $(seq 0 1 $max)
    do
      if [ $i == $max ]; then continue; fi
      point=${pointarr[$i]}
      image=${imgarr[$i]}
      #protect from dangerous action
      if [ $point == "/" ]; then continue; fi
      if [ $point == "//" ]; then continue; fi
      if [ $point == "/fff" ]; then continue; fi
      if [ $point == "/fff/" ]; then continue; fi
      if [ $point == "/fff/ramdisk" ]; then continue; fi
      if [ $point == "/fff/ramdisk/" ]; then continue; fi
      if [ $point == "fff/ramdisk" ]; then continue; fi
      if [ $point == "fff/ramdisk/" ]; then continue; fi

      echo "found mountpoint $point $image"
      #kill any processes that might use the mount point and remove from NFS
      fuser -km $point
      #unmunt loop device
      sleep 0.2
      exportfs -u *:$point
      umount $point
      if [ $? != 0 ]; then
        sleep 0.1
        fuser -km $point
        sleep 0.2
        exportfs -u *:$point
        umount $point
        if [ $? != 0 ]; then
          echo "Unsuccessful unmount of $point !"
          exit 1
        fi
      fi

      #deleting mount point
      exportfs -u *:$point
      rm -rf $point
      if [ $? != 0 ]; then
        echo "Unsuccessful delete of unmounted mount point $point !"
        exit 2
      fi

      #remove image
      chmod 755 $image
      rm -rf $image
      if [ $? != 0 ]; then
        echo "Unsuccessful delete of image file $image"
        exit 3
      fi
    done
    exit 0
  else
    echo "base directory not found!"
  fi
fi
exit 1
