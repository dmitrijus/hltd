#!/bin/bash
if [ -n "$1" ]; then
  if [ -n "$2" ]; then
    if [ -n "$3" ]; then

      if [ -d $1 ]; then

        basedir=`readlink -e $1`
        image=$basedir/$2.img
        mountpoint=$basedir/$2
        sizemb=$3
        ret=0
        umask 0

        #protect from going wrong
        if [ "$mountpoint" == "/" ]; then exit 99; fi
        if [ "$mountpoint" == "//" ]; then exit 99; fi
        if [ "$mountpoint" == "/fff" ]; then exit 99; fi
        if [ "$mountpoint" == "/fff/" ]; then exit 99; fi
        if [ "$mountpoint" == "/fff/ramdisk" ]; then exit 99; fi 
        if [ "$mountpoint" == "/fff/ramdisk/" ]; then exit 99; fi 
        if [ "$mountpoint" == "fff/ramdisk" ]; then exit 99; fi 
        if [ "$mountpoint" == "fff/ramdisk/" ]; then exit 99; fi 

        echo "makeloop script invoked for creating loop device disk $2 in ${basedir} of size $3 MB"

        if [ -d $mountpoint ]; then

          point=`mount | grep $mountpoint | grep /dev/loop | awk '{print $3}'`

          if [ "$point" != "" ]; then
            #kill any processes that might use the mount point and remove from NFS
            fuser -km $point
            exportfs -u *:$point
            #unmunt loop device
            umount $point
            if [ $? != 0 ]; then
              sleep 0.1
              fuser -km $point
              exportfs -u *:$point
              umount $point
              if [ $? != 0 ]; then
                echo "Unsuccessful umount of $point !"
                exit 1
              fi
            fi
            exportfs -u *:$point
          fi
        fi
        #deleting mount point
        rm -rf $mountpoint
        if [ $? != 0 ]; then
          echo "Unsuccessful delete of unmounted mount point $mountpoint !"
          exit 2
        fi

        if [ -f $image ]; then
          chmod 755 $image
          rm -rf $image
          if [ $? != 0 ]; then
            echo "Unsuccessful delete old image file $image"
            exit 3
          fi
        fi
    
        dd if=/dev/zero of=$image bs=1048576 count=$sizemb >& /dev/null
        echo y | mkfs.ext3 $image > /dev/null
        #try mount
        mkdir $mountpoint
        if [ $? != 0 ]; then
          echo "Unsuccessful make mount point directory!"
          exit 4
        fi

        echo "mounting image directory..."
        mount -o loop,noatime $image $mountpoint
        if [ $? != 0 ]; then
          echo "Unsuccessful mount with parameters $image $mountpoint"
          exit 5
        fi

        chmod -R 777 $mountpoint

        exportfs -o rw,sync,no_root_squash,no_subtree_check *:$mountpoint
        if [ $? != 0 ]; then
          echo "exportfs command failed for $mountpoint !"
          exit 6
        fi
        exit 0
        #end
      else
        echo "base directory not found!"
      fi
    else
      echo "No parameter 3 given!"
    fi
  else
    echo "No parameter 2 given!"
  fi
else
  echo "No parameter 1 given!"
fi

echo "Usage: makeloopfs.sh basedir subdir imgsize(MB)"
exit 1

