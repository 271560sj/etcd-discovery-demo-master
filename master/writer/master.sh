#!/bin/sh

function watcher(){
   echo "Waiting for keys $1...."
   while true
   do
      keysValue=$(./etcdctl watch  $1)
      echo $keysValue
   done
}
watcher $1