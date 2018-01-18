#!/bin/sh

function createKeys() {
  echo "Waiting for create value of key $1"
  while true
  do
     keysValue=$(./etcdctl set $1 $1"/hello`date +%Y%m%d%H%M%s`")
     echo "set value of $1 is $keysValue"
  done
}
createKeys $1