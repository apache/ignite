#!/bin/bash

function download {
  wget -O ignite.zip $1

  unzip ignite.zip -d ignite

  rm ignite.zip

  exit 0
}

if [ ! -z $IGNITE_URL ]; then
  download $IGNITE_URL
fi

if [ ! -z $IGNITE_VERSION ]; then
  if [[ $IGNITE_VERSION  =~ [0-9]*\.[0-9]*\.0 ]]; then
    download http://apache-mirror.rbc.ru/pub/apache/incubator/ignite/${IGNITE_VERSION}/apache-ignite-fabric-${IGNITE_VERSION}-incubating-bin.zip
  else
    download http://www.gridgain.com/media/gridgain-community-fabric-${IGNITE_VERSION}.zip
  fi
fi

if [ -z $IGNITE_SOURCE ] || [ $IGNITE_SOURCE = "COMMUNITY" ]; then
  download http://tiny.cc/updater/download_community.php
fi

if [ $IGNITE_SOURCE = "APACHE" ]; then
  download http://tiny.cc/updater/download_ignite.php
fi

echo "Unsupported IGNITE_SOURCE type: ${IGNITE_SOURCE}"
