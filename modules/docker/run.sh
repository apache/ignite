#!/bin/bash

IGNITE_VERSION=""

if [ -z $SKIP_BUILD_LIBS ]; then
  ./build_users_libs.sh

  IGNITE_VERSION=$(mvn -f user-repo/pom.xml dependency:list | grep ':ignite-core:jar:.*:' | \
    sed -rn 's/.*([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')
fi

if [ -z $SKIP_DOWNLOAD ]; then
  IGNITE_VERSION=$IGNITE_VERSION ./download_ignite.sh
fi

./execute.sh