#!/bin/bash

if [ -z $SKIP_DOWNLOAD ]; then
  ./download_ignite.sh
fi

if [ -z $SKIP_BUILD_LIBS ]; then
  ./build_users_libs.sh
fi

./execute.sh