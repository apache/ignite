#!/bin/bash

if [ -z $GIT_REPO ]; then
  echo Users git repo is not provided.

  exit 0
fi

git clone $GIT_REPO user-repo

cd user-repo

if [ ! -z $GIT_BRANCH ]; then
  git checkout $GIT_BRANCH
fi

if [ ! -z "$BUILD_CMD" ]; then
  echo "Starting to execute build command: $BUILD_CMD"

  eval "$BUILD_CMD"
else
  mvn clean package
fi