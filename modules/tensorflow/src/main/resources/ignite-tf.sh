#!/usr/bin/env bash

java -Xms4G -Xmx4G -DIGNITE_QUIET=false -jar lib/ignite-tensorflow-2.7.0-SNAPSHOT-jar-with-dependencies.jar "$@"