#!/usr/bin/env bash

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
java -Xms4G -Xmx4G -DIGNITE_QUIET=false -cp "$SCRIPT_PATH/lib/*" org.apache.ignite.tensorflow.submitter.JobSubmitter "$@"