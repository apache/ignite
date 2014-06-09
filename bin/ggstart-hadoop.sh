#!/bin/bash
#
# @sh.file.header
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: @sh.file.version
#

#
# Grid command line loader with hadoop classpath.
#

#
# Check HADOOP_HOME
#

HADOOP_COMMON_HOME=

if [ "$HADOOP_HOME" == "" ]; then
    #Try get all variables from /etc/default
    HADOOP_DEFAULTS=/etc/default/hadoop

    if [ -f $HADOOP_DEFAULTS ]; then
        . $HADOOP_DEFAULTS
    fi
fi

if [ "$HADOOP_HOME" == "" ]; then
    echo ERROR: HADOOP_HOME variable is not set.
    exit 1
fi

echo
echo "INFO: Hadoop was found in $HADOOP_HOME"
echo

#
# Setting all hadoop modules if it's not set by /etc/default/hadoop
#
if [ "$HADOOP_COMMON_HOME" == "" ]; then
    export HADOOP_COMMON_HOME=$HADOOP_HOME/share/hadoop/common
fi

if [ "$HADOOP_HDFS_HOME" == "" ]; then
    HADOOP_HDFS_HOME=$HADOOP_HOME/share/hadoop/hdfs
fi

if [ "$HADOOP_MAPRED_HOME" == "" ]; then
    HADOOP_MAPRED_HOME=$HADOOP_HOME/share/hadoop/mapreduce
fi

#
# OS specific support.
#
SEP=":"

case "`uname`" in
    MINGW*)
        SEP=";" ;;
    CYGWIN*)
        SEP=";" ;;
esac

#
# Libraries included in classpath.
#

CP="$HADOOP_COMMON_HOME/lib/*${SEP}$HADOOP_MAPRED_HOME/lib/*${SEP}$HADOOP_MAPRED_HOME/lib/*"

files=$(ls \
$HADOOP_HDFS_HOME/hadoop-hdfs-* \
$HADOOP_COMMON_HOME/hadoop-common-* \
$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-common-* \
$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-core-*)

for file in $files; do
    if [ ${file: -10} != "-tests.jar" ]; then
        CP=${CP}${SEP}$file
    fi
done

CP=${CP}${SEP}$file

export GRIDGAIN_HADOOP_CLASSPATH=$CP

$(dirname $0)/ggstart.sh $@
