#!/bin/bash
# Add this and all nodes to the baseline after waiting for the delay time
IGNITE_CONSISTENT_ID=$1
IGNITE_AUTO_BASELINE_DELAY=$2

# Activate or set a baseline for all current server nodes, but only after a while.
if [ ! -z "$IGNITE_CONSISTENT_ID" ]  && [ "$IGNITE_AUTO_BASELINE_DELAY" -ne 0 ]
then 
    sleep $IGNITE_AUTO_BASELINE_DELAY
    
    $IGNITE_HOME/bin/control.sh --baseline > /tmp/baseline
    if [ $? == 0 ]
    then     

        X=`egrep "Cluster state.* active" /tmp/baseline`
        if [ "$?" != 0 ]
        then 
            $IGNITE_HOME/bin/control.sh --activate
        else
            X=`sed -n '/Other/q;p' /tmp/baseline | grep "ConsistentID=${IGNITE_CONSISTENT_ID}"` 
            if [ "$?" != 0 ]; then
                VERSION=`grep "Current topology version" /tmp/baseline | grep -oP '\d+'`
                $IGNITE_HOME/bin/control.sh --baseline version $VERSION
            fi
        fi
    fi
fi
      