#!/bin/bash
# Add this and all nodes to the baseline after waiting for the delay time
IGNITE_CONSISTENT_ID=$1
IGNITE_AUTO_BASELINE_DELAY=$2

# allow control.sh to use its default java options rather than the ones from the environment
export JVM_OPTS=


# Activate or set a baseline for all current server nodes, but only after a while.
if [ ! -z "$IGNITE_CONSISTENT_ID" ]  && [ "$IGNITE_AUTO_BASELINE_DELAY" -ne 0 ]
then 
    sleep $IGNITE_AUTO_BASELINE_DELAY
    
    $IGNITE_HOME/bin/control.sh --baseline > /tmp/baseline
    if [ $? -eq 0 ]
    then     

        X=`egrep "Cluster state.* active" /tmp/baseline`
        if [ $? -ne 0 ]
        then 
            X=`egrep "inactive" /tmp/baseline`
            if [ "$?" -eq 0 ]
            then
            
                # cluster is not active
                # case 1: new cluster - want to activate
                # case 2: cluster restarting without all baseline nodes or perhaps some new nodes - activation risks data loss.
                
                # if there 
                X=`grep "Baseline nodes not found." /tmp/baseline` 
                if [ "$?" -eq 0 ] 
                then             
                    $IGNITE_HOME/bin/control.sh --activate
                fi      
            # else did not get a baseline, just bail out
            fi
        else
            # this cluster is already active
            # case 1:  new nodes added to expand cluster, use them
            # case 2:  new nodes are replacements for failed nodes, use them, because cluster is already active
            
            
            X=`sed -n '/Other/q;p' /tmp/baseline | grep "ConsistentID=${IGNITE_CONSISTENT_ID}"` 
            if [ $? -ne 0 ]; then
                VERSION=`grep "Current topology version" /tmp/baseline | grep -oP '\d+'`
                $IGNITE_HOME/bin/control.sh --baseline version $VERSION
            fi
        fi
    fi
fi
      