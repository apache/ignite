/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// For server side we should load required libraries.
if (typeof window === 'undefined') {
    $commonUtils = require('./common-utils');
}

// Entry point for common data structures.
$dataStructures = {};

// Ignite events groups.
$dataStructures.EVENT_GROUPS = {
    EVTS_CHECKPOINT: ['EVT_CHECKPOINT_SAVED', 'EVT_CHECKPOINT_LOADED', 'EVT_CHECKPOINT_REMOVED'],
    EVTS_DEPLOYMENT: ['EVT_CLASS_DEPLOYED', 'EVT_CLASS_UNDEPLOYED', 'EVT_CLASS_DEPLOY_FAILED', 'EVT_TASK_DEPLOYED',
        'EVT_TASK_UNDEPLOYED', 'EVT_TASK_DEPLOY_FAILED'],
    EVTS_ERROR: ['EVT_JOB_TIMEDOUT', 'EVT_JOB_FAILED', 'EVT_JOB_FAILED_OVER', 'EVT_JOB_REJECTED', 'EVT_JOB_CANCELLED',
        'EVT_TASK_TIMEDOUT', 'EVT_TASK_FAILED', 'EVT_CLASS_DEPLOY_FAILED', 'EVT_TASK_DEPLOY_FAILED',
        'EVT_TASK_DEPLOYED', 'EVT_TASK_UNDEPLOYED', 'EVT_CACHE_REBALANCE_STARTED', 'EVT_CACHE_REBALANCE_STOPPED'],
    EVTS_DISCOVERY: ['EVT_NODE_JOINED', 'EVT_NODE_LEFT', 'EVT_NODE_FAILED', 'EVT_NODE_SEGMENTED',
        'EVT_CLIENT_NODE_DISCONNECTED', 'EVT_CLIENT_NODE_RECONNECTED'],
    EVTS_JOB_EXECUTION: ['EVT_JOB_MAPPED', 'EVT_JOB_RESULTED', 'EVT_JOB_FAILED_OVER', 'EVT_JOB_STARTED',
        'EVT_JOB_FINISHED', 'EVT_JOB_TIMEDOUT', 'EVT_JOB_REJECTED', 'EVT_JOB_FAILED', 'EVT_JOB_QUEUED',
        'EVT_JOB_CANCELLED'],
    EVTS_TASK_EXECUTION: ['EVT_TASK_STARTED', 'EVT_TASK_FINISHED', 'EVT_TASK_FAILED', 'EVT_TASK_TIMEDOUT',
        'EVT_TASK_SESSION_ATTR_SET', 'EVT_TASK_REDUCED'],
    EVTS_CACHE: ['EVT_CACHE_ENTRY_CREATED', 'EVT_CACHE_ENTRY_DESTROYED', 'EVT_CACHE_OBJECT_PUT',
        'EVT_CACHE_OBJECT_READ', 'EVT_CACHE_OBJECT_REMOVED', 'EVT_CACHE_OBJECT_LOCKED', 'EVT_CACHE_OBJECT_UNLOCKED',
        'EVT_CACHE_OBJECT_SWAPPED', 'EVT_CACHE_OBJECT_UNSWAPPED', 'EVT_CACHE_OBJECT_EXPIRED'],
    EVTS_CACHE_REBALANCE: ['EVT_CACHE_REBALANCE_STARTED', 'EVT_CACHE_REBALANCE_STOPPED',
        'EVT_CACHE_REBALANCE_PART_LOADED', 'EVT_CACHE_REBALANCE_PART_UNLOADED', 'EVT_CACHE_REBALANCE_OBJECT_LOADED',
        'EVT_CACHE_REBALANCE_OBJECT_UNLOADED', 'EVT_CACHE_REBALANCE_PART_DATA_LOST'],
    EVTS_CACHE_LIFECYCLE: ['EVT_CACHE_STARTED', 'EVT_CACHE_STOPPED', 'EVT_CACHE_NODES_LEFT'],
    EVTS_CACHE_QUERY: ['EVT_CACHE_QUERY_EXECUTED', 'EVT_CACHE_QUERY_OBJECT_READ'],
    EVTS_SWAPSPACE: ['EVT_SWAP_SPACE_CLEARED', 'EVT_SWAP_SPACE_DATA_REMOVED', 'EVT_SWAP_SPACE_DATA_READ',
        'EVT_SWAP_SPACE_DATA_STORED', 'EVT_SWAP_SPACE_DATA_EVICTED'],
    EVTS_IGFS: ['EVT_IGFS_FILE_CREATED', 'EVT_IGFS_FILE_RENAMED', 'EVT_IGFS_FILE_DELETED', 'EVT_IGFS_FILE_OPENED_READ',
        'EVT_IGFS_FILE_OPENED_WRITE', 'EVT_IGFS_FILE_CLOSED_WRITE', 'EVT_IGFS_FILE_CLOSED_READ', 'EVT_IGFS_FILE_PURGED',
        'EVT_IGFS_META_UPDATED', 'EVT_IGFS_DIR_CREATED', 'EVT_IGFS_DIR_RENAMED', 'EVT_IGFS_DIR_DELETED']
};

// Pairs of Java build-in classes.
$dataStructures.JAVA_BUILD_IN_CLASSES = [
    {short: 'BigDecimal', full: 'java.math.BigDecimal'},
    {short: 'Boolean', full: 'java.lang.Boolean'},
    {short: 'Byte', full: 'java.lang.Byte'},
    {short: 'Date', full: 'java.sql.Date'},
    {short: 'Double', full: 'java.lang.Double'},
    {short: 'Float', full: 'java.lang.Float'},
    {short: 'Integer', full: 'java.lang.Integer'},
    {short: 'Long', full: 'java.lang.Long'},
    {short: 'Object', full: 'java.lang.Object'},
    {short: 'Short', full: 'java.lang.Short'},
    {short: 'String', full: 'java.lang.String'},
    {short: 'Time', full: 'java.sql.Time'},
    {short: 'Timestamp', full: 'java.sql.Timestamp'},
    {short: 'UUID', full: 'java.util.UUID'}
];

/**
 * @param clsName Class name to check.
 * @returns 'true' if given class name is a java build-in type.
 */
$dataStructures.isJavaBuildInClass = function (clsName) {
    if ($commonUtils.isDefined(clsName)) {
        for (var i = 0; i < $dataStructures.JAVA_BUILD_IN_CLASSES.length; i++) {
            var jbic = $dataStructures.JAVA_BUILD_IN_CLASSES[i];

            if (clsName == jbic.short || clsName == jbic.full)
                return true;
        }
    }

    return false;
};

/**
 * @param clsName Class name to check.
 * @returns Full class name for java build-in types or source class otherwise.
 */
$dataStructures.fullClassName = function (clsName) {
    if ($commonUtils.isDefined(clsName)) {
        for (var i = 0; i < $dataStructures.JAVA_BUILD_IN_CLASSES.length; i++) {
            var jbic = $dataStructures.JAVA_BUILD_IN_CLASSES[i];

            if (clsName == jbic.short)
                return jbic.full;
        }
    }

    return clsName;
};

// For server side we should export properties generation entry point.
if (typeof window === 'undefined') {
    module.exports = $dataStructures;
}
