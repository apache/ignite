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

package org.apache.ignite.client;

/**
 * Client operation type.
 */
public enum ClientOperationType {
    CACHE_CREATE,
    CACHE_GET_OR_CREATE,
    CACHE_GET_NAMES,
    CACHE_DESTROY,
    CACHE_GET,
    CACHE_PUT,
    CACHE_CONTAINS_KEY,
    CACHE_CONTAINS_KEYS,
    CACHE_GET_CONFIGURATION,
    CACHE_GET_SIZE,
    CACHE_PUT_ALL,
    CACHE_GET_ALL,
    CACHE_REPLACE,
    CACHE_REMOVE_ONE,
    CACHE_REMOVE_MULTIPLE,
    CACHE_REMOVE_EVERYTHING,
    CACHE_CLEAR_ONE,
    CACHE_CLEAR_MULTIPLE,
    CACHE_CLEAR_EVERYTHING,
    CACHE_GET_AND_PUT,
    CACHE_GET_AND_REMOVE,
    CACHE_GET_AND_REPLACE,
    CACHE_PUT_IF_ABSENT,
    CACHE_GET_AND_PUT_IF_ABSENT,
    CACHE_PARTITIONS,
    QUERY_SCAN,
    QUERY_SQL,
    QUERY_CONTINUOUS,
    GET_BINARY_TYPE,
    REGISTER_BINARY_TYPE_NAME,
    PUT_BINARY_TYPE,
    GET_BINARY_TYPE_NAME,
    TRANSACTION_START,
    CLUSTER_GET_STATE,
    CLUSTER_CHANGE_STATE,
    CLUSTER_GET_WAL_STATE,
    CLUSTER_CHANGE_WAL_STATE,
    CLUSTER_GROUP_GET_NODE_IDS,
    CLUSTER_GROUP_GET_NODE_INFO,
    COMPUTE_TASK_EXECUTE,
    SERVICE_INVOKE,
    SERVICE_GET_DESCRIPTORS,
    SERVICE_GET_DESCRIPTOR
}
