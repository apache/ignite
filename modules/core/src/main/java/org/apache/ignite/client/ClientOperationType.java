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

import java.util.Set;

/**
 * Client operation type.
 */
public enum ClientOperationType {
    /**
     * Create cache ({@link IgniteClient#createCache(String)},
     * {@link IgniteClient#createCache(ClientCacheConfiguration)}).
     */
    CACHE_CREATE,

    /**
     * Get or create cache ({@link IgniteClient#getOrCreateCache(String)},
     * {@link IgniteClient#getOrCreateCache(ClientCacheConfiguration)}).
     */
    CACHE_GET_OR_CREATE,

    /**
     * Get cache names ({@link IgniteClient#cacheNames()}).
     */
    CACHE_GET_NAMES,

    /**
     * Destroy cache ({@link IgniteClient#destroyCache(String)}).
     */
    CACHE_DESTROY,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_GET,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_PUT,

    /**
     * Determines if the cache contains a key ({@link ClientCache#containsKey(Object)}).
     */
    CACHE_CONTAINS_KEY,

    /**
     * Determines if the cache contains multiple keys ({@link ClientCache#containsKeys}).
     */
    CACHE_CONTAINS_KEYS,

    /**
     * Get cache configuration ({@link ClientCache#getConfiguration()}).
     */
    CACHE_GET_CONFIGURATION,

    /**
     * Get cache size ({@link ClientCache#size}).
     */
    CACHE_GET_SIZE,

    /**
     * Put values to cache ({@link ClientCache#putAll}).
     */
    CACHE_PUT_ALL,

    /**
     * Get values from cache ({@link ClientCache#getAll}).
     */
    CACHE_GET_ALL,

    /**
     * Replace cache value ({@link ClientCache#replace(Object, Object)},
     * {@link ClientCache#replace(Object, Object, Object)}).
     */
    CACHE_REPLACE,

    /**
     * Remove entry from cache ({@link ClientCache#remove(Object)}, {@link ClientCache#remove(Object, Object)}).
     */
    CACHE_REMOVE_ONE,

    /**
     * Remove entries from cache ({@link ClientCache#removeAll(Set)}).
     */
    CACHE_REMOVE_MULTIPLE,

    /**
     * Remove everyting from cache ({@link ClientCache#removeAll()}).
     */
    CACHE_REMOVE_EVERYTHING,

    /**
     * Clear cache entry ({@link ClientCache#clear(Object)} ).
     */
    CACHE_CLEAR_ONE,

    /**
     * Clear multiple cache entries ({@link ClientCache#clearAll(Set)}).
     */
    CACHE_CLEAR_MULTIPLE,

    /**
     * Clear entire cache ({@link ClientCache#clear()}).
     */
    CACHE_CLEAR_EVERYTHING,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_GET_AND_PUT,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_GET_AND_REMOVE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_GET_AND_REPLACE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_PUT_IF_ABSENT,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_GET_AND_PUT_IF_ABSENT,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CACHE_PARTITIONS,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    QUERY_SCAN,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    QUERY_SQL,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    QUERY_CONTINUOUS,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    GET_BINARY_TYPE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    REGISTER_BINARY_TYPE_NAME,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    PUT_BINARY_TYPE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    GET_BINARY_TYPE_NAME,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    TRANSACTION_START,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_GET_STATE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_CHANGE_STATE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_GET_WAL_STATE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_CHANGE_WAL_STATE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_GROUP_GET_NODE_IDS,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    CLUSTER_GROUP_GET_NODE_INFO,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    COMPUTE_TASK_EXECUTE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    SERVICE_INVOKE,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    SERVICE_GET_DESCRIPTORS,

    /**
     * Get value from cache ({@link ClientCache#get(Object)}).
     */
    SERVICE_GET_DESCRIPTOR
}
