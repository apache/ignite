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

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;

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
     * Put value to cache ({@link ClientCache#put(Object, Object)}).
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
     * Remove everything from cache ({@link ClientCache#removeAll()}).
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
     * Get and put ({@link ClientCache#getAndPut(Object, Object)}).
     */
    CACHE_GET_AND_PUT,

    /**
     * Get and remove ({@link ClientCache#getAndRemove(Object)}).
     */
    CACHE_GET_AND_REMOVE,

    /**
     * Get and replace ({@link ClientCache#getAndReplace(Object, Object)}).
     */
    CACHE_GET_AND_REPLACE,

    /**
     * Put if absent ({@link ClientCache#putIfAbsent(Object, Object)}).
     */
    CACHE_PUT_IF_ABSENT,

    /**
     * Get and put if absent ({@link ClientCache#getAndPutIfAbsent(Object, Object)}).
     */
    CACHE_GET_AND_PUT_IF_ABSENT,

    /**
     * Scan query ({@link ClientCache#query(Query)}).
     */
    QUERY_SCAN,

    /**
     * SQL query ({@link ClientCache#query(SqlFieldsQuery)}).
     */
    QUERY_SQL,

    /**
     * Continuous query ({@link ClientCache#query(ContinuousQuery, ClientDisconnectListener)}).
     */
    QUERY_CONTINUOUS,

    /**
     * Index query ({@link ClientCache#query(Query)}).
     */
    QUERY_INDEX,

    /**
     * Start transaction ({@link ClientTransactions#txStart}).
     */
    TRANSACTION_START,

    /**
     * Get cluster state ({@link ClientCluster#state()}).
     */
    CLUSTER_GET_STATE,

    /**
     * Change cluster state ({@link ClientCluster#state(ClusterState)}).
     */
    CLUSTER_CHANGE_STATE,

    /**
     * Get cluster WAL state ({@link ClientCluster#isWalEnabled(String)}).
     */
    CLUSTER_GET_WAL_STATE,

    /**
     * Change cluster WAL state ({@link ClientCluster#enableWal(String)}, {@link ClientCluster#disableWal(String)}).
     */
    CLUSTER_CHANGE_WAL_STATE,

    /**
     * Get cluster nodes ({@link ClientCluster#nodes()}).
     */
    CLUSTER_GROUP_GET_NODES,

    /**
     * Execute compute task ({@link ClientCompute#execute(String, Object)}).
     */
    COMPUTE_TASK_EXECUTE,

    /**
     * Invoke service.
     */
    SERVICE_INVOKE,

    /**
     * Get service descriptors ({@link ClientServices#serviceDescriptors()}).
     */
    SERVICE_GET_DESCRIPTORS,

    /**
     * Get service descriptor ({@link ClientServices#serviceDescriptor(String)}).
     */
    SERVICE_GET_DESCRIPTOR,

    /**
     * Get or create an AtomicLong ({@link IgniteClient#atomicLong(String, long, boolean)},
     * {@link IgniteClient#atomicLong(String, ClientAtomicConfiguration, long, boolean)}).
     */
    ATOMIC_LONG_CREATE,

    /**
     * Remove an AtomicLong ({@link ClientAtomicLong#close()}).
     */
    ATOMIC_LONG_REMOVE,

    /**
     * Check if AtomicLong exists ({@link ClientAtomicLong#removed()}).
     */
    ATOMIC_LONG_EXISTS,

    /**
     * AtomicLong.get ({@link ClientAtomicLong#get()}).
     */
    ATOMIC_LONG_VALUE_GET,

    /**
     * AtomicLong.addAndGet (includes {@link ClientAtomicLong#addAndGet(long)}, {@link ClientAtomicLong#incrementAndGet()},
     * {@link ClientAtomicLong#getAndIncrement()}, {@link ClientAtomicLong#getAndAdd(long)}, {@link ClientAtomicLong#decrementAndGet()},
     * {@link ClientAtomicLong#getAndDecrement()}).
     */
    ATOMIC_LONG_VALUE_ADD_AND_GET,

    /**
     * AtomicLong.getAndSet ({@link ClientAtomicLong#getAndSet(long)}).
     */
    ATOMIC_LONG_VALUE_GET_AND_SET,

    /**
     * AtomicLong.compareAndSet ({@link ClientAtomicLong#compareAndSet(long, long)}).
     */
    ATOMIC_LONG_VALUE_COMPARE_AND_SET,

    /**
     * Create an IgniteSet ({@link IgniteClient#set(String, ClientCollectionConfiguration)}).
     */
    SET_GET_OR_CREATE,

    /**
     * Remove an IgniteSet ({@link ClientIgniteSet#close()}).
     */
    SET_REMOVE,

    /**
     * Check if IgniteSet exists ({@link ClientIgniteSet#removed()}).
     */
    SET_EXISTS,

    /**
     * IgniteSet.add ({@link ClientIgniteSet#add(Object)}).
     */
    SET_VALUE_ADD,

    /**
     * IgniteSet.addAll ({@link ClientIgniteSet#addAll(Collection)}).
     */
    SET_VALUE_ADD_ALL,

    /**
     * IgniteSet.remove ({@link ClientIgniteSet#remove}).
     */
    SET_VALUE_REMOVE,

    /**
     * IgniteSet.removeAll ({@link ClientIgniteSet#removeAll}).
     */
    SET_VALUE_REMOVE_ALL,

    /**
     * IgniteSet.contains ({@link ClientIgniteSet#contains(Object)}).
     */
    SET_VALUE_CONTAINS,

    /**
     * IgniteSet.containsAll ({@link ClientIgniteSet#containsAll}).
     */
    SET_VALUE_CONTAINS_ALL,

    /**
     * IgniteSet.retainAll ({@link ClientIgniteSet#retainAll}).
     */
    SET_VALUE_RETAIN_ALL,

    /**
     * IgniteSet.size ({@link ClientIgniteSet#size()}).
     */
    SET_SIZE,

    /**
     * IgniteSet.clear ({@link ClientIgniteSet#clear()}).
     */
    SET_CLEAR,

    /**
     * IgniteSet.iterator ({@link ClientIgniteSet#iterator()}, {@link ClientIgniteSet#toArray()}).
     */
    SET_ITERATOR
}
