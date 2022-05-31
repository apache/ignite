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

package org.apache.ignite.gatling.builder.cache

import io.gatling.core.session.Expression

import java.util.concurrent.locks.Lock

final class Cache(requestName: Expression[String], cacheName: Expression[String]) {
    def put[K, V](key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
        CachePutActionBuilder(requestName, cacheName, key, value)
    def putAll[K, V](map: Expression[Map[K, V]]): CachePutAllActionBuilder[K, V] =
        CachePutAllActionBuilder(requestName, cacheName, map)
    def get[K, V](key: Expression[K]): CacheGetActionBuilder[K, V] =
        CacheGetActionBuilder(requestName, cacheName, key)
    def getAll[K, V](keys: Expression[Set[K]]): CacheGetAllActionBuilder[K, V] =
        CacheGetAllActionBuilder(requestName, cacheName, keys)
    def remove[K](key: Expression[K]): CacheRemoveActionBuilder[K] =
        CacheRemoveActionBuilder(requestName, cacheName, key)
    def removeAll[K](keys: Expression[Set[K]]): CacheRemoveAllActionBuilder[K] =
        CacheRemoveAllActionBuilder(requestName, cacheName, keys)
    def invoke[K, V, T](key: Expression[K]): CacheInvokeActionBuilderBase[K, V, T] =
        CacheInvokeActionBuilderBase[K, V, T](requestName, cacheName, key)
    def lock[K](key: Expression[K]): CacheLockActionBuilder[K] =
        CacheLockActionBuilder(requestName, cacheName, key)
    def unlock(lock: Expression[Lock]): CacheUnlockActionBuilder =
        CacheUnlockActionBuilder(requestName, cacheName, lock)
    def sql(sql: Expression[String]): CacheSqlActionBuilder =
        CacheSqlActionBuilder(requestName, cacheName, sql)
}
