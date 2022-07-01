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

import java.util.concurrent.locks.Lock

import javax.cache.processor.MutableEntry

import io.gatling.core.session.Expression
import org.apache.ignite.cache.CacheEntryProcessor

/**
 * DSL to create cache operations.
 */
trait Cache {
  /**
   * Start constructing of the cache put action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @return PutBuilder
   */
  def put[K, V]: PutBuilder[K, V] = PutBuilder()

  /**
   * Helper cache put action builder.
   *
   * Supports two flavours of configuration. The first takes two separate parameters for key and value, another one
   * accepts a 2-tuple containing both key and pair.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   */
  case class PutBuilder[K, V]() {

    /**
     * Starts cache put action constructing accepting separate key and value parameters.
     *
     * @param cacheName Cache name.
     * @param key The cache entry key.
     * @param value The cache entry value.
     * @return ActionBuilder
     */
    def apply(cacheName: Expression[String], key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
      new CachePutActionBuilder(cacheName, session => key(session).flatMap(k => value(session).map(v => (k, v))))

    /**
     * Starts put action constructing accepting a single tuple parameter containing both key and value.
     *
     * @param cacheName Cache name.
     * @param pair Tuple containing both cache entry key and value.
     * @return ActionBuilder
     */
    def apply(cacheName: Expression[String], pair: Expression[(K, V)]): CachePutActionBuilder[K, V] =
      new CachePutActionBuilder(cacheName, pair)
  }

  /**
   * Starts constructing of the cache get action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @return ActionBuilder
   */
  def get[K, V](cacheName: Expression[String], key: Expression[K]): CacheGetActionBuilder[K, V] =
    new CacheGetActionBuilder(cacheName, key)

  /**
   * Starts constructing of the SQL query action.
   *
   * @param cacheName Cache name.
   * @param query SQL query string.
   * @return ActionBuilder
   */
  def sql(cacheName: Expression[String], query: Expression[String]): CacheSqlActionBuilder =
    CacheSqlActionBuilder(cacheName, query)

  /**
   * Starts constructing of the cache putAll action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @param map Collection of the cache entry keys and values.
   * @return ActionBuilder
   */
  def putAll[K, V](cacheName: Expression[String], map: Expression[Map[K, V]]): CachePutAllActionBuilder[K, V] =
    new CachePutAllActionBuilder(cacheName, map)

  /**
   * Starts constructing of the cache getAndRemove action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @return ActionBuilder
   */
  def getAndRemove[K, V](cacheName: Expression[String], key: Expression[K]): CacheGetAndRemoveActionBuilder[K, V] =
    new CacheGetAndRemoveActionBuilder(cacheName, key)

  /**
   * Starts constructing of the cache getAndRemove action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @param value The cache entry value.
   * @return ActionBuilder
   */
  def getAndPut[K, V](cacheName: Expression[String], key: Expression[K], value: Expression[V]): CacheGetAndPutActionBuilder[K, V] =
    new CacheGetAndPutActionBuilder(cacheName, key, value)

  /**
   * Starts constructing of the cache getAll action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @param keys Collection of the cache entry keys.
   * @return ActionBuilder
   */
  def getAll[K, V](cacheName: Expression[String], keys: Expression[Set[K]]): CacheGetAllActionBuilder[K, V] =
    new CacheGetAllActionBuilder(cacheName, keys)

  /**
   * Starts constructing of the cache remove action.
   *
   * @tparam K Type of the cache key.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @return ActionBuilder
   */
  def remove[K](cacheName: Expression[String], key: Expression[K]): CacheRemoveActionBuilder[K] =
    new CacheRemoveActionBuilder(cacheName, key)

  /**
   * Starts constructing of the cache removeAll action.
   *
   * @tparam K Type of the cache key.
   * @param cacheName Cache name.
   * @param keys Collection of the cache entry keys.
   * @return ActionBuilder
   */
  def removeAll[K](cacheName: Expression[String], keys: Expression[Set[K]]): CacheRemoveAllActionBuilder[K] =
    new CacheRemoveAllActionBuilder(cacheName, keys)

  /**
   * Starts constructing of the cache entry processor invoke action.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @tparam T Type of the cache entry processor result.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @return ActionBuilder
   */
  def invoke[K, V, T](cacheName: Expression[String], key: Expression[K]): CacheInvokeActionBuilderBase[K, V, T] =
    CacheInvokeActionBuilderBase[K, V, T](cacheName, key)

  /**
   * Starts constructing of the cache invokeAll action. This is a variant with the same
   * entry processor instance for all keys passed.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @tparam T Type of the cache entry processor result.
   * @param cacheName Cache name.
   * @param keys Collection of the cache entry keys.
   * @return ActionBuilder
   */
  def invokeAll[K, V, T](cacheName: Expression[String], keys: Expression[Set[K]]): CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T] =
    CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T](cacheName, keys)

  /**
   * Starts constructing of the cache invokeAll action. This is a variant with the individual
   * entry processor instance for each keys passed.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @tparam T Type of the cache entry processor result.
   * @param cacheName Cache name.
   * @param map Collection of the cache entry keys and the corresponding entry processor instances.
   * @return ActionBuilder
   */
  def invokeAll[K, V, T](
    cacheName: Expression[String],
    map: Expression[Map[K, CacheEntryProcessor[K, V, T]]]
  ): CacheInvokeAllMultipleProcessorsActionBuilder[K, V, T] =
    new CacheInvokeAllMultipleProcessorsActionBuilder[K, V, T](cacheName, map)

  /**
   * Starts constructing of the cache entry lock action.
   *
   * @tparam K Type of the cache key.
   * @param cacheName Cache name.
   * @param key The cache entry key.
   * @return ActionBuilder
   */
  def lock[K](cacheName: Expression[String], key: Expression[K]): CacheLockActionBuilder[K] =
    new CacheLockActionBuilder(cacheName, key)

  /**
   * Starts constructing of the cache entry unlock action.
   *
   * @param cacheName Cache name.
   * @param lock The lock instance to release.
   * @return ActionBuilder
   */
  def unlock(cacheName: Expression[String], lock: Expression[Lock]): CacheUnlockActionBuilder =
    new CacheUnlockActionBuilder(cacheName, lock)

  /**
   * Implicit conversion to support lambda usage in place of CacheEntryProcessor for invoke and invokeAll actions
   * with additional arguments.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @tparam T Type of the cache entry processor result.
   * @param f Function to be used as a cache entry processor.
   * @return CacheEntryProcessor instance.
   */
  implicit def toCacheEntryProcessor[K, V, T](f: (MutableEntry[K, V], Seq[Any]) => T): CacheEntryProcessor[K, V, T] =
    new CacheEntryProcessor[K, V, T] {
      override def process(mutableEntry: MutableEntry[K, V], objects: Object*): T =
        f.apply(mutableEntry, objects)
    }

  /**
   * Implicit conversion to support lambda usage in place of CacheEntryProcessor for invoke and invokeAll actions.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @tparam T Type of the cache entry processor result.
   * @param f Function to be used as a cache entry processor.
   * @return CacheEntryProcessor instance.
   */
  implicit def toCacheEntryProcessor[K, V, T](f: MutableEntry[K, V] => T): CacheEntryProcessor[K, V, T] =
    new CacheEntryProcessor[K, V, T] {
      override def process(mutableEntry: MutableEntry[K, V], objects: Object*): T =
        f.apply(mutableEntry)
    }
}
