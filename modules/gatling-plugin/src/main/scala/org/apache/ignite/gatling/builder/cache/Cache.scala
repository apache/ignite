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

trait Cache {

  def put[K, V]: PutBuilder[K, V] = PutBuilder()

  case class PutBuilder[K, V]() {
    def apply(cacheName: Expression[String], key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
      CachePutActionBuilder(cacheName, session =>
        key(session).flatMap(k => value(session).map(v => (k, v))))

    def apply(cacheName: Expression[String], pair: Expression[(K, V)]): CachePutActionBuilder[K, V] =
      CachePutActionBuilder(cacheName, pair)
  }

  def get[K, V](cacheName: Expression[String], key: Expression[K]): CacheGetActionBuilder[K, V] =
    CacheGetActionBuilder(cacheName, key)

  def sql(cacheName: Expression[String], query: Expression[String]): CacheSqlActionBuilder =
    CacheSqlActionBuilder(cacheName, query)

  def putAll[K, V](cacheName: Expression[String], map: Expression[Map[K, V]]): CachePutAllActionBuilder[K, V] =
    CachePutAllActionBuilder(cacheName, map)

  def getAndRemove[K, V](cacheName: Expression[String], key: Expression[K]): CacheGetAndRemoveActionBuilder[K, V] =
    CacheGetAndRemoveActionBuilder(cacheName, key)

  def getAndPut[K, V](cacheName: Expression[String], key: Expression[K], value: Expression[V]): CacheGetAndPutActionBuilder[K, V] =
    CacheGetAndPutActionBuilder(cacheName, key, value)

  def getAll[K, V](cacheName: Expression[String], keys: Expression[Set[K]]): CacheGetAllActionBuilder[K, V] =
    CacheGetAllActionBuilder(cacheName, keys)

  def remove[K](cacheName: Expression[String], key: Expression[K]): CacheRemoveActionBuilder[K] =
    CacheRemoveActionBuilder(cacheName, key)

  def removeAll[K](cacheName: Expression[String], keys: Expression[Set[K]]): CacheRemoveAllActionBuilder[K] =
    CacheRemoveAllActionBuilder(cacheName, keys)

  def invoke[K, V, T](cacheName: Expression[String], key: Expression[K]): CacheInvokeActionBuilderBase[K, V, T] =
    CacheInvokeActionBuilderBase[K, V, T](cacheName, key)

  def invokeAll[K, V, T](cacheName: Expression[String], keys: Expression[Set[K]]): CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T] =
    CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T](cacheName, keys)

  def invokeAll[K, V, T](cacheName: Expression[String], map: Expression[Map[K, CacheEntryProcessor[K, V, T]]]): CacheInvokeAllActionBuilderBase[K, V, T] =
    CacheInvokeAllActionBuilderBase[K, V, T](cacheName, map)

  def lock[K](cacheName: Expression[String], key: Expression[K]): CacheLockActionBuilder[K] =
    CacheLockActionBuilder(cacheName, key)

  def unlock(cacheName: Expression[String], lock: Expression[Lock]): CacheUnlockActionBuilder =
    CacheUnlockActionBuilder(cacheName, lock)

  implicit def toCacheEntryProcessor[K, V, T](f: (MutableEntry[K, V], Seq[Any]) => T): CacheEntryProcessor[K, V, T] =
    new CacheEntryProcessor[K, V, T] {
      override def process(mutableEntry: MutableEntry[K, V], objects: Object*): T =
        f.apply(mutableEntry, Seq(objects))
    }

  implicit def toCacheEntryProcessor[K, V, T](f: MutableEntry[K, V] => T): CacheEntryProcessor[K, V, T] =
    new CacheEntryProcessor[K, V, T] {
      override def process(mutableEntry: MutableEntry[K, V], objects: Object*): T =
        f.apply(mutableEntry)
    }
}
