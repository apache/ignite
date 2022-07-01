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
package org.apache.ignite.gatling.api

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery

/**
 * Wrapper around the Ignite Cache API abstracting access either via the Ignite node (thick) API or vai the
 * Ignite Client (thin) API.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 */
// noinspection AccessorLikeMethodIsUnit
trait CacheApi[K, V] {
  /**
   * Puts entry to cache.
   *
   * @param key Key.
   * @param value Value.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously puts entry to cache.
   *
   * @param key Key.
   * @param value Value.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Puts batch of entries to cache.
   *
   * @param map Key / value pairs map.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def putAll(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously puts batch of entries to cache.
   *
   * @param map Key / value pairs map.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def putAllAsync(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Gets an entry from cache by key.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously gets an entry from cache by key.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Gets a collection of entries from the cache.
   *
   * @param keys Collection of keys.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAll(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously gets a collection of entries from the cache.
   *
   * @param keys Collection of keys.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAllAsync(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Removes the entry for a key only if currently mapped to some.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously removes the entry for a key only if currently mapped to some.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Puts value with the specified key in this cache, returning an existing value
   * if one existed.
   *
   * @param key Key.
   * @param value Value.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously puts value with the specified key in this cache, returning an existing
   * value if one existed.
   *
   * @param key Key.
   * @param value Value.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Removes the entry for a key.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously removes the entry for a key.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Removes a collection of entries from the cache.
   *
   * @param keys Set of keys.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Asynchronously removes a collection of entries from the cache.
   *
   * @param keys Set of keys.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Invokes an CacheEntryProcessor against the entry specified by the provided key.
   *
   * @tparam T Type of the return value.
   * @param key Key.
   * @param entryProcessor Entry processor.
   * @param arguments Additional arguments to pass to the entry processor.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
    s: Map[K, T] => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Asynchronously invokes an CacheEntryProcessor against the entry specified by the provided key.
   *
   * @tparam T Type of the return value.
   * @param key Key.
   * @param entryProcessor Entry processor.
   * @param arguments Additional arguments to pass to the entry processor.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
    s: Map[K, T] => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Invokes each EntryProcessor from map's values against the correspondent
   * entry specified by map's key set.
   *
   * @tparam T Type of the return value.
   * @param map Map containing keys and entry processors to be applied to values.
   * @param arguments Additional arguments to pass to the entry processors.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def invokeAll[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
    s: Map[K, EntryProcessorResult[T]] => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Asynchronously invokes each EntryProcessor from map's values against the correspondent
   * entry specified by map's key set.
   *
   * @tparam T Type of the return value.
   * @param map Map containing keys and entry processors to be applied to values.
   * @param arguments Additional arguments to pass to the entry processors.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def invokeAllAsync[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
    s: Map[K, EntryProcessorResult[T]] => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Acquires lock associated with a passed key.
   *
   * @param key Key.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit

  /**
   * Releases the lock.
   *
   * @param lock Lock.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Executes the Ignite SqlFieldsQuery
   *
   * @param query Query object.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit): Unit

  /**
   * Returns cache that will operate with binary objects.
   *
   * @return Instance of Cache API.
   */
  def withKeepBinary(): CacheApi[K, V]
}
