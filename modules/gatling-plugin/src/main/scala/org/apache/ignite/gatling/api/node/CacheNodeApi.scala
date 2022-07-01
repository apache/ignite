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
package org.apache.ignite.gatling.api.node

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.gatling.api.CacheApi

/**
 * Implementation of CacheApi working via the Ignite Node (thick) API.
 *
 * @param wrapped Instance of Cache API.
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 */
case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V]) extends CacheApi[K, V] with StrictLogging {
  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param value @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync put")
    Try(wrapped.put(key, value))
      .map(_ => ())
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param value @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async put")
    wrapped
      .putAsync(key, value)
      .listen(future =>
        Try(future.get())
          .map(_ => ())
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param map @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def putAll(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync putAll")
    Try(wrapped.putAll(map.asJava))
      .map(_ => ())
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param map @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def putAllAsync(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async putAll")
    wrapped
      .putAllAsync(map.asJava)
      .listen(future =>
        Try(future.get())
          .map(_ => ())
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync get")
    Try(wrapped.get(key))
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async get")
    wrapped
      .getAsync(key)
      .listen(future =>
        Try(future.get())
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAndRemove")
    Try(wrapped.getAndRemove(key))
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAndRemove")
    wrapped
      .getAndRemoveAsync(key)
      .listen(future =>
        Try(future.get())
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param value @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAndPut")
    Try(wrapped.getAndPut(key, value))
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param value @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAndPut")
    wrapped
      .getAndPutAsync(key, value)
      .listen(future =>
        Try(future.get())
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param keys @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAll(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAll")
    Try(wrapped.getAll(keys.asJava))
      .map(_.asScala.toMap)
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param keys @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getAllAsync(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAll")
    wrapped
      .getAllAsync(keys.asJava)
      .listen(future =>
        Try(future.get())
          .map(_.asScala.toMap)
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync remove")
    Try(wrapped.remove(key))
      .map(_ => ())
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async remove")
    wrapped
      .removeAsync(key)
      .listen(future =>
        Try(future.get())
          .map(_ => ())
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param keys @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync removeAll")
    Try(wrapped.removeAll(keys.asJava))
      .map(_ => ())
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param keys @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async removeAll")
    wrapped
      .removeAllAsync(keys.asJava)
      .listen(future =>
        Try(future.get())
          .map(_ => ())
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @tparam T @inheritdoc
   * @param key @inheritdoc
   * @param entryProcessor @inheritdoc
   * @param arguments @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
    s: Map[K, T] => Unit,
    f: Throwable => Unit
  ): Unit = {
    logger.debug("sync invoke")
    Try(wrapped.invoke[T](key, entryProcessor, arguments: _*))
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @tparam T @inheritdoc
   * @param key @inheritdoc
   * @param entryProcessor @inheritdoc
   * @param arguments @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
    s: Map[K, T] => Unit,
    f: Throwable => Unit
  ): Unit = {
    logger.debug("async invoke")
    wrapped
      .invokeAsync(key, entryProcessor, arguments: _*)
      .listen(future =>
        Try(future.get())
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @tparam T @inheritdoc
   * @param map @inheritdoc
   * @param arguments @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def invokeAll[T](
    map: Map[K, CacheEntryProcessor[K, V, T]],
    arguments: Any*
  )(s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync invokeAll")
    Try(wrapped.invokeAll[T](map.asJava, arguments: _*))
      .map(_.asScala.toMap)
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @tparam T @inheritdoc
   * @param map @inheritdoc
   * @param arguments @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def invokeAllAsync[T](
    map: Map[K, CacheEntryProcessor[K, V, T]],
    arguments: Any*
  )(s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async invokeAll")
    wrapped
      .invokeAllAsync(map.asJava, arguments: _*)
      .listen(future =>
        Try(future.get())
          .map(_.asScala.toMap)
          .fold(f, s)
      )
  }

  /**
   * @inheritdoc
   * @param key @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync lock")
    Try {
      val lock = wrapped.lock(key)
      lock.lock()
      lock
    }.fold(f, s)
  }

  /**
   * @inheritdoc
   * @param lock @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync unlock")
    Try(lock.unlock())
      .fold(f, s)
  }

  /**
   * @inheritdoc
   * @param query @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync sql")
    Try(
      wrapped
        .query(query)
        .getAll
        .asScala
        .toList
        .map(_.asScala.toList)
    ).fold(f, s)
  }

  /**
   * @inheritdoc
   * @return @inheritdoc
   */
  override def withKeepBinary(): CacheApi[K, V] = copy(wrapped = wrapped.withKeepBinary())
}
