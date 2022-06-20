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

package org.apache.ignite.gatling.api.thin

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.client.ClientCache
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.CompletionSupport

case class CacheThinApi[K, V](wrapped: ClientCache[K, V])(implicit val ec: ExecutionContext)
  extends CacheApi[K, V] with CompletionSupport with StrictLogging {

  override def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync put")
    Try {
      wrapped.put(key, value)
    }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async put")
    withCompletion(wrapped.putAsync(key, value).asScala.map(_ => ()))(s, f)
  }

  override def putAll(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync putAll")
    Try {
      wrapped.putAll(map.asJava)
    }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAllAsync(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async putAll")
    withCompletion(wrapped.putAllAsync(map.asJava).asScala.map(_ => ()))(s, f)
  }

  override def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync get")
    Try {
      wrapped.get(key)
    }
      .fold(
        f,
        v => s(Map((key, v)))
      )
  }

  override def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async get")
    withCompletion(wrapped.getAsync(key).asScala.map(v => Map((key, v))))(s, f)
  }

  override def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAndRemove")
    Try {
      wrapped.getAndRemove(key)
    }
      .fold(
        f,
        v => s(Map((key, v)))
      )
  }

  override def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAndRemove")
    withCompletion(wrapped.getAndRemoveAsync(key).asScala.map(v => Map((key, v))))(s, f)
  }

  override def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAndPut")
    Try {
      wrapped.getAndPut(key, value)
    }
      .fold(
        f,
        v => s(Map((key, v)))
      )
  }

  override def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAndPut")
    withCompletion(wrapped.getAndPutAsync(key, value).asScala.map(v => Map((key, v))))(s, f)
  }

  override def getAll(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAll")
    Try {
      wrapped.getAll(keys.asJava)
    }
      .map(_.asScala.toMap)
      .fold(f, s)
  }

  override def getAllAsync(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAll")
    withCompletion(wrapped.getAllAsync(keys.asJava).asScala.map(v => v.asScala.toMap))(s, f)
  }

  override def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync remove")
    Try {
      wrapped.remove(key)
    }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async remove")
    withCompletion(wrapped.removeAsync(key).asScala.map(_ => ()))(s, f)
  }

  override def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync removeAll")
    Try {
      wrapped.removeAll(keys.asJava)
    }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async removeAll")
    withCompletion(wrapped.removeAllAsync(keys.asJava).asScala.map(_ => ()))(s, f)
  }

  override def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                        (s: Map[K, T] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invoke is not supported in thin client API")

  override def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                             (s: Map[K, T] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invokeAsync is not supported in thin client API")

  override def invokeAll[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)
                           (s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invokeAll is not supported in thin client API")

  override def invokeAllAsync[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)
                                (s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invokeAllAsync is not supported in thin client API")

  override def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("lock is not supported in thin client API")

  override def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("unlock is not supported in thin client API")

  override def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync sql")
    Try {
      wrapped.query(query)
        .getAll.asScala.toList
        .map(_.asScala.toList)
    }.fold(f, s)
  }

  override def withKeepBinary(): CacheApi[K, V] = copy(wrapped = wrapped.withKeepBinary())
}
