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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.Validation
import io.gatling.core.session.Session
import org.apache.ignite.Ignition
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.gatling.api.thin.IgniteThinApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.gatling.protocol.IgniteClientCfg
import org.apache.ignite.gatling.protocol.IgniteClientConfigurationCfg
import org.apache.ignite.gatling.protocol.IgniteConfigurationCfg
import org.apache.ignite.gatling.protocol.IgniteNodeCfg
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.IGNITE_API_SESSION_KEY
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

trait IgniteApi {
  def cache[K, V](name: String): Validation[CacheApi[K, V]]

  def getOrCreateCache[K, V](name: String)(s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)
                                          (s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheByClientConfiguration[K, V](cfg: ClientCacheConfiguration)
                                                 (s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])
                                           (s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def close()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStart()(s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStartEx(concurrency: TransactionConcurrency, isolation: TransactionIsolation)
               (s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStartEx2(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)
                (s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

  def binaryObjectBuilder: String => BinaryObjectBuilder

  def wrapped[API]: API
}

//noinspection AccessorLikeMethodIsUnit
trait CacheApi[K, V] {
  def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def putAll(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def putAllAsync(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAll(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAllAsync(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def remove(key: K)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
               (s: Map[K, T] => Unit, f: Throwable => Unit = _ => ()): Unit

  def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                    (s: Map[K, T] => Unit, f: Throwable => Unit = _ => ()): Unit

  def invokeAll[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)
               (s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit = _ => ()): Unit

  def invokeAllAsync[T](map: Map[K, CacheEntryProcessor[K, V, T]], arguments: Any*)
                    (s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit = _ => ()): Unit

  def lock(key: K)(s: Lock => Unit, f: Throwable => Unit = _ => ()): Unit

  def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit = _ => ()): Unit

  def withKeepBinary(): CacheApi[K, V]
}

trait TransactionApi {
  def commit()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def rollback()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit
}

object IgniteApi extends CompletionSupport with StrictLogging {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def manualStart[U](protocol: IgniteProtocol, session: Session)(s: IgniteApi => U, f: Throwable => U): Unit =
    protocol.cfg match {
      case IgniteClientConfigurationCfg(cfg) if protocol.manualClientStart => Try(startClient(cfg)).fold(f, s)

      case IgniteConfigurationCfg(cfg) if protocol.manualClientStart => Try(startNode(cfg)).fold(f, s)

      case _ => s(session(IGNITE_API_SESSION_KEY).as[IgniteApi])
    }

  def autoStart(protocol: IgniteProtocol): Option[IgniteApi] =
    protocol.cfg match {
      case IgniteClientConfigurationCfg(cfg) if !protocol.manualClientStart => Some(startClient(cfg))

      case IgniteConfigurationCfg(cfg) if !protocol.manualClientStart => Some(startNode(cfg))

      case IgniteNodeCfg(node) =>
        node.cacheNames.forEach(name => node.cache(name))
        Some(IgniteNodeApi(node))

      case IgniteClientCfg(igniteClient) => Some(IgniteThinApi(igniteClient))

      case _ => None
    }

  private def startNode(cfg: IgniteConfiguration): IgniteNodeApi = {
    val node = Ignition.start(cfg)
    node.cacheNames.forEach(name => node.cache(name))
    IgniteNodeApi(node)
  }

  private def startClient(cfg: ClientConfiguration): IgniteThinApi = IgniteThinApi(Ignition.startClient(cfg))
}

trait CompletionSupport {
  implicit val ec: ExecutionContext

  def withCompletion[T, U](fut: Future[T])(s: T => U, f: Throwable => U): Unit = fut.onComplete {
    case Success(value) => s(value)
    case Failure(exception) => f(exception)
  }
}
