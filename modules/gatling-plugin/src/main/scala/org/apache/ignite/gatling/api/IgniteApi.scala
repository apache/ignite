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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import io.gatling.commons.validation.{ Failure => ValidationFailure }
import io.gatling.commons.validation.{ Success => ValidationSuccess }
import io.gatling.commons.validation.Validation
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.gatling.api.thin.IgniteThinApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

trait IgniteApi {
  def cache[K, V](name: String): Try[CacheApi[K, V]]

  def cacheV[K, V](name: String): Validation[CacheApi[K, V]] =
    cache[K, V](name).fold(ex => ValidationFailure(ex.getMessage), ValidationSuccess(_))

  def getOrCreateCache[K, V](name: String)(s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheByClientConfiguration[K, V](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => Unit, f: Throwable => Unit = _ => ()): Unit

  def close()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStart()(s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStartEx(concurrency: TransactionConcurrency, isolation: TransactionIsolation)
               (s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

  def txStartEx2(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)
                (s: TransactionApi => Unit, f: Throwable => Unit = _ => ()): Unit

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

  def remove(key: K)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
               (s: Map[K, T] => Unit, f: Throwable => Unit = _ => ()): Unit

  def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                    (s: Map[K, T] => Unit, f: Throwable => Unit = _ => ()): Unit

  def lock(key: K)(s: Lock => Unit, f: Throwable => Unit = _ => ()): Unit

  def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit = _ => ()): Unit
}

trait TransactionApi {
  def commit()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit

  def rollback()(s: Unit => Unit, f: Throwable => Unit = _ => ()): Unit
}

object IgniteApi extends CompletionSupport {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def apply[U](protocol: IgniteProtocol)(s: IgniteApi => U, f: Throwable => U): Unit = {
    protocol.cfg match {
      case Left(clientConfiguration) =>
        withCompletion(Future(Ignition.startClient(clientConfiguration)).map(IgniteThinApi(_)))(s, f)
      case Right(ignite) =>
        withCompletion(Future(ignite).map(IgniteNodeApi(_)))(s, f)
    }
  }
}

trait CompletionSupport {
  implicit val ec: ExecutionContext

  def withCompletion[T, U](fut: Future[T])(s: T => U, f: Throwable => U): Unit = fut.onComplete {
    case Success(value) => s(value)
    case Failure(exception) => f(exception)
  }
}
