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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

import io.gatling.commons.validation.{Failure => ValidationFailure}
import io.gatling.commons.validation.{Success => ValidationSuccess}
import io.gatling.commons.validation.Validation
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.CompletionSupport
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

case class IgniteThinApi(wrapped: IgniteClient)(implicit val ec: ExecutionContext) extends IgniteApi with CompletionSupport {
  override def cache[K, V](name: String): Validation[CacheApi[K, V]] =
    Try(wrapped.cache[K, V](name))
      .map(CacheThinApi(_))
      .fold(ex => ValidationFailure(ex.getMessage), ValidationSuccess(_))

  override def getOrCreateCache[K, V](name: String)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](name).asScala.map(CacheThinApi(_)))(s, f)

  override def getOrCreateCacheByClientConfiguration[K, V](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](cfg).asScala.map(CacheThinApi(_)))(s, f)

  override def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("Node client cache configuration was used to create cache via thin client API")

  override def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    getOrCreateCacheByClientConfiguration(cacheConfiguration(name, cfg))(s, f)

  override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
    withCompletion(Future(wrapped.close()))(s, f)

  override def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try {
      wrapped.transactions().txStart()
    }.fold(
      f,
      tx => s(TransactionThinApi(tx))
    )

  override def txStartEx(concurrency: TransactionConcurrency, isolation: TransactionIsolation)
                        (s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try {
      wrapped.transactions().txStart(concurrency, isolation)
    }.fold(
      f,
      tx => s(TransactionThinApi(tx))
    )

  override def txStartEx2(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)
                         (s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try {
      wrapped.transactions().txStart(concurrency, isolation, timeout)
    }.fold(
      f,
      tx => s(TransactionThinApi(tx))
    )

  override def wrapped[API]: API = wrapped.asInstanceOf[API]

  private def cacheConfiguration(name: String, simpleCacheConfiguration: SimpleCacheConfiguration): ClientCacheConfiguration =
    new ClientCacheConfiguration()
      .setName(name)
      .setCacheMode(simpleCacheConfiguration.mode)
      .setAtomicityMode(simpleCacheConfiguration.atomicity)
      .setBackups(simpleCacheConfiguration.backups)

  override def binaryObjectBuilder: String => BinaryObjectBuilder = typeName =>
    wrapped.binary().builder(typeName)
}
