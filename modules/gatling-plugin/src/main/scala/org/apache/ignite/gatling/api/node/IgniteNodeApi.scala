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

import org.apache.ignite.Ignite
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.{CacheApi, CompletionSupport, IgniteApi, TransactionApi}
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.transactions.{TransactionConcurrency, TransactionIsolation}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class IgniteNodeApi(wrapped: Ignite)(implicit val ec: ExecutionContext) extends IgniteApi with CompletionSupport {

  override def cache[K, V](name: String): Try[CacheApi[K, V]] =
    Try(wrapped.cache[K, V](name)).map(CacheNodeApi(_))

  override def getOrCreateCache[K, V](name: String)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    withCompletion(Future(wrapped.getOrCreateCache[K, V](name)).map(CacheNodeApi(_)))(s, f)

  override def getOrCreateCacheByClientConfiguration[K, V](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("Thin client cache configuration was used to create cache via node API")

  override def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    withCompletion(Future(wrapped.getOrCreateCache(cfg)).map(CacheNodeApi(_)))(s, f)

  override def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    getOrCreateCacheByConfiguration(cacheConfiguration[K, V](name, cfg))(s, f)

  override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
    withCompletion(Future.successful(()))(s, f)

  private def cacheConfiguration[K, V](name: String, simpleCacheConfiguration: SimpleCacheConfiguration): CacheConfiguration[K, V] =
    new CacheConfiguration()
      .setName(name)
      .setCacheMode(simpleCacheConfiguration.mode)
      .setAtomicityMode(simpleCacheConfiguration.atomicity)
      .setBackups(simpleCacheConfiguration.backups)

  override def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit = {
    Try {
      wrapped.transactions().txStart()
    }.fold(
      f,
      tx => TransactionNodeApi(tx)
    )
  }

  override def txStartEx(concurrency: TransactionConcurrency, isolation: TransactionIsolation)
                      (s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try { wrapped.transactions().txStart(concurrency, isolation) }.fold(
      f,
      tx => s(TransactionNodeApi(tx))
    )

  override def txStartEx2(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)
                      (s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try { wrapped.transactions().txStart(concurrency, isolation, timeout, txSize) }.fold(
      f,
      tx => s(TransactionNodeApi(tx))
    )

  override def wrapped[API]: API = wrapped.asInstanceOf[API]
}
