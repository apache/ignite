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
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * Implementation of IgniteApi working via the Ignite (thin) Client API.
 *
 * @param wrapped Enclosed IgniteClient instance.
 */
case class IgniteThinApi(wrapped: IgniteClient) extends IgniteApi with CompletionSupport {
  /**
   * @inheritdoc
   * @tparam K @inheritdoc
   * @tparam V @inheritdoc
   * @param name @inheritdoc
   * @return @inheritdoc
   */
  override def cache[K, V](name: String): Validation[CacheApi[K, V]] =
    Try(wrapped.cache[K, V](name))
      .map(CacheThinApi[K, V])
      .fold(ex => ValidationFailure(ex.getMessage), ValidationSuccess(_))

  /**
   * @inheritdoc
   * @tparam K @inheritdoc
   * @tparam V @inheritdoc
   * @param cfg @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getOrCreateCacheByClientConfiguration[K, V](
    cfg: ClientCacheConfiguration
  )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](cfg).asScala.map(CacheThinApi[K, V]))(s, f)

  /**
   * @inheritdoc
   * @tparam K @inheritdoc
   * @tparam V @inheritdoc
   * @param cfg @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
    f(new NotImplementedError("Node client cache configuration was used to create cache via thin client API"))

  /**
   * @inheritdoc
   * @tparam K @inheritdoc
   * @tparam V @inheritdoc
   * @param name @inheritdoc
   * @param cfg @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(
    s: CacheApi[K, V] => Unit,
    f: Throwable => Unit
  ): Unit =
    getOrCreateCacheByClientConfiguration(
      new ClientCacheConfiguration()
        .setName(name)
        .setCacheMode(cfg.cacheMode)
        .setAtomicityMode(cfg.atomicity)
        .setBackups(cfg.backups)
    )(s, f)

  /**
   * @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.close()).fold(f, s)

  /**
   * @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.transactions().txStart())
      .map(TransactionThinApi)
      .fold(f, s)

  /**
   * @inheritdoc
   * @param concurrency @inheritdoc
   * @param isolation @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def txStart(
    concurrency: TransactionConcurrency,
    isolation: TransactionIsolation
  )(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.transactions().txStart(concurrency, isolation))
      .map(TransactionThinApi)
      .fold(f, s)

  /**
   * @inheritdoc
   * @param concurrency @inheritdoc
   * @param isolation @inheritdoc
   * @param timeout @inheritdoc
   * @param txSize @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)(
    s: TransactionApi => Unit,
    f: Throwable => Unit
  ): Unit =
    Try(wrapped.transactions().txStart(concurrency, isolation, timeout))
      .map(TransactionThinApi)
      .fold(f, s)

  /**
   * @inheritdoc
   * @return @inheritdoc
   */
  override def binaryObjectBuilder: String => BinaryObjectBuilder = typeName => wrapped.binary().builder(typeName)
}
