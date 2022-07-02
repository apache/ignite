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

import scala.util.Try

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.Validation
import io.gatling.core.session.Session
import org.apache.ignite.Ignition
import org.apache.ignite.binary.BinaryObjectBuilder
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
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * Wrapper around the Ignite API abstracting access either via the Ignite Node (thick) API or via the
 * Ignite Client (thin) API.
 */
trait IgniteApi {
  /**
   * Gets an instance of cache API by name.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param name Cache name.
   * @return Instance of the CacheAPI.
   */
  def cache[K, V](name: String): Validation[CacheApi[K, V]]

  /**
   * Gets existing cache or creates new one with the given name and the simplified cache configuration
   * which may be defined via the Ignite Gatling DSL.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param name Cache name.
   * @param cfg Simplified configuration of the cache (see the SimpleCacheConfiguration).
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(
    s: CacheApi[K, V] => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Gets existing cache or creates new one with the provided Ignite (thin) Client cache configuration.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cfg Instance of Ignite Client cache configuration.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getOrCreateCacheByClientConfiguration[K, V](
    cfg: ClientCacheConfiguration
  )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Gets existing cache or creates new one with the provided Ignite (thick) cache configuration.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cfg Instance of Ignite cache configuration.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def getOrCreateCacheByConfiguration[K, V](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit

  /**
   * Closes the Ignite API.
   *
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def close()(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Starts the transaction with the default parameters.
   *
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit

  /**
   * Starts the transaction with the provided concurrency and isolation.
   *
   * @param concurrency Concurrency.
   * @param isolation Isolation.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation)(
    s: TransactionApi => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Starts the transaction with the provided concurrency, isolation timeout and transaction size.
   *
   * @param concurrency Concurrency.
   * @param isolation Isolation.
   * @param timeout Timeout.
   * @param txSize Number of entries participating in transaction (may be approximate).
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, txSize: Int)(
    s: TransactionApi => Unit,
    f: Throwable => Unit
  ): Unit

  /**
   * Creates instance of BinaryObjectBuilder.
   *
   * @return Instance of BinaryObjectBuilder.
   */
  def binaryObjectBuilder: String => BinaryObjectBuilder
}

/**
 * Factory for IgniteApi instances.
 */
object IgniteApi extends StrictLogging {
  /**
   * Starts new instance of IgniteApi.
   *
   * Starts new Ignite node or thin client instances if protocol was configured via configuration objects.
   * Do nothing and returns existing instances from session otherwise.
   *
   * @param protocol Gatling Ignite protocol.
   * @param session Gatling session.
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def start(protocol: IgniteProtocol, session: Session)(s: IgniteApi => Unit, f: Throwable => Unit): Unit =
    protocol.cfg match {
      case IgniteClientConfigurationCfg(cfg) if protocol.manualClientStart => Try(startClient(cfg)).fold(f, s)

      case IgniteConfigurationCfg(cfg) if protocol.manualClientStart => Try(startNode(cfg)).fold(f, s)

      case _ => s(session(IgniteApiSessionKey).as[IgniteApi])
    }

  /**
   * Creates default instance of IgniteApi on simulation start depending on the protocol passed.
   *
   * If protocol contains configuration objects it starts either ignite node
   * in client mode or thin ignite client.
   *
   * If protocol contains started instances of node or client just returns it.
   *
   * @param protocol Gatling Ignite protocol
   * @return Instance of Ignite API
   */
  def apply(protocol: IgniteProtocol): Option[IgniteApi] =
    protocol.cfg match {
      case IgniteClientConfigurationCfg(cfg) if !protocol.manualClientStart => Some(startClient(cfg))

      case IgniteConfigurationCfg(cfg) if !protocol.manualClientStart => Some(startNode(cfg))

      case IgniteNodeCfg(node) =>
        node.cacheNames.forEach(name => node.cache(name))
        Some(IgniteNodeApi(node))

      case IgniteClientCfg(igniteClient) => Some(IgniteThinApi(igniteClient))

      case _ => None
    }

  /**
   * Starts Ignite node in client mode.
   *
   * @param cfg Node configuration.
   * @return Instance of IgniteNodeApi.
   */
  private def startNode(cfg: IgniteConfiguration): IgniteNodeApi = {
    val node = Ignition.start(cfg)
    node.cacheNames.forEach(name => node.cache(name))
    IgniteNodeApi(node)
  }

  /**
   * Starts Ignite (thin) client.
   *
   * @param cfg Client configuration.
   * @return Instance of IgniteThinApi.
   */
  private def startClient(cfg: ClientConfiguration): IgniteThinApi = IgniteThinApi(Ignition.startClient(cfg))
}
