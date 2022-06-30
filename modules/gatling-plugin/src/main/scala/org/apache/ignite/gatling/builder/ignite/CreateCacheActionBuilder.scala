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
package org.apache.ignite.gatling.builder.ignite

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.action.ignite.CreateCacheAction

/**
 * Base create cache action builder.
 *
 * Works in two modes. The simplified one allows to specify three basic parameters via the DSL (backups, atomicity and mode).
 * Other way the full fledged instances of ClientCacheConfiguration or CacheConfiguration may be passed via the `cfg`
 * method.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 */
case class CreateCacheActionBuilderBase[K, V](cacheName: Expression[String]) extends ActionBuilder {
  /**
   * Specify number of backup copies.
   *
   * @param backups Number of backup copies.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def backups(backups: Integer): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(backups = backups))

  /**
   * Specify atomicity.
   *
   * @param atomicity Atomicity.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def atomicity(atomicity: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(atomicity = atomicity))

  /**
   * Specify cache partitioning mode.
   *
   * @param cacheMode Cache partitioning mode.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def mode(cacheMode: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(cacheMode = cacheMode))

  /**
   * Specify full cache configuration via the ClientCacheConfiguration instance
   * which is part of Ignite Client (thin) API.
   *
   * @param clientCacheCfg Client cache configuration.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def cfg(clientCacheCfg: ClientCacheConfiguration): CreateCacheActionBuilder[K, V] =
    new CreateCacheActionBuilder(cacheName, ThinConfiguration(clientCacheCfg))

  /**
   * Specify full cache configuration via the CacheConfiguration instance
   * which is part of Ignite Node (thick) API.
   *
   * @param cacheCfg Cache configuration.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def cfg(cacheCfg: CacheConfiguration[K, V]): CreateCacheActionBuilder[K, V] =
    new CreateCacheActionBuilder(cacheName, ThickConfiguration(cacheCfg))

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CreateCacheActionBuilder(cacheName, SimpleCacheConfiguration()).build(ctx, next)

  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): ActionBuilder =
    new CreateCacheActionBuilder(cacheName, SimpleCacheConfiguration()).as(requestName)
}

/**
 * Builder step to specify simple cache parameters via DSL.
 *
 * @param cacheName Cache name.
 * @param simpleConfig Simple cache configuration instance.
 */
case class CreateCacheActionBuilderSimpleConfigStep(
  cacheName: Expression[String],
  simpleConfig: SimpleCacheConfiguration
) extends ActionBuilder {
  /**
   * Specify number of backup copies.
   *
   * @param backups Number of backup copies.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def backups(backups: Integer): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(simpleConfig = simpleConfig.copy(backups = backups))

  /**
   * Specify atomicity.
   *
   * @param atomicity Atomicity.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def atomicity(atomicity: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(simpleConfig = simpleConfig.copy(atomicity = atomicity))

  /**
   * Specify cache partitioning mode.
   *
   * @param cacheMode Cache partitioning mode.
   * @return Build step to specify other cache parameters via the DSL.
   */
  def mode(cacheMode: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(simpleConfig = simpleConfig.copy(cacheMode = cacheMode))

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    createCacheActionBuilder.build(ctx, next)

  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): ActionBuilder =
    createCacheActionBuilder.as(requestName)

  private def createCacheActionBuilder[K, V]: CreateCacheActionBuilder[K, V] =
    new CreateCacheActionBuilder[K, V](
      cacheName,
      simpleConfig
    )
}

/**
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param config Cache configuration.
 */
class CreateCacheActionBuilder[K, V](cacheName: Expression[String], config: Configuration) extends ActionBuilder {
  /** Request name. */
  var requestName: Expression[String] = EmptyStringExpressionSuccess

  /**
   * Builds an action.
   *
   * @param ctx  The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CreateCacheAction(requestName, cacheName, config, next, ctx)

  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): CreateCacheActionBuilder[K, V] = {
    this.requestName = requestName
    this
  }
}

/**
 * Abstract cache configuration
 */
sealed trait Configuration

/**
 * Simplified cache configuration.
 *
 * @param backups Number of backup copies.
 * @param atomicity Atomicity.
 * @param cacheMode Cache partitioning mode.
 */
case class SimpleCacheConfiguration(
  backups: Integer = 0,
  atomicity: CacheAtomicityMode = CacheAtomicityMode.ATOMIC,
  cacheMode: CacheMode = CacheMode.PARTITIONED
) extends Configuration

/**
 * Ignite Client (thin) cache configuration.
 *
 * @param cfg ClientCacheConfiguration instance.
 */
case class ThinConfiguration(cfg: ClientCacheConfiguration) extends Configuration

/**
 * Ignite node (thick) cache configuration.
 *
 * @param cfg CacheConfiguration instance.
 */
case class ThickConfiguration(cfg: CacheConfiguration[_, _]) extends Configuration
