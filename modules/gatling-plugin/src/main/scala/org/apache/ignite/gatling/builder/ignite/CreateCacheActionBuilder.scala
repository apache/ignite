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

case class CreateCacheActionBuilderBase[K, V](cacheName: Expression[String], requestName: Expression[String] = EmptyStringExpressionSuccess)
    extends ActionBuilder {
  def backups(newValue: Integer): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(backups = newValue)

  def atomicity(newValue: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(atomicity = newValue)

  def mode(newValue: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(mode = newValue)

  def cfg(clientCacheCfg: ClientCacheConfiguration): CreateCacheActionBuilder[K, V] =
    CreateCacheActionBuilder(requestName, cacheName, Configuration(clientCacheCfg = Some(clientCacheCfg)))

  def cfg(cacheCfg: CacheConfiguration[K, V]): CreateCacheActionBuilder[K, V] =
    CreateCacheActionBuilder(requestName, cacheName, Configuration(cacheCfg = Some(cacheCfg)))

  override def build(ctx: ScenarioContext, next: Action): Action =
    new CreateCacheAction(requestName, cacheName, Configuration(), next, ctx)

  def as(name: Expression[String]): ActionBuilder = this.copy(requestName = name)
}

case class CreateCacheActionBuilderSimpleConfigStep(
  requestName: Expression[String],
  cacheName: Expression[String],
  backups: Integer = 0,
  atomicity: CacheAtomicityMode = CacheAtomicityMode.ATOMIC,
  mode: CacheMode = CacheMode.PARTITIONED
) extends ActionBuilder {
  def backups(newValue: Integer): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(backups = newValue)

  def atomicity(newValue: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(atomicity = newValue)

  def mode(newValue: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(mode = newValue)

  def createCacheActionBuilder[K, V]: CreateCacheActionBuilder[K, V] =
    CreateCacheActionBuilder[K, V](
      requestName,
      cacheName,
      Configuration(simpleCfg = Some(SimpleCacheConfiguration(backups, atomicity, mode)))
    )

  override def build(ctx: ScenarioContext, next: Action): Action =
    createCacheActionBuilder.build(ctx, next)

  def as(name: Expression[String]): ActionBuilder = this.copy(requestName = name)
}

case class SimpleCacheConfiguration(backups: Integer, atomicity: CacheAtomicityMode, mode: CacheMode)

case class Configuration[K, V](
  clientCacheCfg: Option[ClientCacheConfiguration] = None,
  cacheCfg: Option[CacheConfiguration[K, V]] = None,
  simpleCfg: Option[SimpleCacheConfiguration] = None
)

case class CreateCacheActionBuilder[K, V](requestName: Expression[String], cacheName: Expression[String], config: Configuration[K, V])
    extends ActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CreateCacheAction(requestName, cacheName, config, next, ctx)

  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)
}
