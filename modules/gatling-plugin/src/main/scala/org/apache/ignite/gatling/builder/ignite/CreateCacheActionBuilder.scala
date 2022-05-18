package org.apache.ignite.gatling.builder.ignite

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.{CacheAtomicityMode, CacheMode}
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.action.ignite
import org.apache.ignite.gatling.action.ignite.CreateCacheAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class  CreateCacheActionBuilderBase[K, V](
                                     requestName: Expression[String],
                                     cacheName: Expression[String]) extends IgniteActionBuilder {
  def backups(newValue: Integer): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(backups = newValue)

  def atomicity(newValue: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(atomicity = newValue)

  def mode(newValue: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    CreateCacheActionBuilderSimpleConfigStep(requestName, cacheName).copy(mode = newValue)

  def cfg(clientCacheCfg: ClientCacheConfiguration): CreateCacheActionBuilder[K, V] =
    CreateCacheActionBuilder(requestName, cacheName, Configuration(clientCacheCfg=clientCacheCfg))

  def cfg(cacheCfg: CacheConfiguration[K, V]): CreateCacheActionBuilder[K, V] =
    CreateCacheActionBuilder(requestName, cacheName, Configuration(cacheCfg=cacheCfg))

  override def build(ctx: ScenarioContext, next: Action): Action =
    ignite.CreateCacheAction(requestName, cacheName, Configuration(), next, ctx)
}

case class CreateCacheActionBuilderSimpleConfigStep(requestName: Expression[String],
                                                    cacheName: Expression[String],
                                                    backups: Integer = 0,
                                                    atomicity: CacheAtomicityMode = CacheAtomicityMode.ATOMIC,
                                                    mode: CacheMode = CacheMode.PARTITIONED) extends IgniteActionBuilder {
  def backups(newValue: Integer): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(backups = newValue)

  def atomicity(newValue: CacheAtomicityMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(atomicity = newValue)

  def mode(newValue: CacheMode): CreateCacheActionBuilderSimpleConfigStep =
    this.copy(mode = newValue)

  def createCacheActionBuilder[K,V]: CreateCacheActionBuilder[K,V] =
    CreateCacheActionBuilder[K,V](requestName, cacheName, Configuration(simpleCfg=SimpleCacheConfiguration(backups, atomicity, mode)))

  override def build(ctx: ScenarioContext, next: Action): Action =
    createCacheActionBuilder.build(ctx, next)
}

case class SimpleCacheConfiguration(backups: Integer, atomicity: CacheAtomicityMode, mode: CacheMode)

case class Configuration[K,V](clientCacheCfg: ClientCacheConfiguration = null,
                              cacheCfg: CacheConfiguration[K, V] = null,
                              simpleCfg: SimpleCacheConfiguration = null)

case class CreateCacheActionBuilder[K,V](requestName: Expression[String],
                                         cacheName: Expression[String],
                                         config: Configuration[K,V]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    ignite.CreateCacheAction(requestName, cacheName, config, next, ctx)
}