package org.apache.ignite.gatling.api.thin

import org.apache.ignite.client.{ClientCacheConfiguration, IgniteClient}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.{CompletionSupport, CacheApi, IgniteApi}
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

case class IgniteThinApi(wrapped: IgniteClient)(implicit val ec: ExecutionContext) extends IgniteApi with CompletionSupport {
  override def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](name).asScala.map(CacheThinApi(_)))(s, f)

  override def getOrCreateCache[K, V, U](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](cfg).asScala.map(CacheThinApi(_)))(s, f)

  override def getOrCreateCache[K, V, U](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    throw new NotImplementedError("Node client cache configuration was used to create cache via thin client API")

  override def getOrCreateCache[K, V, U](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    getOrCreateCache(cacheConfiguration(name, cfg))(s, f)

  override def close[U]()(s: Unit => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.close()))(s, f)

  private def cacheConfiguration(name: String, simpleCacheConfiguration: SimpleCacheConfiguration): ClientCacheConfiguration =
    new ClientCacheConfiguration()
      .setName(name)
      .setCacheMode(simpleCacheConfiguration.mode)
      .setAtomicityMode(simpleCacheConfiguration.atomicity)
      .setBackups(simpleCacheConfiguration.backups)
}
