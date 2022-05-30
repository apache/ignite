package org.apache.ignite.gatling.api.thin

import org.apache.ignite.client.{ClientCacheConfiguration, IgniteClient}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.{CacheApi, CompletionSupport, IgniteApi, TransactionApi}
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

case class IgniteThinApi(wrapped: IgniteClient)(implicit val ec: ExecutionContext) extends IgniteApi with CompletionSupport {

  override def cache[K, V](name: String): Try[CacheApi[K, V]] =
    Try{ wrapped.cache[K, V](name) }
      .map(CacheThinApi(_))

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
    Try { wrapped.transactions().txStart() }.fold(
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
}

