package org.apache.ignite.gatling.api.node

import org.apache.ignite.Ignite
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.{CacheApi, CompletionSupport, IgniteApi, TransactionApi}
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration

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

  override def wrapped[API]: API = wrapped.asInstanceOf[API]
}
