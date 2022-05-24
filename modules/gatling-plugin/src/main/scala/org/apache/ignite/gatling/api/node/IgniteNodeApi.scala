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

  override def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.getOrCreateCache[K, V](name)).map(CacheNodeApi(_)))(s, f)

  override def getOrCreateCache[K, V, U](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    throw new NotImplementedError("Thin client cache configuration was used to create cache via node API")

  override def getOrCreateCache[K, V, U](cfg: CacheConfiguration[K, V])(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.getOrCreateCache(cfg)).map(CacheNodeApi(_)))(s, f)

  override def getOrCreateCache[K, V, U](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    getOrCreateCache(cacheConfiguration[K, V](name, cfg))(s, f)

  override def close[U]()(s: Unit => U, f: Throwable => U): Unit =
    withCompletion(Future.successful(()))(s, f)

  private def cacheConfiguration[K, V](name: String, simpleCacheConfiguration: SimpleCacheConfiguration): CacheConfiguration[K, V] =
    new CacheConfiguration()
      .setName(name)
      .setCacheMode(simpleCacheConfiguration.mode)
      .setAtomicityMode(simpleCacheConfiguration.atomicity)
      .setBackups(simpleCacheConfiguration.backups)

  override def txStart[U]()(s: TransactionApi => U, f: Throwable => U): Unit = {
    Try {
      wrapped.transactions().txStart()
    }.fold(
      f,
      tx => TransactionNodeApi(tx)
    )
  }
}
