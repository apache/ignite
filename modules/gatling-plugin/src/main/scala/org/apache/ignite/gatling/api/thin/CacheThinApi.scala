package org.apache.ignite.gatling.api.thin

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.client.ClientCache
import org.apache.ignite.gatling.api.{CacheApi, CompletionSupport}

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

case class CacheThinApi[K, V](wrapped: ClientCache[K, V])(implicit val ec: ExecutionContext)
  extends CacheApi[K, V] with CompletionSupport with StrictLogging {

  override def put[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync put")
    Try { wrapped.put(key, value) }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAsync[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("async put")
    withCompletion(wrapped.putAsync(key, value).asScala.map(_ => ()))(s, f)
  }

  override def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("sync get")
    Try { wrapped.get(key) }
      .fold(
        f,
        v => s(Map((key, v)))
      )
  }

  override def getAsync[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("async get")
    withCompletion(wrapped.getAsync(key).asScala.map(v => Map((key, v))))(s, f)
  }
}
