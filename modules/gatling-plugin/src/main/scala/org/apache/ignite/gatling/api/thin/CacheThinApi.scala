package org.apache.ignite.gatling.api.thin

import org.apache.ignite.client.ClientCache
import org.apache.ignite.gatling.api.{CompletionSupport, CacheApi}

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.CompletionStageOps

case class CacheThinApi[K, V](wrapped: ClientCache[K, V])(implicit val ec: ExecutionContext) extends CacheApi[K, V] with CompletionSupport {
  override def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit =
    withCompletion(wrapped.putAsync(key, value).asScala)(s, f)
}
