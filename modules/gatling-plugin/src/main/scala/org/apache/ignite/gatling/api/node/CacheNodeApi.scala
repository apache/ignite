package org.apache.ignite.gatling.api.node

import org.apache.ignite.IgniteCache
import org.apache.ignite.gatling.api.{CompletionSupport, CacheApi}

import scala.concurrent.{ExecutionContext, Future}

case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V])(implicit val ec: ExecutionContext) extends CacheApi[K, V] with CompletionSupport {
  override def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.putAsync(key, value).get()))(s, f)

  override def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.getAsync(key).get()).map(v => Map((key, v))))(s, f)
}
