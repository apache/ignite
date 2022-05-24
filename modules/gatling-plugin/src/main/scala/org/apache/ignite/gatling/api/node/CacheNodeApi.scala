package org.apache.ignite.gatling.api.node

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.IgniteCache
import org.apache.ignite.gatling.api.CacheApi

import scala.concurrent.ExecutionContext
import scala.util.Try

case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V])(implicit val ec: ExecutionContext)
  extends CacheApi[K, V] with StrictLogging {

  override def put[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync put")
    Try { wrapped.put(key, value) }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAsync[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("async put")
    wrapped.putAsync(key, value)
      .listen(future =>
        Try { future.get() }
          .map(_ => ())
          .fold(f, s)
      )
  }

  override def getAsync[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("async get")
    wrapped.getAsync(key)
      .listen(future =>
        Try { future.get() }.fold(
          f,
          value => s(Map((key, value)))
        )
      )
  }

  override def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("sync get")
    Try { wrapped.get(key) }.fold(
      f,
      value => s(Map((key, value)))
    )
  }
}
