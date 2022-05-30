package org.apache.ignite.gatling.api.thin

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.client.ClientCache
import org.apache.ignite.gatling.api.{CacheApi, CompletionSupport}

import java.util.concurrent.locks.Lock
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.CompletionStageOps
import scala.jdk.CollectionConverters._
import scala.util.Try

case class CacheThinApi[K, V](wrapped: ClientCache[K, V])(implicit val ec: ExecutionContext)
  extends CacheApi[K, V] with CompletionSupport with StrictLogging {

  override def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync put")
    Try { wrapped.put(key, value) }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async put")
    withCompletion(wrapped.putAsync(key, value).asScala.map(_ => ()))(s, f)
  }

  override def putAll(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync putAll")
    Try { wrapped.putAll(map.asJava) }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAllAsync(map: Map[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async putAll")
    withCompletion(wrapped.putAllAsync(map.asJava).asScala.map(_ => ()))(s, f)
  }

  override def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync get")
    Try { wrapped.get(key) }
      .fold(
        f,
        v => s(Map((key, v)))
      )
  }

  override def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async get")
    withCompletion(wrapped.getAsync(key).asScala.map(v => Map((key, v))))(s, f)
  }

  override def getAll(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync getAll")
    Try { wrapped.getAll(keys.asJava) }
      .map(_.asScala.toMap)
      .fold(f, s)
  }

  override def getAllAsync(keys: Set[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async getAll")
    withCompletion(wrapped.getAllAsync(keys.asJava).asScala.map(v => v.asScala.toMap))(s, f)
  }

  override def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync remove")
    Try { wrapped.remove(key) }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async remove")
    withCompletion(wrapped.removeAsync(key).asScala.map(_ => ()))(s, f)
  }

  override def removeAll(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("sync removeAll")
    Try { wrapped.removeAll(keys.asJava) }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAllAsync(keys: Set[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
    logger.debug("async removeAll")
    withCompletion(wrapped.removeAllAsync(keys.asJava).asScala.map(_ => ()))(s, f)
  }

  override def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                           (s: Map[K, T] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invoke is not supported in thin client API")

  override def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                                (s: Map[K, T] => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("invokeAsync is not supported in thin client API")

  override def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("lock is not supported in thin client API")

  override def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit =
    throw new NotImplementedError("unlock is not supported in thin client API")
}
