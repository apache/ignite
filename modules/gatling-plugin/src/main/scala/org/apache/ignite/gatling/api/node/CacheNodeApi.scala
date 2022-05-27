package org.apache.ignite.gatling.api.node

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.api.CacheApi

import java.util.concurrent.locks.Lock
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
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

  override def putAll[U](map: Map[K, V])(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync putAll")
    Try { wrapped.putAll(map.asJava) }
      .map(_ => ())
      .fold(f, s)
  }

  override def putAllAsync[U](map: Map[K, V])(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("async putAll")
    wrapped.putAllAsync(map.asJava)
      .listen(future =>
        Try { future.get() }
          .map(_ => ())
          .fold(f, s)
      )
  }

  override def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("sync get")
    Try { wrapped.get(key) }
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  override def getAsync[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("async get")
    wrapped.getAsync(key)
      .listen(future =>
        Try { future.get() }
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  override def getAll[U](keys: Set[K])(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("sync getAll")
    Try { wrapped.getAll(keys.asJava) }
      .map(_.asScala.toMap)
      .fold(f, s)
  }

  override def getAllAsync[U](keys: Set[K])(s: Map[K, V] => U, f: Throwable => U): Unit = {
    logger.debug("async getAll")
    wrapped.getAllAsync(keys.asJava)
      .listen(future =>
        Try { future.get() }
          .map(_.asScala.toMap)
          .fold(f, s)
      )
  }

  override def remove[U](key: K)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync remove")
    Try { wrapped.remove(key) }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAsync[U](key: K)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("async remove")
    wrapped.removeAsync(key)
      .listen(future =>
        Try { future.get() }
          .map(_ => ())
          .fold(f, s)
      )
  }

  override def removeAll[U](keys: Set[K])(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync removeAll")
    Try { wrapped.removeAll(keys.asJava) }
      .map(_ => ())
      .fold(f, s)
  }

  override def removeAllAsync[U](keys: Set[K])(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("async removeAll")
    wrapped.removeAllAsync(keys.asJava)
      .listen(future =>
        Try { future.get() }
          .map(_ => ())
          .fold(f, s)
      )
  }

  override def invoke[T, U](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                           (s: Map[K, T] => U, f: Throwable => U): Unit = {
    logger.debug("sync invoke")
    Try { wrapped.invoke[T](key, entryProcessor, arguments) }
      .map(value => Map((key, value)))
      .fold(f, s)
  }

  override def invokeAsync[T, U](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                                (s: Map[K, T] => U, f: Throwable => U): Unit = {
    logger.debug("async invoke")
    wrapped.invokeAsync(key, entryProcessor, arguments)
      .listen(future =>
        Try { future.get() }
          .map(value => Map((key, value)))
          .fold(f, s)
      )
  }

  override def lock[U](key: K)(s: Lock => U, f: Throwable => U): Unit = {
    logger.debug("sync lock")
    Try {
      val lock = wrapped.lock(key)
      lock.lock()
      lock
    }.fold(f, s)
  }

  override def unlock[U](lock: Lock)(s: Unit => U, f: Throwable => U): Unit = {
    logger.debug("sync unlock")
    Try {
      lock.unlock()
    }.fold(f, s)
  }
}
