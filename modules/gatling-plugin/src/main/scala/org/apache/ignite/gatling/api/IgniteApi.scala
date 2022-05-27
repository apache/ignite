package org.apache.ignite.gatling.api

import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.gatling.api.thin.IgniteThinApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.gatling.protocol.IgniteProtocol

import java.util.concurrent.locks.Lock
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait IgniteApi {
  def cache[K, V](name: String): Try[CacheApi[K, V]]

  def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit
  def getOrCreateCache[K, V, U](name: String, cfg: SimpleCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit
  def getOrCreateCache[K, V, U](cfg: ClientCacheConfiguration)(s: CacheApi[K, V] => U, f: Throwable => U): Unit
  def getOrCreateCache[K, V, U](cfg: CacheConfiguration[K,V])(s: CacheApi[K, V] => U, f: Throwable => U): Unit

  def close[U]()(s: Unit => U, f: Throwable => U): Unit

  def txStart[U]()(s: TransactionApi => U, f: Throwable => U): Unit
}

//noinspection AccessorLikeMethodIsUnit
trait CacheApi[K, V] {
  def put[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit
  def putAsync[U](key: K, value: V)(s: Unit => U, f: Throwable => U): Unit

  def putAll[U](map: Map[K, V])(s: Unit => U, f: Throwable => U): Unit
  def putAllAsync[U](map: Map[K, V])(s: Unit => U, f: Throwable => U): Unit

  def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit
  def getAsync[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit

  def getAll[U](keys: Set[K])(s: Map[K, V] => U, f: Throwable => U): Unit
  def getAllAsync[U](keys: Set[K])(s: Map[K, V] => U, f: Throwable => U): Unit

  def remove[U](key: K)(s: Unit => U, f: Throwable => U): Unit
  def removeAsync[U](key: K)(s: Unit => U, f: Throwable => U): Unit

  def removeAll[U](keys: Set[K])(s: Unit => U, f: Throwable => U): Unit
  def removeAllAsync[U](keys: Set[K])(s: Unit => U, f: Throwable => U): Unit

  def invoke[T, U](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                  (s: Map[K, T] => U, f: Throwable => U): Unit
  def invokeAsync[T, U](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)
                       (s: Map[K, T] => U, f: Throwable => U): Unit

  def lock[U](key: K)(s: Lock => U, f: Throwable => U): Unit
  def unlock[U](lock: Lock)(s: Unit => U, f: Throwable => U): Unit
}

trait TransactionApi {
  def commit[U]()(s: Unit => U, f: Throwable => U): Unit
  def rollback[U]()(s: Unit => U, f: Throwable => U): Unit
}

object IgniteApi extends CompletionSupport {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def apply[U](protocol: IgniteProtocol)(s: IgniteApi => U, f: Throwable => U): Unit = {
    protocol.cfg match {
      case Left(clientConfiguration) =>
        withCompletion(Future(Ignition.startClient(clientConfiguration)).map(IgniteThinApi(_)))(s, f)
      case Right(ignite) =>
        withCompletion(Future(ignite).map(IgniteNodeApi(_)))(s, f)
    }
  }
}

trait CompletionSupport {
  implicit val ec: ExecutionContext

  def withCompletion[T, U](fut: Future[T])(s: T => U, f: Throwable => U): Unit = fut.onComplete {
    case Success(value)     => s(value)
    case Failure(exception) => f(exception)
  }
}
