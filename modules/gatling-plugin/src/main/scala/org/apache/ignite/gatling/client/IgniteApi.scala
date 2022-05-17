package org.apache.ignite.gatling.client

import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.ignite.client.{ClientCache, IgniteClient}
import org.apache.ignite.gatling.protocol.IgniteProtocol

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

trait IgniteApi {
  def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit
  def close[U]()(s: Unit => U, f: Throwable => U): Unit
}

trait CacheApi[K, V] {
  def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit
}

case class IgniteThinApi(wrapped: IgniteClient)(implicit val ec: ExecutionContext) extends IgniteApi with AsyncSupport {
  override def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(wrapped.getOrCreateCacheAsync[K, V](name).asScala.map(CacheThinApi(_)))(s, f)

  override def close[U]()(s: Unit => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.close()))(s, f)
}

case class CacheThinApi[K, V](wrapped: ClientCache[K, V])(implicit val ec: ExecutionContext) extends CacheApi[K, V] with AsyncSupport {
  override def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit =
    withCompletion(wrapped.putAsync(key, value).asScala)(s, f)
}

case class IgniteNodeApi(wrapped: Ignite)(implicit val ec: ExecutionContext) extends IgniteApi with AsyncSupport {
  override def getOrCreateCache[K, V, U](name: String)(s: CacheApi[K, V] => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.getOrCreateCache[K, V](name)).map(CacheNodeApi(_)))(s, f)

  override def close[U]()(s: Unit => U, f: Throwable => U): Unit =
    withCompletion(Future(wrapped.close()))(s, f)
}

case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V])(implicit val ec: ExecutionContext) extends CacheApi[K, V] with AsyncSupport {
  override def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit =
  // TODO
    withCompletion(Future(wrapped.putAsync(key, value).get()))(s, f)
}

object IgniteApi extends AsyncSupport {
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

trait AsyncSupport {
  implicit val ec: ExecutionContext

  def withCompletion[T, U](fut: Future[T])(s: T => U, f: Throwable => U): Unit = fut.onComplete {
    case Success(value)     => s(value)
    case Failure(exception) => f(exception)
  }
}