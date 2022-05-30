package org.apache.ignite.gatling.api.thin

import org.apache.ignite.client.ClientTransaction
import org.apache.ignite.gatling.api.TransactionApi

case class TransactionThinApi(wrapped: ClientTransaction) extends TransactionApi {
  override def commit()(s: Unit => Unit, f: Throwable => Unit): Unit = {
    try {
      s(wrapped.commit())
    } catch  {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }

  override def rollback()(s: Unit => Unit, f: Throwable => Unit): Unit = {
    try {
      s(wrapped.rollback())
    } catch {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }
}
