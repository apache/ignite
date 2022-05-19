package org.apache.ignite.gatling.api.thin

import org.apache.ignite.client.ClientTransaction
import org.apache.ignite.gatling.api.TransactionApi


case class TransactionThinApi(wrapped: ClientTransaction) extends TransactionApi {
  override def commit[U]()(s: Unit => U, f: Throwable => U): Unit = {
    try {
      s(wrapped.commit())
    } catch  {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }

  override def rollback[U]()(s: Unit => U, f: Throwable => U): Unit = {
    try {
      s(wrapped.rollback())
    } catch {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }
}
