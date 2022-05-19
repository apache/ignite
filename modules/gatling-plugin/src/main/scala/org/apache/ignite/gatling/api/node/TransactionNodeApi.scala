package org.apache.ignite.gatling.api.node

import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.transactions.Transaction


case class TransactionNodeApi(wrapped: Transaction) extends TransactionApi {
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
