package org.apache.ignite.gatling.api.node

import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.transactions.Transaction

case class TransactionNodeApi(wrapped: Transaction) extends TransactionApi {
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
