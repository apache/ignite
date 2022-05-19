package org.apache.ignite.gatling


object Predef extends IgniteDsl {
  val TRANSACTIONAL = org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL
  val ATOMIC = org.apache.ignite.cache.CacheAtomicityMode.ATOMIC

  val PESSIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
  val OPTIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC

  val REPEATABLE_READ = org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
  val READ_COMMITTED = org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
  val SERIALIZABLE = org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE
}
