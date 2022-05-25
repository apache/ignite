package org.apache.ignite.gatling

import scala.language.implicitConversions

import org.apache.ignite.cache.CacheEntryProcessor

import javax.cache.processor.MutableEntry


object Predef extends IgniteDsl {
  val REPLICATED = org.apache.ignite.cache.CacheMode.REPLICATED
  val PARTITIONED = org.apache.ignite.cache.CacheMode.PARTITIONED

  val TRANSACTIONAL = org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL
  val ATOMIC = org.apache.ignite.cache.CacheAtomicityMode.ATOMIC

  val PESSIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
  val OPTIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC

  val REPEATABLE_READ = org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
  val READ_COMMITTED = org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
  val SERIALIZABLE = org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE

  implicit def toCacheEntryProcessor[K, V, T](f: (MutableEntry[K, V], Seq[Any]) => T): CacheEntryProcessor[K, V, T] =
    new CacheEntryProcessor[K, V, T] {
      override def process(mutableEntry: MutableEntry[K, V], objects: Object*): T =
        f.apply(mutableEntry, Seq(objects))
    }
}
