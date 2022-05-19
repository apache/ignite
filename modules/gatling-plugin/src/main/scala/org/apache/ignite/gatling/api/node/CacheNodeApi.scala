package org.apache.ignite.gatling.api.node

import org.apache.ignite.IgniteCache
import org.apache.ignite.gatling.api.CacheApi

case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V]) extends CacheApi[K, V] {
  override def put[U](key: K, value: V)(s: Void => U, f: Throwable => U): Unit = {
    wrapped.putAsync(key, value)
      .listen(fut => {
        try {
          s.apply(fut.get())
        } catch {
          case ex: Throwable => f(ex)
        }
      })
  }

  override def get[U](key: K)(s: Map[K, V] => U, f: Throwable => U): Unit = {
    wrapped.getAsync(key)
      .listen(fut => {
        try {
          s(Map((key, fut.get())))
        } catch {
          case ex: Throwable => f(ex)
        }
      })
  }
}
