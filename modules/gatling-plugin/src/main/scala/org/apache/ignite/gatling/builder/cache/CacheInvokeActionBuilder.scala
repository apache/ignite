package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder


case class CacheInvokeActionBuilderBase[K, V, T](requestName: Expression[String],
                                                 cacheName: Expression[String],
                                                 key: Expression[K],
                                                 args: Seq[Any] = null) {

    def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
      CacheInvokeActionBuilder[K, V, T](requestName, cacheName, key, entryProcessor, null)

    def args(args: Any*): CacheInvokeActionBuilderProcessorStep[K, V, T] =
      CacheInvokeActionBuilderProcessorStep[K, V, T](requestName, cacheName, key, args)
}

case class CacheInvokeActionBuilderProcessorStep[K, V, T](requestName: Expression[String],
                                             cacheName: Expression[String],
                                             key: Expression[K],
                                             arguments: Seq[Any]) {

    def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
      CacheInvokeActionBuilder[K, V, T](requestName, cacheName, key, entryProcessor, arguments)
}

case class CacheInvokeActionBuilder[K, V, T](requestName: Expression[String],
                                             cacheName: Expression[String],
                                             key: Expression[K],
                                             entryProcessor: CacheEntryProcessor[K, V, T],
                                             arguments: Seq[Any],
                                             checks: Seq[IgniteCheck[K, T]] = Seq.empty) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, T]*): CacheInvokeActionBuilder[K, V, T] = this.copy(checks = newChecks)

  override def build(ctx: ScenarioContext, next: Action): Action =
    cache.CacheInvokeAction[K, V, T](requestName, cacheName, key, entryProcessor, arguments, checks, next, ctx)
}
