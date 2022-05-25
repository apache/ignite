package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CacheGetAllActionBuilder[K, V](requestName: Expression[String],
                                          cacheName: Expression[String],
                                          keys: Expression[Set[K]],
                                          checks: Seq[IgniteCheck[K, V]] = Seq.empty) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, V]*): CacheGetAllActionBuilder[K, V] = this.copy(checks = newChecks)

  override def build(ctx: ScenarioContext, next: Action): Action =
    cache.CacheGetAllAction(requestName, cacheName, keys, checks, next, ctx)
}
