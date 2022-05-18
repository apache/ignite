package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CacheGetActionBuilder[K, V](requestName: Expression[String],
                                       cacheName: Expression[String],
                                       key: Expression[K],
                                       checks: Seq[IgniteCheck[K, V]] = Seq.empty) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, V]*): CacheGetActionBuilder[K, V] = this.copy(checks = newChecks)

  override def build(ctx: ScenarioContext, next: Action): Action =
    cache.CacheGetAction(requestName, cacheName, key, checks, next, ctx)
}
