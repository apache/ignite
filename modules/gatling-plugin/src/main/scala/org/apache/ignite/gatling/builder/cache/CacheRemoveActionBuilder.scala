package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CacheRemoveActionBuilder[K](requestName: Expression[String],
                                       cacheName: Expression[String],
                                       key: Expression[K]) extends IgniteActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action =
    cache.CacheRemoveAction(requestName, cacheName, key, next, ctx)
}
