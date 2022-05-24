package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CachePutActionBuilder[K, V](requestName: Expression[String],
                                       cacheName: Expression[String],
                                       key: Expression[K],
                                       value: Expression[V]) extends IgniteActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action =
    cache.CachePutAction(requestName, cacheName, key, value, next, ctx)
}
