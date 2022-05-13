package org.apache.ignite.gatling.request.builder

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.{CreateCacheAction, IgniteActionBuilder}

case class CreateCacheActionBuilder(
    requestName: Expression[String],
    cacheName: Expression[String]) extends IgniteActionBuilder {

    override def build(ctx: ScenarioContext, next: Action): Action =
        CreateCacheAction(requestName, cacheName, next, ctx)
}

