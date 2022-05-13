package org.apache.ignite.gatling.request.builder

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.{StartClientAction, IgniteActionBuilder}

case class StartClientActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
    override def build(ctx: ScenarioContext, next: Action): Action =
        StartClientAction(requestName, next, ctx)
}
