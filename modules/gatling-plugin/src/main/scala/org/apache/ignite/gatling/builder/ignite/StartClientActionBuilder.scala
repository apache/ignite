package org.apache.ignite.gatling.builder.ignite

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.ignite
import org.apache.ignite.gatling.action.ignite.StartClientAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class StartClientActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    ignite.StartClientAction(requestName, next, ctx)
}
