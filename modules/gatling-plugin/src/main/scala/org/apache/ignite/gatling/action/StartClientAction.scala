package org.apache.ignite.gatling.action

import io.gatling.commons.validation._
import io.gatling.commons.stats.OK
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.Ignition

case class StartClientAction[K, V](requestName: Expression[String],
    next: Action,
    ctx: ScenarioContext
) extends ChainableAction with NameGen with ActionBase {

    override val name: String = genName("startClient")

    override protected def execute(session: Session): Unit = {
        for {
            resolvedRequestName <- requestName(session)
            startTime           <- ctx.coreComponents.clock.nowMillis.success
        } yield {
            val client = Ignition.startClient(components.igniteProtocol.cfg)
            val finishTime = ctx.coreComponents.clock.nowMillis

            ctx.coreComponents.statsEngine.logResponse(
                session.scenario,
                session.groups,
                resolvedRequestName,
                startTime,
                finishTime,
                OK,
                None,
                None
            )

            next ! session.set("client", client)
        }
    }
}
