package org.apache.ignite.gatling.action.ignite

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

case class StartClientAction(requestName: Expression[String],
                             next: Action,
                             ctx: ScenarioContext
                            ) extends ChainableAction with NameGen with ActionBase {

  override val name: String = genName("startClient")

  override protected def execute(session: Session): Unit = {
    (for {
      resolvedRequestName <- requestName(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield IgniteApi(components.igniteProtocol)(
      igniteApi => logAndExecuteNext(session.set("igniteApi", igniteApi), resolvedRequestName, startTime,
        ctx.coreComponents.clock.nowMillis, OK, next, None, None),
      ex => logAndExecuteNext(session, resolvedRequestName, startTime,
        ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
    ))
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }
}
