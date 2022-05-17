package org.apache.ignite.gatling.action

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation._
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen

import org.apache.ignite.gatling.client.IgniteApi

case class CloseClientAction(requestName: Expression[String],
    next: Action,
    ctx: ScenarioContext
) extends ChainableAction with NameGen with ActionBase {

    override val name: String = genName("close")

    override protected def execute(session: Session): Unit = {

        val igniteApi: IgniteApi = session("igniteApi").as[IgniteApi]

        (for {
            resolvedRequestName <- requestName(session)
            startTime           <- ctx.coreComponents.clock.nowMillis.success
        } yield igniteApi
          .close()(
              _ => executeNext(session.set("igniteApi", None), resolvedRequestName, startTime,
                  ctx.coreComponents.clock.nowMillis, OK, next, None, None),
              ex => executeNext(session, resolvedRequestName, startTime,
                  ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
          ))
          .onFailure(ex =>
              requestName(session).map { resolvedRequestName =>
                  ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
                  executeNext(session, resolvedRequestName, ctx.coreComponents.clock.nowMillis,
                      ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex),
                  )
              },
          )
    }
}
