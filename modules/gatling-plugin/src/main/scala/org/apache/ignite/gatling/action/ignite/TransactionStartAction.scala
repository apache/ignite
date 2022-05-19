package org.apache.ignite.gatling.action.ignite

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

case class TransactionStartAction(requestName: Expression[String],
                                  next: Action,
                                  ctx: ScenarioContext
                            ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("txStart")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, txStart")

    val igniteApi: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield igniteApi.txStart()(
      transactionApi => logAndExecuteNext(session.set("transactionApi", transactionApi), resolvedRequestName, startTime,
        ctx.coreComponents.clock.nowMillis, OK, next, None, None),
      ex => {
        logger.error(s"session user id: #${session.userId}, txStart failed", ex)
        logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      }
    ))
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }
}
