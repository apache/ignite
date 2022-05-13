package org.apache.ignite.gatling.action

import io.gatling.commons.validation._
import io.gatling.commons.stats.{KO, OK}
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.client.IgniteClient

case class CreateCacheAction[K, V](requestName: Expression[String],
    cacheName: Expression[String],
    next: Action,
    ctx: ScenarioContext
) extends ChainableAction with NameGen with ActionBase {

    override val name: String = genName("createCache")

    override protected def execute(session: Session): Unit = {

        val client: IgniteClient = session("client").as[IgniteClient]

        for {
            resolvedRequestName <- requestName(session)
            resolvedCacheName   <- cacheName(session)
            startTime           <- ctx.coreComponents.clock.nowMillis.success
        } yield {
            client.getOrCreateCache(resolvedCacheName)
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

            next ! session
        }
    }
}
