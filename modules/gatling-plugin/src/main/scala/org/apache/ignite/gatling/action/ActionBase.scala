package org.apache.ignite.gatling.action

import io.gatling.commons.stats.Status
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.Components

trait ActionBase {
    val ctx: ScenarioContext
    protected val components: Components = ctx.protocolComponentsRegistry.components(IgniteProtocol.igniteProtocolKey)

    protected def executeNext(
                               session: Session,
                               requestName: String,
                               sent: Long,
                               received: Long,
                               status: Status,
                               next: Action,
                               responseCode: Option[String],
                               message: Option[String],
                             ): Unit = {
        ctx.coreComponents.statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestName,
            sent,
            received,
            status,
            responseCode,
            message,
        )
        next ! session.logGroupRequestTimings(sent, received)
    }
}

trait IgniteActionBase {}
