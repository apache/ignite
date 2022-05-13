package org.apache.ignite.gatling.action

import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.Components

trait ActionBase {
    val ctx: ScenarioContext
    protected val components: Components = ctx.protocolComponentsRegistry.components(IgniteProtocol.igniteProtocolKey)
}
