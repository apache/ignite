package org.apache.ignite.gatling.action

import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.protocol.ProtocolComponentsRegistry
import org.apache.ignite.gatling.protocol.IgniteComponents
import org.apache.ignite.gatling.protocol.IgniteProtocol.igniteProtocolKey

abstract class IgniteActionBuilder() extends ActionBuilder {
    protected def components(protocolComponentsRegistry: ProtocolComponentsRegistry): IgniteComponents =
        protocolComponentsRegistry.components(igniteProtocolKey)
}
