package org.apache.ignite.gatling.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolKey}
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.ClientConfiguration

object IgniteProtocol {

    type Components = IgniteComponents

    val igniteProtocolKey: ProtocolKey[IgniteProtocol, Components] = new ProtocolKey[IgniteProtocol, Components] {
        override def protocolClass: Class[Protocol] =
            classOf[IgniteProtocol].asInstanceOf[Class[Protocol]]

        override def defaultProtocolValue(configuration: GatlingConfiguration): IgniteProtocol =
            throw new IllegalStateException("Can't provide a default value for IgniteProtocol")

        override def newComponents(coreComponents: CoreComponents): IgniteProtocol => IgniteComponents =
            igniteProtocol => {
                IgniteComponents(coreComponents, igniteProtocol)
            }
    }

    def apply(cfg: ClientConfiguration): IgniteProtocol = new IgniteProtocol(Left(cfg))
    def apply(ignite: Ignite): IgniteProtocol = new IgniteProtocol(Right(ignite))
}

class IgniteProtocol(val cfg: Either[ClientConfiguration, Ignite]) extends Protocol {
//    def cfg(cfg: ClientConfiguration): IgniteProtocol = copy(client_cfg = cfg)
}
