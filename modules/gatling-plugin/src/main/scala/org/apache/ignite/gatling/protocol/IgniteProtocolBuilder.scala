package org.apache.ignite.gatling.protocol

import org.apache.ignite.configuration.ClientConfiguration

case object IgniteProtocolBuilder {
    def cfg(cfg: ClientConfiguration): IgniteProtocolBuilder = IgniteProtocolBuilder(cfg)
}

case class IgniteProtocolBuilder(cfg: ClientConfiguration) {
    def build: IgniteProtocol = IgniteProtocol(cfg)
}
