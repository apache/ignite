package org.apache.ignite.gatling.protocol

import org.apache.ignite.Ignite
import org.apache.ignite.configuration.{ClientConfiguration, IgniteConfiguration}

case object IgniteProtocolBuilder {
    def cfg(cfg: ClientConfiguration): IgniteThinProtocolBuilder = IgniteThinProtocolBuilder(cfg)
    def cfg(ignite: Ignite): IgniteProtocolBuilder = IgniteProtocolBuilder(ignite)
}

case class IgniteThinProtocolBuilder(cfg: ClientConfiguration) {
    def build: IgniteProtocol = IgniteProtocol(cfg)
}

case class IgniteProtocolBuilder(ignite: Ignite) {
    def build: IgniteProtocol = IgniteProtocol(ignite)
}
