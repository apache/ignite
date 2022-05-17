package org.apache.ignite.gatling

import io.gatling.core.session.Expression
import org.apache.ignite.gatling.protocol.{IgniteProtocol, IgniteProtocolBuilder, IgniteThinProtocolBuilder}
import org.apache.ignite.gatling.request.builder.Ignite

trait IgniteDsl {
    val ignite: IgniteProtocolBuilder.type = IgniteProtocolBuilder

    def ignite(requestName: Expression[String]): Ignite = new Ignite(requestName)

    implicit def igniteProtocolBuilder2igniteProtocol(builder: IgniteProtocolBuilder): IgniteProtocol = builder.build
    implicit def igniteThinProtocolBuilder2igniteProtocol(builder: IgniteThinProtocolBuilder): IgniteProtocol = builder.build
}
