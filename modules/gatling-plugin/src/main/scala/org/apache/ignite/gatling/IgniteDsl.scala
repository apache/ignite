package org.apache.ignite.gatling

import io.gatling.core.session.Expression
import org.apache.ignite.gatling.builder.ignite.Ignite
import org.apache.ignite.gatling.check.IgniteCheckSupport
import org.apache.ignite.gatling.protocol.{IgniteProtocol, IgniteProtocolBuilder, IgniteThinProtocolBuilder}

trait IgniteDsl extends IgniteCheckSupport {
    val ignite: IgniteProtocolBuilder.type = IgniteProtocolBuilder

    def ignite(requestName: Expression[String]): Ignite = new Ignite(requestName)

    implicit def igniteProtocolBuilder2igniteProtocol(builder: IgniteProtocolBuilder): IgniteProtocol = builder.build
    implicit def igniteThinProtocolBuilder2igniteProtocol(builder: IgniteThinProtocolBuilder): IgniteProtocol = builder.build
}
