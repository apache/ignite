package org.apache.ignite.internal.gatling.simulation

import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

import scala.util.Try

trait DucktapeIgniteSupport {
  val ducktapeIgnite: IgniteProtocol =
    Option(System.getProperty("config"))
      .map(cfgPath =>
        Try(IgnitionEx.loadSpringBean[ClientConfiguration](cfgPath, "thin.client.cfg"))
          .map(IgniteProtocol(_))
          .getOrElse(IgniteProtocol(IgnitionEx.allGrids().get(0)))
      )
      .getOrElse(IgniteProtocol(IgnitionEx.allGrids().get(0)))
}
