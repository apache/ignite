package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

import scala.util.Try

class IgniteSimulationBase extends Simulation {
  val protocol: IgniteProtocol = Option(System.getProperty("config"))
        .map(cfgPath =>
          Try(IgnitionEx.loadSpringBean[ClientConfiguration](cfgPath, "thin.client.cfg"))
            .map(IgniteProtocol(_))
            .getOrElse(IgniteProtocol(IgnitionEx.allGrids().get(0)))
        )
        .getOrElse(IgniteProtocol(IgnitionEx.allGrids().get(0)))
}
