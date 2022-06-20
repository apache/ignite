package org.apache.ignite.internal.gatling.simulation

import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx
import scala.util.Try
import io.gatling.core.Predef.Simulation
import org.apache.ignite.internal.ducktest.utils.gatling.GatlingRunnerApplication

trait DucktapeIgniteSupport {
  this: Simulation =>

  val ducktapeIgnite: IgniteProtocol =
    Option(GatlingRunnerApplication.igniteClient)
      .map(client => ignite.cfg(client))
      .getOrElse(ignite.cfg(IgnitionEx.allGrids().get(0)))
      .build

//    Option(System.getProperty("config"))
//      .map(cfgPath =>
//        ignite.cfg(GatlingRunnerApplication.igniteClient)
////        Try(IgnitionEx.loadSpringBean[ClientConfiguration](cfgPath, "thin.client.cfg"))
////          .map(cfg => ignite.cfg(cfg))
////          .getOrElse(ignite.cfg(IgnitionEx.allGrids().get(0)))
//      )
//      .getOrElse(ignite.cfg(IgnitionEx.allGrids().get(0)))
//      .build
}
