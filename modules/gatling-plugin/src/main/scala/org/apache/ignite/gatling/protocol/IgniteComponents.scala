package org.apache.ignite.gatling.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session
import org.apache.ignite.gatling.api.IgniteApi

case class IgniteComponents(
    coreComponents: CoreComponents,
    igniteProtocol: IgniteProtocol,
) extends ProtocolComponents {

    override def onStart: Session => Session = igniteProtocol.cfg match {
        case Left(_) => Session.Identity
        case Right(_) => s => s.set("igniteApi", IgniteApi(igniteProtocol)(_ => (),_ => ()))
    }

    override def onExit: Session => Unit = session => {
        session("igniteApi").asOption[IgniteApi]
          .foreach(_.close()(_ => (), _ => ()))
    }
}
