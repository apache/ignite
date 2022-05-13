package org.apache.ignite.gatling.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session
import org.apache.ignite.client.IgniteClient

case class IgniteComponents(
    coreComponents: CoreComponents,
    igniteProtocol: IgniteProtocol,
) extends ProtocolComponents {

    override def onStart: Session => Session = Session.Identity

    override def onExit: Session => Unit = session => {
        Option(session("client").as[IgniteClient]).foreach(_.close())
    }
}
