/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.gatling.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.session.Session
import org.apache.ignite.Ignite

/** */
case class IgniteProtocol(ignite: Ignite) extends Protocol {
    type Components = IgniteComponents
}

/** */
object IgniteProtocol {
    val igniteProtocolKey = new ProtocolKey[IgniteProtocol, IgniteComponents] {
        def protocolClass: Class[io.gatling.core.protocol.Protocol] =
            classOf[IgniteProtocol].asInstanceOf[Class[io.gatling.core.protocol.Protocol]]

        def defaultProtocolValue(configuration: GatlingConfiguration): IgniteProtocol =
            throw new IllegalStateException("Can't provide a default value for IgniteProtocol")

        def newComponents(coreComponents: CoreComponents): IgniteProtocol => IgniteComponents = {
            igniteProtocol => IgniteComponents(igniteProtocol)
        }
    }
}

/** */
case class IgniteComponents(igniteProtocol: IgniteProtocol) extends ProtocolComponents {
    override def onStart: Session => Session = Session.Identity

    override def onExit: Session => Unit = ProtocolComponents.NoopOnExit
}
