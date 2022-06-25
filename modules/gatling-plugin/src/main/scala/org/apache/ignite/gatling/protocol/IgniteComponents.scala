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

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.protocol.IgniteProtocol.IGNITE_API_SESSION_KEY

case class IgniteComponents(coreComponents: CoreComponents, igniteProtocol: IgniteProtocol, igniteApi: Option[IgniteApi] = None)
    extends ProtocolComponents
    with StrictLogging {

  override def onStart: Session => Session = session =>
    igniteApi
      .map(api => session.set(IGNITE_API_SESSION_KEY, api))
      .getOrElse(session)

  override def onExit: Session => Unit = session =>
    if (igniteApi.isEmpty) {
      session(IGNITE_API_SESSION_KEY)
        .asOption[IgniteApi]
        .foreach(_.close()(_ => (), _ => ()))
    }
}
