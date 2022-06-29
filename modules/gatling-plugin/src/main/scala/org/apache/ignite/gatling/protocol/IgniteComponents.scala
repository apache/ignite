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
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey

/**
 * Ignite gatling protocol components holder.
 *
 * @param coreComponents Core Gatling components.
 * @param igniteProtocol Ignite protocol instance.
 * @param igniteApi Shared default Ignite API instance.
 */
case class IgniteComponents(coreComponents: CoreComponents, igniteProtocol: IgniteProtocol, igniteApi: Option[IgniteApi] = None)
    extends ProtocolComponents
    with StrictLogging {

  /**
   * Return lambda to init client session before injection into scenario.
   *
   * Lambda puts default shared Ignite API instance (if it exists) into the session object.
   *
   * @return Lambda to be called to init session.
   */
  override def onStart: Session => Session = session =>
    igniteApi
      .map(api => session.set(IgniteApiSessionKey, api))
      .getOrElse(session)

  /**
   * Return lambda to clean-up the client session after scenario finish.
   *
   * Lambda closes Ignite API instance if it is still in the session (unless it's a default shared instance).
   *
   * @return Lambda to be called to clean-up session.
   */
  override def onExit: Session => Unit = session =>
    if (igniteApi.isEmpty) {
      session(IgniteApiSessionKey)
        .asOption[IgniteApi]
        .foreach(_.close()(_ => (), _ => ()))
    }
}
