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
import io.gatling.core.protocol.Protocol
import io.gatling.core.protocol.ProtocolKey
import org.apache.ignite.Ignite
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.api.IgniteApi

object IgniteProtocol {

  type Components = IgniteComponents

  val IGNITE_API_SESSION_KEY = "igniteApi"
  val TRANSACTION_API_SESSION_KEY = "transactionApi"

  val igniteProtocolKey: ProtocolKey[IgniteProtocol, Components] = new ProtocolKey[IgniteProtocol, Components] {
    override def protocolClass: Class[Protocol] =
      classOf[IgniteProtocol].asInstanceOf[Class[Protocol]]

    override def defaultProtocolValue(configuration: GatlingConfiguration): IgniteProtocol =
      throw new IllegalStateException("Can't provide a default value for IgniteProtocol")

    override def newComponents(coreComponents: CoreComponents): IgniteProtocol => IgniteComponents =
      igniteProtocol => {
        IgniteComponents(coreComponents, igniteProtocol, IgniteApi(igniteProtocol))
      }
  }
}

case class IgniteProtocol(cfg: IgniteCfg, manualClientStart: Boolean = false) extends Protocol

sealed trait IgniteCfg

case class IgniteClientCfg(client: IgniteClient) extends IgniteCfg

case class IgniteNodeCfg(ignite: Ignite) extends IgniteCfg

case class IgniteClientConfigurationCfg(cfg: ClientConfiguration) extends IgniteCfg

case class IgniteConfigurationCfg(cfg: IgniteConfiguration) extends IgniteCfg
