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

/**
 * Ignite protocol globals.
 */
object IgniteProtocol {

  /** Session key the IgniteApi instance is stored under. */
  val IgniteApiSessionKey = "__igniteApi__"
  /**
   * Session key the TransactionApi instance is stored under.
   *
   * Presence of the TransactionApi instance in session means that the Ignite transaction is active now.
   */
  val TransactionApiSessionKey = "__transactionApi__"

  /**
   * Initialise components of the Ignite protocol on simulation start.
   */
  val IgniteProtocolKey: ProtocolKey[IgniteProtocol, IgniteComponents] = new ProtocolKey[IgniteProtocol, IgniteComponents] {
    override def protocolClass: Class[Protocol] =
      classOf[IgniteProtocol].asInstanceOf[Class[Protocol]]

    override def defaultProtocolValue(configuration: GatlingConfiguration): IgniteProtocol =
      IgniteProtocolBuilderBase.cfg(new IgniteConfiguration()).build

    /**
     * Return lambda to init the Ignite protocol components.
     *
     * Note, lambda would start new Ignite API instance if none was passed as a protocol configuration parameter
     * (unless the `manualClientStart` protocol parameter was used).
     *
     * @param coreComponents Gatling core components.
     * @return Lambda creating Ignite components from the Ignite protocol parameters provided.
     */
    override def newComponents(coreComponents: CoreComponents): IgniteProtocol => IgniteComponents =
      igniteProtocol => IgniteComponents(coreComponents, igniteProtocol, IgniteApi(igniteProtocol))
  }
}

/**
 * Ignite protocol parameters.
 *
 * @param cfg Ignite API configuration.
 * @param manualClientStart If true the default shared instance of Ignite API will not be started before the
 *                          simulation start and it will not be automatically inserted into the client session
 *                          upon injection into the scenario. To start Ignite API instance scenario should contain
 *                          explicit `start` and `close` actions.
 */
case class IgniteProtocol(cfg: IgniteCfg, manualClientStart: Boolean = false) extends Protocol

/**
 * Abstract Ignite API configuration.
 */
sealed trait IgniteCfg

/**
 * Ignite API configuration containing the pre-started Ignite (thin) Client instance.
 *
 * @param client Instance of Ignite (thin) Client.
 */
case class IgniteClientCfg(client: IgniteClient) extends IgniteCfg

/**
 * Ignite API configuration containing the pre-started Ignite (thick) node instance.
 *
 * @param ignite Instance of the Ignite grid.
 */
case class IgniteNodeCfg(ignite: Ignite) extends IgniteCfg

/**
 * Ignite API configuration containing the Ignite (thin) Client configuration instance.
 *
 * @param cfg Ignite (thin) client configuration.
 */
case class IgniteClientConfigurationCfg(cfg: ClientConfiguration) extends IgniteCfg

/**
 * Ignite API configuration containing the Ignite node (thick) configuration instance.
 *
 * @param cfg Ignite (thick) node configuration.
 */
case class IgniteConfigurationCfg(cfg: IgniteConfiguration) extends IgniteCfg
