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

import org.apache.ignite.Ignite
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration

trait IgniteProtocolSupport {
  /** Ignite protocol builder starting point. */
  val igniteProtocol: IgniteProtocolBuilderBase.type = IgniteProtocolBuilderBase

  /**
   * Implicit to build protocol from the IgniteProtocolBuilder.
   * @param builder Protocol builder.
   * @return Protocol.
   */
  implicit def builder2igniteProtocol(builder: IgniteProtocolBuilder): IgniteProtocol = builder.build
  /**
   * Implicit to build protocol from the IgniteProtocolBuilderManualStartStep.
   * @param builder Protocol builder.
   * @return Protocol.
   */
  implicit def builderManualStartStep2igniteProtocol(builder: IgniteProtocolBuilderManualStartStep): IgniteProtocol = builder.build
}

/**
 * Base Ignite protocol builder.
 */
case object IgniteProtocolBuilderBase {
  /**
   * Specify Ignite API as Ignite (thin) client configuration.
   *
   * @param cfg ClientConfiguration instance.
   * @return Build step for additional protocol parameters.
   */
  def cfg(cfg: ClientConfiguration): IgniteProtocolBuilderManualStartStep =
    IgniteProtocolBuilderManualStartStep(IgniteClientConfigurationCfg(cfg))

  /**
   * Specify Ignite API as Ignite (thick) node configuration.
   *
   * @param cfg IgniteConfiguration instance.
   * @return Build step for additional protocol parameters.
   */
  def cfg(cfg: IgniteConfiguration): IgniteProtocolBuilderManualStartStep =
    IgniteProtocolBuilderManualStartStep(IgniteConfigurationCfg(cfg))

  /**
   * Specify Ignite API as pre-started Ignite (thick) node instance.
   *
   * @param ignite Ignite instance.
   * @return Build step for additional protocol parameters.
   */
  def cfg(ignite: Ignite): IgniteProtocolBuilder = IgniteProtocolBuilder(IgniteNodeCfg(ignite), manualClientStart = false)

  /**
   * Specify Ignite API as pre-started Ignite (thin) Client instance.
   *
   * @param igniteClient IgniteClient instance.
   * @return Build step for additional protocol parameters.
   */
  def cfg(igniteClient: IgniteClient): IgniteProtocolBuilder =
    IgniteProtocolBuilder(IgniteClientCfg(igniteClient), manualClientStart = false)
}

/**
 * Builder step for additional protocol parameters.
 *
 * @param cfg Ignite API configuration.
 */
case class IgniteProtocolBuilderManualStartStep(cfg: IgniteCfg) {

  /**
   * Specify the `withManualClientStart` flag.
   * @return Protocol builder further step.
   */
  def withManualClientStart: IgniteProtocolBuilder = IgniteProtocolBuilder(cfg, manualClientStart = true)

  /**
   * Builds Ignite protocol instance.
   * @return Protocol builder further step.
   */
  def build: IgniteProtocol = IgniteProtocolBuilder(cfg, manualClientStart = false).build
}

/**
 * Ignite protocol builder step for other parameters.
 *
 * @param cfg Ignite API configuration.
 * @param manualClientStart Manual client start flag.
 */
case class IgniteProtocolBuilder(cfg: IgniteCfg, manualClientStart: Boolean) {
  /**
   * Builds Ignite protocol instance.
   * @return Protocol builder further step.
   */
  def build: IgniteProtocol = IgniteProtocol(cfg, manualClientStart)
}
