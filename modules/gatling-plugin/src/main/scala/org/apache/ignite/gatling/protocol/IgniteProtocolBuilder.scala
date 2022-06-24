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

case object IgniteProtocolBuilderBase {
  def cfg(cfg: ClientConfiguration): IgniteProtocolBuilderManualStartStep =
    IgniteProtocolBuilderManualStartStep(IgniteClientConfigurationCfg(cfg))

  def cfg(cfg: IgniteConfiguration): IgniteProtocolBuilderManualStartStep =
    IgniteProtocolBuilderManualStartStep(IgniteConfigurationCfg(cfg))

  def cfg(ignite: Ignite): IgniteProtocolBuilder = IgniteProtocolBuilder(IgniteNodeCfg(ignite))

  def cfg(igniteClient: IgniteClient): IgniteProtocolBuilder = IgniteProtocolBuilder(IgniteClientCfg(igniteClient))
}

case class IgniteProtocolBuilderManualStartStep(cfg: IgniteCfg) {
  def withManualClientStart: IgniteProtocolBuilder = IgniteProtocolBuilder(cfg, manualClientStart = true)

  def build: IgniteProtocol = IgniteProtocol(cfg)
}

case class IgniteProtocolBuilder(cfg: IgniteCfg, manualClientStart: Boolean = false) {
  def build: IgniteProtocol = IgniteProtocol(cfg, manualClientStart)
}
