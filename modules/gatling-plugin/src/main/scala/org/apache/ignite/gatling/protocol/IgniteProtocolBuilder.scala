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
import org.apache.ignite.configuration.ClientConfiguration

case object IgniteProtocolBuilder {
    def cfg(cfg: ClientConfiguration): IgniteThinProtocolBuilder = IgniteThinProtocolBuilder(cfg)
    def cfg(ignite: Ignite): IgniteProtocolBuilder = IgniteProtocolBuilder(ignite)
}

case class IgniteThinProtocolBuilder(cfg: ClientConfiguration) {
    def build: IgniteProtocol = IgniteProtocol(cfg)
}

case class IgniteProtocolBuilder(ignite: Ignite) {
    def build: IgniteProtocol = IgniteProtocol(ignite)
}
