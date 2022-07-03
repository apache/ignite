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
package org.apache.ignite.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.apache.ignite.internal.IgnitionEx
import org.junit.Test

/**
 * Tests ignite protocol configuration.
 */
class ProtocolTest extends AbstractGatlingTest {

  /** @inheritdoc */
  override protected def beforeTest(): Unit = {
    super.beforeTest()
    ProtocolTest.igniteConfiguration = getConfiguration()
  }

  @Test
  /** Tests ignite protocol setup with thin client configuration. */
  def thinClientConfig(): Unit = runWith(ThinClient)(simulation = "org.apache.ignite.gatling.ThinClientConfigSimulation")

  @Test
  /** Tests ignite protocol setup with thick client configuration. */
  def thickClientConfig(): Unit = run(simulation = "org.apache.ignite.gatling.ThickClientConfigSimulation")

  @Test
  /** Tests ignite protocol setup with thin client configuration and manual client start. */
  def thinClientConfigManualStart(): Unit = runWith(ThinClient)(simulation = "org.apache.ignite.gatling.ThinClientConfigManualSimulation")

  @Test
  /** Tests ignite protocol setup with thick client configuration and manual client start.. */
  def thickClientConfigManualStart(): Unit = run(simulation = "org.apache.ignite.gatling.ThickClientConfigManualSimulation")

  @Test
  /** Tests ignite protocol setup with thin client instance. */
  def thinClient(): Unit = runWith(ThinClient)(simulation = "org.apache.ignite.gatling.ThinClientSimulation")

  @Test
  /** Tests ignite protocol setup with thick client instance. */
  def thickClient(): Unit = runWith(NodeApi)(simulation = "org.apache.ignite.gatling.ThickClientSimulation")
}

private object ProtocolTest {
  /** Ignite node configuration to be passed from test to simulation. */
  var igniteConfiguration: IgniteConfiguration = new IgniteConfiguration()
}

abstract class BaseProtocolSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "TEST-CACHE"
  /** Actions to execute. */
  val actions: ChainBuilder = ignite(
    start as "start",
    create(cache) as "create",
    put[String, Int](cache, "#{key}", "#{value}"),
    get[String, Int](cache, "#{key}") check entries[String, Int].transform(_.value).is("#{value}"),
    close as "close"
  )
  setUp(scenario("protocol").feed(feeder).exec(actions).inject(atOnceUsers(1)))
    .protocols(protocol)
    .assertions(global.failedRequests.count.is(0))
}

class ThinClientConfigSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(new ClientConfiguration().setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}"))
}

class ThickClientConfigSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(ProtocolTest.igniteConfiguration.setClientMode(true).setIgniteInstanceName("client"))
}

class ThinClientConfigManualSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(new ClientConfiguration().setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}"))
      .withManualClientStart
}

class ThickClientConfigManualSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(ProtocolTest.igniteConfiguration.setClientMode(true).setIgniteInstanceName("client"))
      .withManualClientStart
}

class ThinClientSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(Ignition.startClient(new ClientConfiguration().setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}")))
}

class ThickClientSimulation extends BaseProtocolSimulation {
  /** @return @inheritdoc */
  override def protocol: IgniteProtocol =
    igniteProtocol
      .cfg(IgnitionEx.allGrids().get(1))
}
