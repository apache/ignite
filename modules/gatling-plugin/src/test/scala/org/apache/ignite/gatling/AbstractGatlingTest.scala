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

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder
import org.apache.ignite.gatling.IgniteClientApi.IgniteApi
import org.apache.ignite.gatling.IgniteClientApi.thinClient
import org.apache.ignite.internal.client.thin.AbstractThinClientTest
import org.junit.Assert.assertTrue

class AbstractGatlingTest extends AbstractThinClientTest {

  override protected def beforeTest(): Unit = {
    super.beforeTest()
    startGrid(0)
  }

  override protected def afterTest(): Unit = {
    stopAllGrids()
    super.afterTest()
  }

  protected def runWith(api: IgniteApi)(simulationClass: String): Unit = {
    if (api == thinClient) {
      val sysProperties = System.getProperties
      sysProperties.setProperty("host", clientHost(grid(0).cluster.localNode))
      sysProperties.setProperty("port", String.valueOf(clientPort(grid(0).cluster.localNode)))
    } else {
      startClientGrid(1)
    }
    val gatlingPropertiesBuilder = new GatlingPropertiesBuilder
    gatlingPropertiesBuilder.simulationClass(simulationClass)
    gatlingPropertiesBuilder.noReports()

    assertTrue("Count of failed gatling events is not zero", Gatling.fromMap(gatlingPropertiesBuilder.build) == 0)
  }
}

object IgniteClientApi extends Enumeration {
  type IgniteApi = Value
  val thinClient, nodeApi = Value
}
