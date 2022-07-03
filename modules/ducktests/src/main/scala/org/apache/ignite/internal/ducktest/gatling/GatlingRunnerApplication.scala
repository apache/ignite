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
package org.apache.ignite.internal.ducktest.gatling

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder
import org.apache.ignite.gatling.Predef
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.ducktest.gatling.GatlingRunnerApplication.igniteProtocol
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication

object GatlingRunnerApplication {
  /** Name of system property to pass node index to simulation. */
  val NODE_IDX_PROPERTY_NAME: String = "nodeIdx"

  /** Name of system property to pass total node count to simulation. */
  val NODE_COUNT_PROPERTY_NAME: String = "nodeCount"

  /** Ignite protocol to be used by the simulation. */
  var igniteProtocol: Option[IgniteProtocol] = None
}

/**
 *
 */
class GatlingRunnerApplication extends IgniteAwareApplication {
  /**
   * @inheritdoc
   * @param jsonNode @inheritdoc
   */
  @throws[Exception]
  override protected def run(jsonNode: JsonNode): Unit = {
    markInitialized()
    if (client != null) {
      igniteProtocol = Some(Predef.igniteProtocol.cfg(client).build)
    } else {
      igniteProtocol = Some(Predef.igniteProtocol.cfg(ignite).build)
    }

    Option(jsonNode.get("options"))
      .foreach(
        _.fields.asScala
          .filter(option => option.getValue.isValueNode)
          .foreach(option => System.setProperty(option.getKey, option.getValue.asText()))
      )

    val gatlingPropertiesBuilder: GatlingPropertiesBuilder = new GatlingPropertiesBuilder

    Option(jsonNode.get("simulation"))
      .map(_.asText)
      .foreach(gatlingPropertiesBuilder.simulationClass)

    Option(jsonNode.get("reportsOnly"))
      .map(_.asText)
      .foreach(gatlingPropertiesBuilder.reportsOnly)

    val result: Int = Gatling.fromMap(gatlingPropertiesBuilder.build)

    if (result == 0) {
      markFinished()
    } else {
      markBroken(new RuntimeException("Gatling simulation assertion(s) failed."))
    }
  }
}
