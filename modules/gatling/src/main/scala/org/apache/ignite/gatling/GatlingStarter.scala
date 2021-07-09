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

import com.fasterxml.jackson.databind.JsonNode
import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication

/**
 * Class to start the Gatling app.
 */
class GatlingStarter extends IgniteAwareApplication {
    def run(jsonNode: JsonNode) = {
        markInitialized()

        val props = new GatlingPropertiesBuilder()
            .resourcesDirectory(jsonNode.get("resourcesDirectory").asText())
            .resultsDirectory(jsonNode.get("resultsDirectory").asText())
            .binariesDirectory(jsonNode.get("binariesDirectory").asText())
            .simulationClass(jsonNode.get("simulationClass").asText())
            .noReports()
            .build

//        TODO
//        props.put("ignite", ignite)
//        props.put("client", client)

        Gatling.fromMap(props)

        markFinished()
    }
}