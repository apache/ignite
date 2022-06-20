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

package org.apache.ignite.internal.ducktest.utils.gatling;

import java.util.Optional;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import io.gatling.core.config.GatlingPropertiesBuilder;
import io.gatling.app.Gatling;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Gatling test.
 */
public class GatlingRunnerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();

        if (client != null) {
            igniteClient = client;
//            client.close();
        }

        Properties sysProperties = System.getProperties();
        sysProperties.setProperty("config", cfgPath);

        GatlingPropertiesBuilder gatlingPropertiesBuilder = new GatlingPropertiesBuilder();

        Optional.ofNullable(jsonNode.get("simulation"))
                .map(JsonNode::asText)
                .ifPresent(gatlingPropertiesBuilder::simulationClass);

        Optional.ofNullable(jsonNode.get("reportsOnly"))
                .map(JsonNode::asText)
                .ifPresent(gatlingPropertiesBuilder::reportsOnly);

        Gatling.fromMap(gatlingPropertiesBuilder.build());

        markFinished();

        igniteClient = null;
    }

    public static IgniteClient igniteClient;
}
