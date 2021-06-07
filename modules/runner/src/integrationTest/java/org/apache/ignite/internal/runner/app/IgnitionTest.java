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

package org.apache.ignite.internal.runner.app;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Ignition interface tests.
 */
class IgnitionTest {
    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>() {{
            put("node0", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");

            put("node1", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");

            put("node2", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");
        }};

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodesStartWithBootstrapConfiguration() {
        List<Ignite> startedNodes = new ArrayList<>();

        for (Map.Entry<String, String> nodeBootstrapCfg : nodesBootstrapCfg.entrySet())
            startedNodes.add(IgnitionManager.start(nodeBootstrapCfg.getKey(), nodeBootstrapCfg.getValue()));

        Assertions.assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);
    }

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-14709")
    void testNodeStartWithoutBootstrapConfiguration() {
        Ignite ignite = IgnitionManager.start("node0", null);

        Assertions.assertNotNull(ignite);
    }
}
