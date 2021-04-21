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

package org.apache.ignite.configuration.sample;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple usage test of generated configuration schema.
 */
public class UsageTest {
    /** */
    private ConfigurationRegistry registry;

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /**
     * Test creation of configuration and calling configuration API methods.
     */
    @Test
    public void test() throws Exception {
        registry = new ConfigurationRegistry(
            Collections.singletonList(LocalConfiguration.KEY),
            Collections.emptyMap(),
            Collections.singletonList(new TestConfigurationStorage())
        );

        LocalConfiguration root = registry.getConfiguration(LocalConfiguration.KEY);

        root.change(local ->
            local.changeBaseline(baseline ->
                baseline.changeNodes(nodes ->
                    nodes.create("node1", node ->
                        node.initConsistentId("test").initPort(1000)
                    )
                ).changeAutoAdjust(autoAdjust ->
                    autoAdjust.changeEnabled(true).changeTimeout(100_000L)
                )
            )
        ).get(1, SECONDS);

        assertTrue(root.baseline().autoAdjust().enabled().value());

        root.baseline().autoAdjust().enabled().update(false).get(1, SECONDS);

        assertFalse(root.value().baseline().autoAdjust().enabled());
        assertFalse(root.baseline().value().autoAdjust().enabled());
        assertFalse(root.baseline().autoAdjust().value().enabled());
        assertFalse(root.baseline().autoAdjust().enabled().value());

        root.baseline().nodes().get("node1").autoAdjustEnabled().update(true).get(1, SECONDS);

        assertTrue(root.value().baseline().nodes().get("node1").autoAdjustEnabled());
        assertTrue(root.baseline().value().nodes().get("node1").autoAdjustEnabled());
        assertTrue(root.baseline().nodes().value().get("node1").autoAdjustEnabled());
        assertTrue(root.baseline().nodes().get("node1").value().autoAdjustEnabled());
        assertTrue(root.baseline().nodes().get("node1").autoAdjustEnabled().value());

        root.baseline().nodes().get("node1").change(node -> node.changeAutoAdjustEnabled(false)).get(1, SECONDS);
        assertFalse(root.value().baseline().nodes().get("node1").autoAdjustEnabled());

        root.baseline().nodes().change(nodes -> nodes.delete("node1")).get(1, SECONDS);

        assertNull(root.baseline().nodes().get("node1"));
        assertNull(root.value().baseline().nodes().get("node1"));
    }

    /**
     * Test to show an API to work with multiroot configurations.
     */
    @Test
    public void multiRootConfiguration() throws Exception {
        int failureDetectionTimeout = 30_000;
        int joinTimeout = 10_000;

        long autoAdjustTimeout = 30_000L;

        registry = new ConfigurationRegistry(
            Arrays.asList(NetworkConfiguration.KEY, LocalConfiguration.KEY),
            Collections.emptyMap(),
            Collections.singletonList(new TestConfigurationStorage())
        );

        registry.getConfiguration(LocalConfiguration.KEY).change(local ->
            local.changeBaseline(baseline ->
                baseline.changeAutoAdjust(autoAdjust ->
                    autoAdjust.changeEnabled(true).changeTimeout(autoAdjustTimeout)
                )
            )
        ).get(1, SECONDS);

        registry.getConfiguration(NetworkConfiguration.KEY).change(network ->
            network.changeDiscovery(discovery ->
                discovery
                    .changeFailureDetectionTimeout(failureDetectionTimeout)
                    .changeJoinTimeout(joinTimeout)
            )
        ).get(1, SECONDS);

        assertEquals(
            failureDetectionTimeout,
            registry.getConfiguration(NetworkConfigurationImpl.KEY).discovery().failureDetectionTimeout().value()
        );

        assertEquals(
            autoAdjustTimeout,
            registry.getConfiguration(LocalConfigurationImpl.KEY).baseline().autoAdjust().timeout().value()
        );
    }
}
