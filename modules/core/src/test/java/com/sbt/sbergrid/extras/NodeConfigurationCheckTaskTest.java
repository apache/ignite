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

package com.sbt.sbergrid.extras;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static com.sbt.sbergrid.extras.NodeConfigurationCheckTask.prepareChecks;

/** */
public class NodeConfigurationCheckTaskTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setThinClientConfiguration(new ThinClientConfiguration()
                .setMaxActiveComputeTasksPerConnection(1)));

        return cfg;
    }

    /** Simple test for {@link NodeConfigurationCheckTask}. */
    @Test
    public void testCollectNodeConfiguration() throws Exception {
        try (IgniteEx ignites = startGrids(2);
             IgniteEx clientNode = startClientGrid(3);
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {

            Map<UUID, List<String>> srvRes = client.compute().execute(
                NodeConfigurationCheckTask.class.getName(),
                prepareChecks(Arrays.asList(
                    new AlwayFailCheck(),
                    new AlwayPassCheck(),
                    new IsClientCheck()
                ))
            );

            assertEquals(2, srvRes.size());

            for (Map.Entry<UUID, List<String>> entry : srvRes.entrySet()) {
                List<String> res = entry.getValue();

                assertEquals(3, res.size());

                assertEquals("false", res.get(0));
                assertNull(res.get(1));
                assertEquals("false", res.get(2)); // isClient = false
            }

            Map<UUID, List<String>> cliRes = client.compute(client.cluster().forClients()).execute(
                NodeConfigurationCheckTask.class.getName(),
                prepareChecks(Arrays.asList(
                    new AlwayFailCheck(),
                    new AlwayPassCheck(),
                    new IsClientCheck()
                ))
            );

            assertEquals(1, cliRes.size());

            List<String> res = cliRes.get(clientNode.localNode().id());

            assertEquals(3, res.size());

            assertEquals("false", res.get(0));
            assertNull(res.get(1));
            assertEquals("true", res.get(2)); // isClient = true
        }
    }

    /** First check. */
    public static class AlwayFailCheck implements Function<IgniteConfiguration, String> {
        /** {@inheritDoc} */
        @Override public String apply(IgniteConfiguration configuration) {
            return "false";
        }
    }

    /** Second check. */
    public static class AlwayPassCheck implements Function<IgniteConfiguration, String> {
        /** {@inheritDoc} */
        @Override public String apply(IgniteConfiguration configuration) {
            return null;
        }
    }

    /** Is client check. */
    public static class IsClientCheck implements Function<IgniteConfiguration, String> {
        /** {@inheritDoc} */
        @Override public String apply(IgniteConfiguration configuration) {
            return Objects.toString(configuration.isClientMode());
        }
    }
}
