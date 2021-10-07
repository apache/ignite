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
import java.util.UUID;
import java.util.function.Predicate;
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

            Map<UUID, List<Boolean>> srvRes = client.compute().execute(
                NodeConfigurationCheckTask.class.getName(),
                prepareChecks(Arrays.asList(
                    new AlwayFailCheck(),
                    new AlwayPassCheck(),
                    new IsClientCheck()
                ))
            );

            assertEquals(2, srvRes.size());

            for (Map.Entry<UUID, List<Boolean>> entry : srvRes.entrySet()) {
                List<Boolean> res = entry.getValue();

                assertEquals(3, res.size());

                assertFalse(res.get(0));
                assertTrue(res.get(1));
                assertFalse(res.get(2));
            }

            Map<UUID, List<Boolean>> cliRes = client.compute(client.cluster().forClients()).execute(
                NodeConfigurationCheckTask.class.getName(),
                prepareChecks(Arrays.asList(
                    new AlwayFailCheck(),
                    new AlwayPassCheck(),
                    new IsClientCheck()
                ))
            );

            assertEquals(1, cliRes.size());

            List<Boolean> res = cliRes.get(clientNode.localNode().id());

            assertEquals(3, res.size());

            assertFalse(res.get(0));
            assertTrue(res.get(1));
            assertTrue(res.get(2));
        }
    }

    /** First check. */
    public static class AlwayFailCheck implements Predicate<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public boolean test(IgniteConfiguration configuration) {
            return false;
        }
    }

    /** Second check. */
    public static class AlwayPassCheck implements Predicate<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public boolean test(IgniteConfiguration configuration) {
            return true;
        }
    }

    /** Is client check. */
    public static class IsClientCheck implements Predicate<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public boolean test(IgniteConfiguration configuration) {
            return configuration.isClientMode();
        }
    }
}
