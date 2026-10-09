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

package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests check the following case:
 * 1. Node "A" starts with the service configuration. But, NodeFilter filters out node "A".
 * 2. Node "B" starts and NodeFilter conforms it.
 * 3. It is expected that service will be deployed on node "B".
 */
@RunWith(Parameterized.class)
public class IgniteServiceDeployOnJoinedNodeTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean nodeBClient;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "nodeBClient={0}")
    public static Collection<?> parameters() {
        return List.of(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setClientMode("B".equals(igniteInstanceName) && nodeBClient);

        if (cfg.getConsistentId().equals("A")) {
            // Server node stores service config.
            cfg = cfg.setServiceConfiguration(new ServiceConfiguration()
                .setName("service")
                .setService(new GreeterService())
                .setMaxPerNodeCount(1)
                // Service deployed on node "B", only.
                .setNodeFilter(n -> n.consistentId().equals("B"))
            );
        }

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        // Node A only stores service config.
        try (IgniteEx a = startGrid("A")) {
            // Service must be deployed on node B.
            try (IgniteEx b = startGrid("B")) {
                assertEquals(b.configuration().isClientMode(), (Boolean)nodeBClient);

                ClusterGroup grp = nodeBClient ? b.cluster().forClients() : b.cluster().forServers();

                assertEquals("Hello", b.services(grp).serviceProxy("service", Supplier.class, false).get());
            }
        }
    }

    /** */
    private static class GreeterService implements Supplier<String>, Service {
        /** {@inheritDoc} */
        @Override public String get() {
            return "Hello";
        }
    }
}
