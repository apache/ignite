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

import java.util.function.Supplier;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgniteServiceDeployOnJoinedNodeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            // Server node stores service config.
            cfg = cfg.setServiceConfiguration(new ServiceConfiguration()
                .setName("service")
                .setService(new GreeterService())
                .setMaxPerNodeCount(1)
                // Service deployed on client node, only.
                //.setNodeFilter(n -> true)
                .setNodeFilter(ClusterNode::isClient)
            );
        }

        return cfg;
    }

    /** */
    @Test
    public void testDeployOnJoinedNode() throws Exception {
        // Server node only stores service config.
        try (IgniteEx ignored = startGrid(0)) {
            // Service must be deployed on client node.
            try (IgniteEx cli = startClientGrid(1)) {
                Supplier<String> srvc = cli.services(cli.cluster().forClients()).serviceProxy("service", Supplier.class, false);

                assertEquals("Hello", srvc.get());
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
