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

package org.apache.ignite.internal.processors.security.events;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId when
 * a server (client) node activates/deactivates the cluster.
 */
@RunWith(Parameterized.class)
@SuppressWarnings({"rawtypes"})
public class ActivateDeactivateClusterCacheEventTest extends AbstractSecurityCacheEventTest {
    /** Server. */
    private static final String SRV = "server";

    /** Client. */
    private static final String CLNT = "client";

    /** Parameters. */
    @Parameterized.Parameters(name = "node={0}")
    public static Iterable<String[]> data() {
        return Arrays.asList(new String[] {SRV}, new String[] {CLNT});
    }

    /** Initiate node name. */
    @Parameterized.Parameter
    public String node;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startClientAllowAll(CLNT);

        startGridAllowAll(SRV).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testActivateCluster() throws Exception {
        clusterShouldBeActive();

        Collection<CacheConfiguration> configurations = cacheConfigurations(2, false);

        configurations.forEach(c -> grid(CLNT).createCache(c.getName()));

        grid(SRV).cluster().state(ClusterState.INACTIVE);

        testCacheEvents(6, node, EVT_CACHE_STARTED, configurations,
            ccfgs -> grid(node).cluster().state(ClusterState.ACTIVE));
    }

    /** */
    @Test
    public void testDeactivateCluster() throws Exception {
        clusterShouldBeActive();

        testCacheEvents(6, node, EVT_CACHE_STOPPED, cacheConfigurations(2, true), ccfgs -> {
            ccfgs.forEach(c -> grid(CLNT).cache(c.getName()));

            grid(node).cluster().state(ClusterState.INACTIVE);
        });
    }

    /** */
    private void clusterShouldBeActive() {
        if (grid(SRV).cluster().state() != ClusterState.ACTIVE)
            grid(SRV).cluster().state(ClusterState.ACTIVE);
    }
}
