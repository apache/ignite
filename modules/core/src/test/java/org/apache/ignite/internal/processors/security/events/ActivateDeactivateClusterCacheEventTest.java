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

import java.util.Collection;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;

/**
 * Test that a local listener and a remote filter get correct subjectId when
 * a server (client) node activates/deactivates the cluster.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ActivateDeactivateClusterCacheEventTest extends AbstractSecurityCacheEventTest {
    /** Server. */
    private static final String SRV = "server";

    /** Client. */
    private static final String CLNT = "client";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startClientAllowAll(CLNT);

        startGridAllowAll(SRV).cluster().state(ACTIVE);
    }

    /** */
    @Test
    public void testActivateClusterSrv() throws Exception {
        checkActivateCluster(SRV);
    }

    /** */
    @Test
    public void testActivateClusterClnt() throws Exception {
        checkActivateCluster(CLNT);
    }

    /** */
    @Test
    public void testDeactivateClusterSrv() throws Exception {
        checkDeactivateCluster(SRV);
    }

    /** */
    @Test
    public void testDeactivateClusterClnt() throws Exception {
        checkDeactivateCluster(CLNT);
    }

    /** */
    private void checkActivateCluster(final String node) throws Exception {
        clusterShouldBeActive();

        Collection<CacheConfiguration> configurations = cacheConfigurations(2);

        configurations.forEach(c -> grid(SRV).createCache(c));
        configurations.forEach(c -> grid(CLNT).cache(c.getName()));

        grid(SRV).cluster().state(INACTIVE);

        testCacheEvents(6, node, EVT_CACHE_STARTED, configurations,
            ccfgs -> grid(node).cluster().state(ACTIVE));
    }

    /** */
    private void checkDeactivateCluster(final String node) throws Exception {
        clusterShouldBeActive();

        testCacheEvents(6, node, EVT_CACHE_STOPPED, cacheConfigurations(2), ccfgs -> {
            ccfgs.forEach(c -> grid(CLNT).cache(c.getName()));

            grid(node).cluster().state(INACTIVE);
        });
    }

    /** */
    private void clusterShouldBeActive() {
        if (grid(SRV).cluster().state() != ACTIVE)
            grid(SRV).cluster().state(ACTIVE);
    }
}
