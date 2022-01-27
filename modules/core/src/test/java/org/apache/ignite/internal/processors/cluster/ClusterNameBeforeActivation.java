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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that cluster name could not be accessed before system cache initialized.
 */
public class ClusterNameBeforeActivation extends GridCommonAbstractTest {
    /** Activation latch. */
    private CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(new DelayedActivationPluginProvider());

        return cfg;
    }

    /**
     * Tests that getting the cluster name cache waits until system cache started. Scenario:
     * <ul>
     *     <li>Start a server node.</li>
     *     <li>Start a client node with the plugin provider delaying activation and system cache initalization.</li>
     *     <li>Run a job on the client node that gets the cluster name.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetClusterNameBeforeSystemCacheStarted() throws Exception {
        IgniteEx ignite = startGrid(0);

        latch = new CountDownLatch(1);

        multithreadedAsync(() -> startClientGrid(1), 1);

        latch.await(1, TimeUnit.MINUTES);

        String clusterName = ignite.compute(ignite.cluster().forClients()).call(new IgniteCallable<String>() {
            @Override public String call() throws Exception {
                return ((IgniteEx)Ignition.localIgnite()).context().cluster().clusterName();
            }
        });

        assertNotNull(clusterName);
    }

    /**
     * Plugin provider delaying activation.
     */
    private class DelayedActivationPluginProvider extends AbstractTestPluginProvider implements IgniteChangeGlobalStateSupport {
        /** {@inheritDoc} */
        @Override public String name() {
            return "testPlugin";
        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node, Serializable data) {
        }

        /** {@inheritDoc} */
        @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
            if (latch != null) {
                latch.countDown();

                U.sleep(2_000);
            }
        }

        /** {@inheritDoc} */
        @Override public void onDeActivate(GridKernalContext kctx) {
        }
    }
}
