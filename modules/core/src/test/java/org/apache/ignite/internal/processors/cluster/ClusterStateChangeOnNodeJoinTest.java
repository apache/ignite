/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.loadtests.colocation.GridTestLifecycleBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests joining a new node to the cluster that is in transition state.
 */
public class ClusterStateChangeOnNodeJoinTest extends GridCommonAbstractTest {
    /** */
    private LifecycleBean lifecycleBean;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (lifecycleBean != null)
            cfg.setLifecycleBeans(lifecycleBean);

        cfg.setClusterStateOnStart(INACTIVE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testJoiningClientNodeToClusterInTransitionState() throws Exception {
        joiningNodeToClusterInTransitionState(true);
    }

    @Test
    public void testJoiningServerNodeToClusterInTransitionState() throws Exception {
        joiningNodeToClusterInTransitionState(false);
    }

    /**
     * Starts a cluster of 2 nodes and then tries to join a new node when the cluster is in transition state, i.e.
     * the cluster is moved from INACTIVE to ACTIVE state.
     *
     * @param client If {@code true} then the test will start a client node, and server node in case of {@code false.}
     * @throws Exception If failed.
     */
    public void joiningNodeToClusterInTransitionState(boolean client) throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(g1);

        // Block PME that is related to cluster state change.
        spi1.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                latch.countDown();

                return true;
            }

            return false;
        });

        IgniteInternalFuture<?> activationFut = runAsync(() -> g0.cluster().state(ACTIVE));

        latch.await();

        AtomicReference<IgniteEx> clientRef = new AtomicReference<>();
        lifecycleBean = new GridTestLifecycleBean() {
            /** {@inheritDoc}. */
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == BEFORE_NODE_START)
                    clientRef.set(g);
            }
        };

        IgniteInternalFuture<?> clientFut = runAsync(() -> client ? startClientGrid(2) : startGrid(2));

        boolean res = waitForCondition(() ->
            clientRef.get() != null
                && clientRef.get().context().state() != null
                && clientRef.get().context().state().clusterState().transitionRequestId() != null, 30_000);

        assertTrue("Failed to wait for transition state.", res);

        spi1.stopBlock();

        activationFut.get();

        clientFut.get();

        // Try to create a new data structure.
        clientRef.get().atomicLong("testAtomicLong", 1L, true);

        // Check that the node is able to execute compute tasks as well.
        clientRef.get().compute().execute(new TestTask(), null);
    }

    /**
     * Task for testing purposes.
     */
    static final class TestTask extends ComputeTaskAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,@Nullable String arg) {
            return Collections.singletonMap(new ComputeJobAdapter() {
                /** {@inheritDoc} */
                @Override public Object execute() throws IgniteException {
                    return null;
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
