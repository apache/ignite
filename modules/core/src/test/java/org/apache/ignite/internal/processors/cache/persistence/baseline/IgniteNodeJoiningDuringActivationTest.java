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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.loadtests.colocation.GridTestLifecycleBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests that Ignite processors and managers are properly started
 * when a new node joins a cluster that is in transition state.
 */
public class IgniteNodeJoiningDuringActivationTest extends GridCommonAbstractTest {
    /** Lifecycle bean. */
    private LifecycleBean lifecycleBean;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (lifecycleBean != null)
            cfg.setLifecycleBeans(lifecycleBean);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests client joining a cluster that is in a transition state.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientJoiningClusterInTransitionState() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        CountDownLatch transitionStateLatch = new CountDownLatch(1);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(g1);

        // Block PME related to activation in order to freeze the cluster in a transition state.
        spi1.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage)
                return ((GridDhtPartitionsSingleMessage)msg).exchangeId() != null;

            return false;
        });

        // Start activation.
        IgniteInternalFuture<?> activationFut = GridTestUtils.runAsync(() -> g0.cluster().state(ACTIVE));

        transitionStateLatch.await(30_000, MILLISECONDS);

        AtomicReference<IgniteEx> clientRef = new AtomicReference<>();

        lifecycleBean = new GridTestLifecycleBean() {
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START)
                    clientRef.set(g);
            }
        };

        // Start a new client node that will join the cluster which is in a transition state.
        IgniteInternalFuture<?> clientFut = GridTestUtils.runAsync(new GridTestUtils.RunnableX() {
            @Override public void runx() throws Exception {
                startClientGrid(2);
            }
        });

        // Wait for a new client node.
        boolean res = GridTestUtils.waitForCondition(() ->
            clientRef.get() != null
            && clientRef.get().context().state() != null
            && clientRef.get().context().state().clusterState().transitionRequestId() != null,
            30_000);

        assertTrue("Client node is not started.", res);

        spi1.stopBlock();
        activationFut.get();
        clientFut.get();

        // Let's start a new distributed atomic long, for instance.
        // The successful result of the operation indicates the corresponding processor properly initialized.
        IgniteAtomicLong atomicLong = clientRef.get().atomicLong("testAtomicLong", 1L, true);

        assertNotNull("Distributed atomic long was not created.", atomicLong);
    }
}
