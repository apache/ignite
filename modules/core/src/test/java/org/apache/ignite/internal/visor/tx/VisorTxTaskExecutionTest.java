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

package org.apache.ignite.internal.visor.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * VisorTxTask integration test
 */
public class VisorTxTaskExecutionTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "test";

    /** Server node count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (!igniteInstanceName.startsWith("client")) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(GRID_CNT - 1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        final IgniteEx crd = startGrid(0);

        startGridsMultiThreaded(1, GRID_CNT - 1);

        crd.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Started client.
     * @throws Exception If f nodeailed.
     */
    private Ignite startClient() throws Exception {
        Ignite client = startClientGrid("client");

        assertTrue(client.configuration().isClientMode());

        assertNotNull(client.cache(CACHE_NAME));

        return client;
    }


    /** Test limit parameter for VisorTxTask */
    @Test
    public void testVisorTxTaskLimitParam() throws Exception {
        final int txCnt = 50;
        final long latchTimeoutSeconds = 60;
        final int testLimit = 10;

        final Ignite client = startClient();

        final List<Integer> keys = primaryKeys(grid(0).cache(CACHE_NAME), txCnt);

        CountDownLatch txLatch = new CountDownLatch(txCnt);
        CountDownLatch commitLatch = new CountDownLatch(1);

        List<IgniteInternalFuture> futures = new ArrayList<>(txCnt);

        for (int i = 0; i < txCnt; i++) {

            final int f = i;

            futures.add(runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart()) {
                        client.cache(CACHE_NAME).put(keys.get(f), 0);

                        txLatch.countDown();

                        U.await(commitLatch, latchTimeoutSeconds, TimeUnit.SECONDS);

                        tx.commit();
                    }
                    catch (Exception e) {
                        if (!(e.getCause() instanceof NodeStoppingException))
                            log.error("Error while transaction executing.", e);

                        //NodeStoppingException expected.
                    }
                }
            }));
        }

        spi(client).blockMessages((node, msg) -> msg instanceof GridNearTxFinishRequest);

        U.awaitQuiet(txLatch);

        commitLatch.countDown();

        VisorTxTaskArg arg =
            new VisorTxTaskArg(VisorTxOperation.INFO, testLimit, null, null, null, null, null, null, null, null, null);

        Map<ClusterNode, VisorTxTaskResult> res = client.compute(client.cluster().forPredicate(F.alwaysTrue())).
            execute(new VisorTxTask(), new VisorTaskArgument<>(client.cluster().localNode().id(), arg, false));

        //All transactions are in PREPARED state.
        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            if (entry.getValue().getInfos().isEmpty())
                fail("Every node should have transaction info.");

            assertEquals(testLimit, entry.getValue().getInfos().size());
        }

        arg = new VisorTxTaskArg(VisorTxOperation.KILL, null, null, null, null, null, null, null, null, null, null);

        client.compute(client.cluster().forPredicate(F.alwaysTrue()))
            .execute(new VisorTxTask(), new VisorTaskArgument<>(client.cluster().localNode().id(), arg, false));
    }

}
