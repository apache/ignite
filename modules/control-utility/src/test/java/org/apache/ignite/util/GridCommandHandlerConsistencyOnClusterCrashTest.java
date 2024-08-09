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

package org.apache.ignite.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Before;
import org.junit.Test;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** */
public class GridCommandHandlerConsistencyOnClusterCrashTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Listening logger. */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** Number of cluster nodes. */
    public int nodes = 3;

    /** Number of backups for the default cache. */
    public int backupNodes = nodes - 1;

    /** */
    @Before public void beforeEachTest() throws Exception {
        cleanPersistenceDir();

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog)
            .setFailureHandler(new StopNodeFailureHandler());

        cfg.getDataStorageConfiguration()
            .setFileIOFactory(new ThrowableFileIOTestFactory(cfg.getDataStorageConfiguration().getFileIOFactory()))
            .setWalMode(WALMode.FSYNC); // Allows to use special IO at WAL as well.

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows to check values on backups for idle_verify.

        return cfg;
    }

    /** */
    @Test
    public void testLwmCountersOnClusterCrash() throws Exception {
        int prepareRqNum = 20;
        int finishRqNum = 30;

        IgniteEx ignite = startGrids(nodes);
        ignite.cluster().state(ClusterState.ACTIVE);

        final AtomicInteger updateCnt = new AtomicInteger();

        // Enough to have historical rebalance when needed.
        for (int i = 0; i < 2_000; i++)
            ignite.cache(DEFAULT_CACHE_NAME).put(updateCnt.incrementAndGet(), 0);

        stopAllGrids();
        startGrids(nodes);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        CountDownLatch prepareLatch = new CountDownLatch(backupNodes * prepareRqNum);
        CountDownLatch finishLatch = new CountDownLatch(backupNodes * finishRqNum);

        TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxPrepareRequest && prepareLatch.getCount() > 0) {
                    prepareLatch.countDown();

                    return true;
                }
                else if (msg instanceof GridDhtTxFinishRequest && finishLatch.getCount() > 0) {
                    finishLatch.countDown();

                    return true;
                }
                else
                    return false;
            }
        });

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < prepareRqNum; i++)
            GridTestUtils.runAsync(() -> primCache.put(updateCnt.incrementAndGet(), 0));

        prepareLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        for (int i = 0; i < finishRqNum; i++)
            GridTestUtils.runAsync(() -> primCache.put(updateCnt.incrementAndGet(), 0));

        finishLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        G.allGrids().forEach(
            node -> ((ThrowableFileIOTestFactory)node.configuration().getDataStorageConfiguration().getFileIOFactory())
                .setThrowEx(true));

        stopAllGrids();
        startGrids(nodes);

        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
        assertConflicts(false, false);
    }
}
