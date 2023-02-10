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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/** */
public class TxRecoveryCommitMessagesTest extends GridCommonAbstractTest {
    /** Backups count. */
    private static final int BACKUPS = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setBackups(BACKUPS)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        return cfg;
    }

    /**
     * All transactions are prepared, originated node failed. Primary nodes commits themselves and backup nodes.
     */
    @Test
    public void testNoTryRollbackConcurrentlyWithCommit() throws Exception {
        int nodes = 3;
        int primTxsPerNode = 3;

        Ignite g = startGrids(nodes);

        g.cluster().state(ACTIVE);

        Ignite cln = startClientGrid(nodes);

        TestRecordingCommunicationSpi.spi(cln).blockMessages((n, msg) -> msg instanceof GridNearTxFinishRequest);

        AtomicInteger key = new AtomicInteger();

        Affinity<Object> aff = cln.affinity(DEFAULT_CACHE_NAME);

        for (int n = 0; n < nodes; n++) {
            int n0 = n;

            multithreadedAsync(
                () -> cln.cache(DEFAULT_CACHE_NAME).put(keyForNode(aff, key, grid(n0).localNode()), 0),
                primTxsPerNode, "async-tx-" + n);
        }

        TestRecordingCommunicationSpi.spi(cln).waitForBlocked(nodes * primTxsPerNode);

        GridCompoundFuture<IgniteInternalTx, ?> txFinFuts = new GridCompoundFuture<>();

        for (int n = 0; n < nodes; n++) {
            TestRecordingCommunicationSpi.spi(grid(n)).record((node, msg) -> true);

            for (IgniteInternalTx tx: grid(n).context().cache().context().tm().activeTransactions()) {
                txFinFuts.add(tx.finishFuture());
            }
        }

        txFinFuts.markInitialized();

        stopGrid(nodes);

        txFinFuts.get(getTestTimeout());

        for (int n = 0; n < nodes; n++) {
            List<Object> msgs = TestRecordingCommunicationSpi.spi(grid(n)).recordedMessages(true);

            int reqCnt = primTxsPerNode * BACKUPS;
            int resCnt = (nodes - 1) * primTxsPerNode;

            assertEquals(reqCnt + resCnt, msgs.size());

            List<Object> reqMsgs = msgs.stream().filter(m -> m instanceof GridDhtTxFinishRequest)
                .collect(Collectors.toList());

            assertEquals(primTxsPerNode * BACKUPS, reqMsgs.size());
            assertFalse(reqMsgs.stream().anyMatch(m -> !((GridDhtTxFinishRequest)m).commit()));

            List<Object> resMsgs = msgs.stream().filter(m -> m instanceof GridDhtTxFinishResponse)
                .collect(Collectors.toList());

            assertEquals(resCnt, resMsgs.size());
        }
    }
}
