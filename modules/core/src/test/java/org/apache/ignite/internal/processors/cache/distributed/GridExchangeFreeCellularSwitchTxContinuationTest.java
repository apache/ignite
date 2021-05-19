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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridExchangeFreeCellularSwitchTxContinuationTest extends GridExchangeFreeCellularSwitchAbstractTest {
    /** Concurrency. */
    @Parameterized.Parameter(0)
    public TransactionConcurrency concurrency;

    /** Isolation. */
    @Parameterized.Parameter(1)
    public TransactionIsolation isolation;

    /** Start from. */
    @Parameterized.Parameter(2)
    public TransactionCoordinatorNode startFrom;

    /**
     *
     */
    @Parameterized.Parameters(name = "Isolation = {0}, Concurrency = {1}, Started from = {2}")
    public static Collection<Object[]> runConfig() {
        ArrayList<Object[]> params = new ArrayList<>();
        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values())
                for (TransactionCoordinatorNode from : TransactionCoordinatorNode.values())
                    if (from != TransactionCoordinatorNode.FAILED) // Impossible to continue tx started at failed node :)
                        params.add(new Object[] {concurrency, isolation, from});

        return params;
    }

    /**
     * Tests checks that txs started before the switch can be continued after the switch if they are not affected by
     * node fail.
     */
    @Test
    public void testAlreadyStartedTxsContinuationDuringAndAfterTheSwitch() throws Exception {
        int nodes = 6;

        startGridsMultiThreaded(nodes);

        blockRecoveryMessages();

        CellularCluster cluster = resolveCellularCluster(nodes, startFrom);

        Ignite orig = cluster.orig;
        Ignite failed = cluster.failed;

        int txCnt = 1024;
        int keysPerTx = 6; // See puts count inside the closure.
        int prepTxCnt = 100;
        int dataAmount = txCnt * keysPerTx + prepTxCnt;
        int totalDataAmount = dataAmount + prepTxCnt;

        Queue<Integer> keys = new ConcurrentLinkedDeque<>();
        Queue<Integer> primaryOnFailedKeys = new ConcurrentLinkedDeque<>();

        Queue<Integer> keysToCheck = new ConcurrentLinkedDeque<>();

        for (int i = 0; keys.size() < dataAmount; i++)
            if (!primaryNode(i, PART_CACHE_NAME).equals(failed)) // Will not cause node failed exception on put.
                keys.add(i);

        for (int i = 0; primaryOnFailedKeys.size() < prepTxCnt; i++)
            if (primaryNode(i, PART_CACHE_NAME).equals(failed)) // Will cause explicit recovery on node fail.
                primaryOnFailedKeys.add(i);

        CountDownLatch putInitLatch = new CountDownLatch(txCnt);
        CountDownLatch prepLatch = new CountDownLatch(prepTxCnt * 2);

        CountDownLatch nodeFailedLatch = new CountDownLatch(1);
        CountDownLatch nodesAwareOfFailLatch = new CountDownLatch(1);
        CountDownLatch txsRecoveryAllowedLatch = new CountDownLatch(1);
        CountDownLatch txsRecoveryFinishedLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
            try {
                Transaction tx = orig.transactions().txStart(concurrency, isolation);

                IgniteCache<Integer, Integer> cache = orig.getOrCreateCache(PART_CACHE_NAME);

                // Put before node fail.
                put(cache, keys, keysToCheck);

                long initTopVer =
                    ((IgniteEx)orig).context().cache().context().exchange().readyAffinityVersion().topologyVersion();

                putInitLatch.countDown();

                nodeFailedLatch.await();

                // Put right after node fail.
                put(cache, keys, keysToCheck);

                nodesAwareOfFailLatch.await();

                // Put when nodes are aware of fail.
                put(cache, keys, keysToCheck);

                txsRecoveryAllowedLatch.await();

                // Put right after recovery allowed.
                put(cache, keys, keysToCheck);

                txsRecoveryFinishedLatch.await();

                // Put right after recovery finished.
                put(cache, keys, keysToCheck);

                // Put with some random delay after recovery happen.
                U.sleep(ThreadLocalRandom.current().nextInt(5_000));

                put(cache, keys, keysToCheck);

                ((TransactionProxyImpl<?, ?>)tx).commit();

                long commitTopVer =
                    ((IgniteEx)orig).context().cache().context().exchange().readyAffinityVersion().topologyVersion();

                assertTrue(commitTopVer > initTopVer); // Started before the switch, but continued after it.
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, txCnt);

        IgniteInternalFuture<?> prepFut1 = multithreadedAsync(() -> { // Keys with unaffected primary.
            try {
                putInitLatch.await();

                Transaction tx = failed.transactions().txStart();

                IgniteCache<Integer, Integer> cache = failed.getOrCreateCache(PART_CACHE_NAME);

                put(cache, keys, keysToCheck);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();
                txsRecoveryFinishedLatch.await();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, prepTxCnt);

        IgniteInternalFuture<?> prepFut2 = multithreadedAsync(() -> { // Primary keys of failed primary.
            try {
                putInitLatch.await();

                Transaction tx = failed.transactions().txStart();

                IgniteCache<Integer, Integer> cache = failed.getOrCreateCache(PART_CACHE_NAME);

                put(cache, primaryOnFailedKeys, keysToCheck);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();
                txsRecoveryFinishedLatch.await();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, prepTxCnt);

        prepLatch.await();

        failed.close(); // Stopping node.

        nodeFailedLatch.countDown();

        awaitForSwitchOnNodeLeft(failed);

        nodesAwareOfFailLatch.countDown();

        // Allowing recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, blockedMsg -> true);
        }

        txsRecoveryAllowedLatch.countDown();

        for (Ignite ignite : G.allGrids()) {
            for (IgniteInternalTx tx : ((IgniteEx)ignite).context().cache().context().tm().activeTransactions()) {
                while (tx.state() == TransactionState.PREPARED)
                    U.sleep(100);
            }
        }

        txsRecoveryFinishedLatch.countDown();

        prepFut1.get();
        prepFut2.get();
        txFut.get();

        assertTrue(keys.isEmpty());
        assertTrue(primaryOnFailedKeys.isEmpty());

        assertEquals(totalDataAmount, keysToCheck.size());

        IgniteCache<Integer, Integer> cache = orig.getOrCreateCache(PART_CACHE_NAME);

        for (Integer i : keysToCheck)
            assertEquals(i, cache.get(i));
    }

    /**
     *
     */
    private void put(IgniteCache<Integer, Integer> cache, Queue<Integer> keysToPut, Queue<Integer> keysToCheck) {
        Integer key = keysToPut.remove();

        keysToCheck.add(key);

        cache.put(key, key);
    }
}
