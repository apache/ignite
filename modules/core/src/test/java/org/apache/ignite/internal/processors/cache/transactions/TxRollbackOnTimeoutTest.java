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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static java.lang.Thread.sleep;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests an ability to eagerly rollback timed out transactions.
 */
public class TxRollbackOnTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final long DURATION = 60 * 1000L;

    /** */
    private static final long TX_MIN_TIMEOUT = 1;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = "client".equals(igniteInstanceName);

        cfg.setClientMode(client);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            if (nearCacheEnabled())
                ccfg.setNearConfiguration(new NearCacheConfiguration());

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @return Near cache flag.
     */
    protected boolean nearCacheEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If f nodeailed.
     * @return Started client.
     */
    private Ignite startClient() throws Exception {
        Ignite client = startGrid("client");

        assertTrue(client.configuration().isClientMode());

        if (nearCacheEnabled())
            client.createNearCache(CACHE_NAME, new NearCacheConfiguration<>());
        else
            assertNotNull(client.cache(CACHE_NAME));

        return client;
    }

    /**
     * @param e Exception.
     */
    protected void validateDeadlockException(Exception e) {
        assertEquals("Deadlock report is expected",
            TransactionDeadlockException.class, e.getCause().getCause().getClass());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockAndConcurrentTimeout() throws Exception {
        startClient();

        for (Ignite node : G.allGrids()) {
            log.info("Test with node: " + node.name());

            lock(node, false);

            lock(node, false);

            lock(node, true);
        }
    }

    /**
     * @param node Node.
     * @param retry {@code True}
     * @throws Exception If failed.
     */
    private void lock(final Ignite node, final boolean retry) throws Exception {
        final IgniteCache<Object, Object> cache = node.cache(CACHE_NAME);

        final int KEYS_PER_THREAD = 10_000;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                int start = idx * KEYS_PER_THREAD;
                int end = start + KEYS_PER_THREAD;

                int locked = 0;

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                        for (int i = start; i < end; i++) {
                            cache.get(i);

                            locked++;
                        }

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    info("Expected error: " + e);
                }

                info("Done, locked: " + locked);

                if (retry) {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 10 * 60_000, 0)) {
                        for (int i = start; i < end; i++)
                            cache.get(i);

                        cache.put(start, 0);

                        tx.commit();
                    }
                }
            }
        }, Math.min(4, Runtime.getRuntime().availableProcessors()), "tx-thread");
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout() throws Exception {
        waitingTxUnblockedOnTimeout(grid(0), grid(0));

        waitingTxUnblockedOnTimeout(grid(0), grid(1));

        Ignite client = startClient();

        waitingTxUnblockedOnTimeout(grid(0), client);

        waitingTxUnblockedOnTimeout(grid(1), client);

        waitingTxUnblockedOnTimeout(client, grid(0));

        waitingTxUnblockedOnTimeout(client, grid(1));

        waitingTxUnblockedOnTimeout(client, client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnThreadDeath() throws Exception {
        waitingTxUnblockedOnThreadDeath(grid(0), grid(0));

        waitingTxUnblockedOnThreadDeath(grid(0), grid(1));

        Ignite client = startClient();

        waitingTxUnblockedOnThreadDeath(grid(0), client);

        waitingTxUnblockedOnThreadDeath(grid(1), client);

        waitingTxUnblockedOnThreadDeath(client, grid(0));

        waitingTxUnblockedOnThreadDeath(client, grid(1));

        waitingTxUnblockedOnThreadDeath(client, client);
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     *
     * @throws Exception If failed.
     */
    public void testDeadlockUnblockedOnTimeout() throws Exception {
        deadlockUnblockedOnTimeout(ignite(0), ignite(1));

        deadlockUnblockedOnTimeout(ignite(0), ignite(0));

        Ignite client = startClient();

        deadlockUnblockedOnTimeout(ignite(0), client);

        deadlockUnblockedOnTimeout(client, ignite(0));
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     *
     * @param node1 First node.
     * @param node2 Second node.
     * @throws Exception If failed.
     */
    private void deadlockUnblockedOnTimeout(final Ignite node1, final Ignite node2) throws Exception {
        info("Start test [node1=" + node1.name() + ", node2=" + node2.name() + ']');

        final CountDownLatch l = new CountDownLatch(2);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction tx = node1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 5000, 2)) {
                        node1.cache(CACHE_NAME).put(1, 10);

                        l.countDown();

                        U.awaitQuiet(l);

                        node1.cache(CACHE_NAME).put(2, 20);

                        tx.commit();

                        fail();
                    }
                }
                catch (CacheException e) {
                    // No-op.
                    validateDeadlockException(e);
                }
            }
        }, "First");

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = node2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 2)) {
                    node2.cache(CACHE_NAME).put(2, 2);

                    l.countDown();

                    U.awaitQuiet(l);

                    node2.cache(CACHE_NAME).put(1, 1);

                    tx.commit();
                }
            }
        }, "Second");

        fut1.get();
        fut2.get();

        assertTrue("Expecting committed key 2", node1.cache(CACHE_NAME).get(2) != null);
        assertTrue("Expecting committed key 1", node1.cache(CACHE_NAME).get(1) != null);

        node1.cache(CACHE_NAME).removeAll(F.asSet(1, 2));
    }

    /**
     * Tests timeout object cleanup on tx commit.
     *
     * @throws Exception If failed.
     */
    public void testTimeoutRemoval() throws Exception {
        IgniteEx client = (IgniteEx)startClient();

        final long TX_TIMEOUT = 250;

        final int modesCnt = 5;

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(grid(0), i, TX_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(client, i, TX_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(grid(0), i, TX_MIN_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(client, i, TX_MIN_TIMEOUT);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // Repeat with more iterations to make sure everything is cleared.
        for (int i = 0; i < 500; i++)
            testTimeoutRemoval0(client, rnd.nextInt(modesCnt), TX_MIN_TIMEOUT);
    }

    /**
     * Tests timeouts in all tx configurations.
     *
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (int op = 0; op < 4; op++)
                    testSimple0(concurrency, isolation, op);
            }
    }

    /**
     * Test timeouts with random values and different tx configurations.
     */
    public void testRandomMixedTxConfigurations() throws Exception {
        final Ignite client = startClient();

        final AtomicBoolean stop = new AtomicBoolean();

        final long seed = System.currentTimeMillis();

        final Random r = new Random(seed);

        log.info("Using seed: " + seed);

        final int threadsCnt = Runtime.getRuntime().availableProcessors() * 2;

        for (int k = 0; k < threadsCnt; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final TransactionConcurrency[] TC_VALS = TransactionConcurrency.values();
        final TransactionIsolation[] TI_VALS = TransactionIsolation.values();

        final LongAdder cntr0 = new LongAdder();
        final LongAdder cntr1 = new LongAdder();
        final LongAdder cntr2 = new LongAdder();
        final LongAdder cntr3 = new LongAdder();

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    int nodeId = r.nextInt(GRID_CNT + 1);

                    Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    TransactionConcurrency conc = TC_VALS[r.nextInt(TC_VALS.length)];
                    TransactionIsolation isolation = TI_VALS[r.nextInt(TI_VALS.length)];

                    int k = r.nextInt(threadsCnt);

                    long timeout = r.nextInt(200) + 50;

                    // Roughly 50% of transactions should time out.
                    try (Transaction tx = node.transactions().txStart(conc, isolation, timeout, 1)) {
                        cntr0.add(1);

                        final Long v = (Long)node.cache(CACHE_NAME).get(k);

                        assertNotNull("Expecting not null value: " + tx, v);

                        final int delay = r.nextInt(400);

                        if (delay > 0)
                            sleep(delay);

                        node.cache(CACHE_NAME).put(k, v + 1);

                        tx.commit();

                        cntr1.add(1);
                    }
                    catch (TransactionTimeoutException e) {
                        cntr2.add(1);
                    }
                    catch (CacheException e) {
                        assertEquals(TransactionTimeoutException.class, X.getCause(e).getClass());

                        cntr2.add(1);
                    }
                    catch (Exception e) {
                        cntr3.add(1);
                    }
                }
            }
        }, threadsCnt, "tx-async-thread");

        sleep(DURATION);

        stop.set(true);

        try {
            fut.get(30_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            error("Transactions hang", e);

            for (Ignite node : G.allGrids())
                ((IgniteKernal)node).dumpDebugInfo();

            fut.cancel(); // Try to interrupt hanging threads.

            throw  e;
        }

        log.info("Tx test stats: started=" + cntr0.sum() +
            ", completed=" + cntr1.sum() +
            ", failed=" + cntr3.sum() +
            ", timedOut=" + cntr2.sum());

        assertEquals("Expected finished count same as started count", cntr0.sum(), cntr1.sum() + cntr2.sum() +
            cntr3.sum());
    }

    /**
     * Tests timeout on DHT primary node for all tx configurations.
     *
     * @throws Exception If failed.
     */
    public void testTimeoutOnPrimaryDHTNode() throws Exception {
        final ClusterNode n0 = grid(0).affinity(CACHE_NAME).mapKeyToNode(0);

        final Ignite prim = G.ignite(n0.id());

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                testTimeoutOnPrimaryDhtNode0(prim, concurrency, isolation);
        }
    }

    /**
     *
     */
    public void testLockRelease() throws Exception {
        final Ignite client = startClient();

        final AtomicInteger idx = new AtomicInteger();

        final int threadCnt = Runtime.getRuntime().availableProcessors() * 2;

        final CountDownLatch readStartLatch = new CountDownLatch(1);

        final CountDownLatch commitLatch = new CountDownLatch(threadCnt - 1);

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                final int idx0 = idx.getAndIncrement();

                if (idx0 == 0) {
                    try(final Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                        client.cache(CACHE_NAME).put(0, 0); // Lock is owned.

                        readStartLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        tx.commit();
                    }
                }
                else {
                    try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 300, 1)) {
                        U.awaitQuiet(readStartLatch);

                        client.cache(CACHE_NAME).get(0); // Lock acquisition is queued.
                    }
                    catch (CacheException e) {
                        assertTrue(e.getMessage(), X.hasCause(e, TransactionTimeoutException.class));
                    }

                    commitLatch.countDown();
                }
            }
        }, threadCnt, "tx-async");

        fut.get();

        Thread.sleep(500);

        assertEquals(0, client.cache(CACHE_NAME).get(0));

        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final IgniteInternalFuture<?> f = ig.context().cache().context().
                partitionReleaseFuture(new AffinityTopologyVersion(G.allGrids().size() + 1, 0));

            assertTrue("Unexpected incomplete future", f.isDone());
        }

    }

    /**
     *
     */
    public void testEnlistManyRead() throws Exception {
        testEnlistMany(false);
    }

    /**
     *
     */
    public void testEnlistManyWrite() throws Exception {
        testEnlistMany(true);
    }

    /**
     *
     */
    private void testEnlistMany(boolean write) throws Exception {
        final Ignite client = startClient();

        Map<Integer, Integer> entries = new HashMap<>();

        for (int i = 0; i < 1000000; i++)
            entries.put(i, i);

        try(Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 200, 0)) {
            if (write)
                client.cache(CACHE_NAME).putAll(entries);
            else
                client.cache(CACHE_NAME).getAll(entries.keySet());

            tx.commit();
        }
        catch (Throwable t) {
            assertTrue(X.hasCause(t, TransactionTimeoutException.class));
        }

        assertEquals(0, client.cache(CACHE_NAME).size());
    }

    /**
     *
     * @param prim Primary node.
     * @param conc Concurrency.
     * @param isolation Isolation.

     * @throws Exception If failed.
     */
    private void testTimeoutOnPrimaryDhtNode0(final Ignite prim, final TransactionConcurrency conc,
        final TransactionIsolation isolation)
        throws Exception {

        log.info("concurrency=" + conc + ", isolation=" + isolation);

        // Force timeout on primary DHT node by blocking DHT prepare response.
        toggleBlocking(GridDhtTxPrepareResponse.class, prim, true);

        final int val = 0;

        try {
            multithreaded(new Runnable() {
                @Override public void run() {
                    try (Transaction txOpt = prim.transactions().txStart(conc, isolation, 300, 1)) {

                        prim.cache(CACHE_NAME).put(val, val);

                        txOpt.commit();
                    }
                }
            }, 1, "tx-async-thread");

            fail();
        }
        catch (TransactionTimeoutException e) {
            // Expected.
        }

        toggleBlocking(GridDhtTxPrepareResponse.class, prim, false);

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(GRID_CNT + 1, 0);

        for (Ignite ignite : G.allGrids())
            ((IgniteEx)ignite).context().cache().context().partitionReleaseFuture(topVer).get(10_000);
    }

    /**
     * @param cls Message class.
     * @param nodeToBlock Node to block.
     * @param block Block.
     */
    private void toggleBlocking(Class<? extends Message> cls, Ignite nodeToBlock, boolean block) {
        for (Ignite ignite : G.allGrids()) {
            if (ignite == nodeToBlock)
                continue;

            final TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            if (block)
                spi.blockMessages(cls, nodeToBlock.name());
            else
                spi.stopBlock(true);
        }
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param op Operation to test.
     * @throws Exception If failed.
     */
    private void testSimple0(TransactionConcurrency concurrency, TransactionIsolation isolation, int op) throws Exception {
        Ignite near = grid(0);

        final int key = 1, val = 1;

        final long TX_TIMEOUT = 250;

        IgniteCache<Object, Object> cache = near.cache(CACHE_NAME);

        try (Transaction tx = near.transactions().txStart(concurrency, isolation, TX_TIMEOUT, 1)) {
            cache.put(key, val);

            U.sleep(TX_TIMEOUT * 2);

            try {
                switch (op) {
                    case 0:
                        cache.put(key + 1, val);

                        break;

                    case 1:
                        cache.remove(key + 1);

                        break;

                    case 2:
                        cache.get(key + 1);

                        break;

                    case 3:
                        tx.commit();

                        break;

                    default:
                        fail();
                }

                fail("Tx must timeout");
            }
            catch (CacheException | IgniteException e) {
                assertTrue("Expected exception: " + e, X.hasCause(e, TransactionTimeoutException.class));
            }
        }

        assertFalse("Must be removed by rollback on timeout", near.cache(CACHE_NAME).containsKey(key));
        assertFalse("Must be removed by rollback on timeout", near.cache(CACHE_NAME).containsKey(key + 1));

        assertNull(near.transactions().tx());
    }

    /**
     * @param near Node.
     * @param mode Test mode.
     *
     * @param timeout Tx timeout.
     * @throws Exception If failed.
     */
    private void testTimeoutRemoval0(IgniteEx near, int mode, long timeout) throws Exception {
        Throwable saved = null;

        try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1)) {
            near.cache(CACHE_NAME).put(1, 1);

            switch (mode) {
                case 0:
                    tx.commit();
                    break;

                case 1:
                    tx.commitAsync().get();
                    break;

                case 2:
                    tx.rollback();
                    break;

                case 3:
                    tx.rollbackAsync().get();
                    break;

                case 4:
                    break;

                default:
                    fail();
            }
        }
        catch (Throwable t) {
            saved = t;
        }

        Collection set = U.field(near.context().cache().context().time(), "timeoutObjs");

        for (Object obj : set) {
            if (obj.getClass().isAssignableFrom(GridNearTxLocal.class)) {
                log.error("Last saved exception: " + saved, saved);

                fail("Not removed [mode=" + mode + ", timeout=" + timeout + ", tx=" + obj +']');
            }
        }
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @throws Exception If failed.
     */
    private void waitingTxUnblockedOnTimeout(final Ignite near, final Ignite other) throws Exception {
        waitingTxUnblockedOnTimeout(near, other, 1000);

        waitingTxUnblockedOnTimeout(near, other, 50);
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void waitingTxUnblockedOnTimeout(final Ignite near, final Ignite other, final long timeout) throws Exception {
        info("Start test [node1=" + near.name() + ", node2=" + other.name() + ']');

        final CountDownLatch blocked = new CountDownLatch(1);

        final CountDownLatch unblocked = new CountDownLatch(1);

        final int recordsCnt = 5;

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 0)) {
                    try {
                        for (int i = 0; i < recordsCnt; i++)
                            near.cache(CACHE_NAME).put(i, i);

                        info("Locked all keys.");
                    }
                    catch (CacheException e) {
                        info("Failed to lock keys: " + e);
                    }
                    finally {
                        blocked.countDown();
                    }

                    // Will be unblocked after tx timeout occurs.
                    U.awaitQuiet(unblocked);

                    try {
                        near.cache(CACHE_NAME).put(0, 0);

                        fail();
                    }
                    catch (CacheException e) {
                        log.info("Expecting error: " + e.getMessage());
                    }

                    try {
                        tx.commit();

                        fail();
                    }
                    catch (IgniteException e) {
                        log.info("Expecting error: " + e.getMessage());
                    }
                }

                // Check thread is able to start new tx.
                try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 60_000, 0)) {
                    for (int i = 0; i < recordsCnt; i++)
                        near.cache(CACHE_NAME).put(i, i);

                    tx.commit();
                }
            }
        }, "First");

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(blocked);

                try (Transaction tx = other.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, "Second");

        fut2.get();

        unblocked.countDown();

        fut1.get();
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @throws Exception If failed.
     */
    private void waitingTxUnblockedOnThreadDeath(final Ignite near, final Ignite other) throws Exception {
        waitingTxUnblockedOnThreadDeath0(near, other, 10, 1000); // Try provoke timeout after all keys are locked.

        waitingTxUnblockedOnThreadDeath0(near, other, 1000, 100);  // Try provoke timeout while trying to lock keys.
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @param recordsCnt Number of records to locks.
     * @param timeout Transaction timeout.
     * @throws Exception If failed.
     */
    private void waitingTxUnblockedOnThreadDeath0(final Ignite near,
        final Ignite other,
        final int recordsCnt,
        final long timeout)
        throws Exception
    {
        info("Start test [node1=" + near.name() + ", node2=" + other.name() + ']');

        final CountDownLatch blocked = new CountDownLatch(1);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, recordsCnt);

                try {
                    for (int i = 0; i < recordsCnt; i++)
                        near.cache(CACHE_NAME).put(i, i);

                    log.info("Locked all records.");
                }
                catch (Exception e) {
                    log.info("Failed to locked all records: " + e);
                }
                finally {
                    blocked.countDown();
                }

                throw new IgniteException("Failure");
            }
        }, 1, "First");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(blocked);

                try (Transaction tx = other.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, recordsCnt)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, 1, "Second");

        try {
            fut1.get();

            fail();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }

        fut2.get();
    }
}
