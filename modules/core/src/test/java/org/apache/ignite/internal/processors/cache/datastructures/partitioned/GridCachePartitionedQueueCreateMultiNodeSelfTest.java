/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCollectionAbstractTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCachePartitionedQueueCreateMultiNodeSelfTest extends IgniteCollectionAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setIncludeEventTypes();
        c.setPeerClassLoadingEnabled(false);

        CacheConfiguration[] ccfg = c.getCacheConfiguration();

        if (ccfg != null) {
            assert ccfg.length == 1 : ccfg.length;

            c.setCacheConfiguration(ccfg[0], cacheConfiguration());
        }
        else
            c.setCacheConfiguration(cacheConfiguration());

        return c;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setBackups(0);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueCreation() throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int idx0 = idx.getAndIncrement();

                    Thread.currentThread().setName("createQueue-" + idx0);

                    final Ignite ignite = startGrid(idx0);

                    UUID locNodeId = ignite.cluster().localNode().id();

                    info("Started grid: " + locNodeId);

                    info("Creating queue: " + locNodeId);

                    GridTestUtils.runMultiThreaded(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            ignite.queue("queue", 1, config(true));

                            return null;
                        }
                    }, 10, "create-queue-" + ignite.name());

                    IgniteQueue<String> q = ignite.queue("queue", 1, config(true));

                    assert q != null;

                    info("Putting first value: " + locNodeId);

                    q.offer("val", 1000, MILLISECONDS);

                    info("Putting second value: " + locNodeId);

                    boolean res2 = q.offer("val1", 1000, MILLISECONDS);

                    assert !res2;

                    info("Thread finished: " + locNodeId);

                    return null;
                }
            },
            10
        );

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx() throws Exception {
        if (cacheConfiguration().getAtomicityMode() != TRANSACTIONAL)
            return;

        int threadCnt = 10;

        final AtomicInteger idx = new AtomicInteger();
        final AtomicBoolean flag = new AtomicBoolean();

        final CountDownLatch latch = new CountDownLatch(threadCnt);

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Ignite ignite = startGrid(idx.getAndIncrement());

                    boolean wait = false;

                    if (wait) {
                        latch.countDown();

                        latch.await();
                    }

                    // If output presents, test passes with greater probability.
                    // info("Start puts.");

                    IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    info("Partition: " + ignite.affinity(DEFAULT_CACHE_NAME).partition(1));

                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        // info("Getting value for key 1");

                        String s = cache.get(1);

                        // info("Got value: " + s);

                        if (s == null) {
                            assert flag.compareAndSet(false, true);

                            // info("Putting value.");

                            cache.put(1, "val");

                            // info("Done putting value");

                            tx.commit();
                        }
                        else
                            assert "val".equals(s) : "String: " + s;
                    }

                    info("Thread finished for grid: " + ignite.name());

                    return null;
                }
            },
            threadCnt
        );

        fut.get();
    }
}
