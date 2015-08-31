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
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCollectionAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCachePartitionedQueueCreateMultiNodeSelfTest extends IgniteCollectionAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-80");
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheMemoryMode collectionMemoryMode() {
        return ONHEAP_TIERED;
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

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
    public void testQueueCreation() throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int idx0 = idx.getAndIncrement();

                    Thread.currentThread().setName("createQueue-" + idx0);

                    Ignite ignite = startGrid(idx0);

                    UUID locNodeId = ignite.cluster().localNode().id();

                    info("Started grid: " + locNodeId);

                    info("Creating queue: " + locNodeId);

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

                    IgniteCache<Integer, String> cache = ignite.cache(null);

                    info("Partition: " + ignite.affinity(null).partition(1));

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