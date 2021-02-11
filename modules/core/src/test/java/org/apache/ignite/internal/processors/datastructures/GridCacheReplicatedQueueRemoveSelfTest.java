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

package org.apache.ignite.internal.processors.datastructures;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCollectionAbstractTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for Ignite Queue remove method.
 */
public class GridCacheReplicatedQueueRemoveSelfTest extends IgniteCollectionAbstractTest {
    /** */
    public static final int CACHE_SIZE = 1_000;

    /** */
    public static final int THREADS_CNT = 8;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * This a unit test of Ignite queue's RemoveProcessor process() method.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testQueueRemovalProcessor() throws Exception {
        GridCacheContext cctx = grid(0).context().cache().cache("ignite-sys-cache").context();

        IgniteUuid id = IgniteUuid.randomUuid();

        CacheInvokeEntry entry = new CacheInvokeEntry<>(null, null, null, false,
            new GridDhtCacheEntry(cctx, null,
                new KeyCacheObjectImpl(1, BigInteger.valueOf(1).toByteArray(), 1)));

        entry.setValue(new GridCacheQueueHeader(
            id,
            2147483647,
            false,
            0L,
            10000L,
            Collections.singleton(1L)));

        GridCacheQueueAdapter.RemoveProcessor rp = new GridCacheQueueAdapter.RemoveProcessor(id, 1L);

        rp.process(entry);

        GridCacheQueueAdapter.RemoveProcessor externalRP = new GridCacheQueueAdapter.RemoveProcessor();

        GridByteArrayOutputStream output = new GridByteArrayOutputStream();

        rp.writeExternal(new ObjectOutputStream(output));

        externalRP.readExternal(new ObjectInputStream(new GridByteArrayInputStream(output.toByteArray())));

        assertEquals(id, GridTestUtils.getFieldValue(externalRP, "id"));

        // idx should be null, cause entry was already removed, see GridCacheQueueAdapter.RemoveProcessor.code
        // for more details.
        assertNull(GridTestUtils.getFieldValue(externalRP, "idx"));
    }

    /**
     * Removes buckets of data from the queue in many threads at the same time.
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testQueueRemoval() throws Exception {
        IgniteQueue queue = grid(0).queue("SomeQueue", 0,
            new CollectionConfiguration().setCollocated(true).setCacheMode(REPLICATED).setAtomicityMode(TRANSACTIONAL));

        // Populate queue with some data.
        for (int i = 0; i < CACHE_SIZE; i++)
            queue.add(i);

        CountDownLatch latch = new CountDownLatch(THREADS_CNT);

        // Remove buckets of data from the queue in many threads at the same time.
        GridTestUtils.runMultiThreaded(() -> {
            ArrayList dataToRmv = new ArrayList();

            for (int i = 0; i < CACHE_SIZE / 10; i++)
                dataToRmv.add(ThreadLocalRandom.current().nextInt(CACHE_SIZE));

            latch.countDown();
            latch.await();

            queue.removeAll(dataToRmv);

            return null;
        }, THREADS_CNT, "queue-test-worker");
    }
}
