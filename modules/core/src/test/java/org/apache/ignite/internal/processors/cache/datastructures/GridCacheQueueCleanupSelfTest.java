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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueHeaderKey;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests cleanup of orphaned queue items.
 */
public class GridCacheQueueCleanupSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final String QUEUE_NAME1 = "CleanupTestQueue1";

    /** */
    private static final String QUEUE_NAME2 = "CleanupTestQueue2";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
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
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration colCfg = super.collectionConfiguration();

        colCfg.setBackups(0);

        return colCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCleanup() throws Exception {
        IgniteQueue<Integer> queue = grid(0).queue(QUEUE_NAME1, 0, config(false));

        GridCacheContext cctx = GridTestUtils.getFieldValue(queue, "cctx");

        final String queueCacheName = cctx.name();

        ClusterNode node = grid(0).affinity(queueCacheName).mapKeyToNode(new GridCacheQueueHeaderKey(QUEUE_NAME1));

        final Ignite ignite = grid(0).localNode().equals(node) ? grid(1) : grid(0);

        /*
        assertNotNull(queue);

        // Add/poll some items.

        for (int i = 0; i < 500; i++)
            queue.add(i);

        for (int i = 0; i < 10; i++)
            queue.poll();

        assertTrue(!queue.isEmpty());

        // Kill node containing queue header.

        final String killGridName = node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME);

        stopGrid(killGridName);

        assertNull(((IgniteKernal)grid).cache(null).dataStructures().queue(QUEUE_NAME1, 0, false, false));

        final AtomicBoolean stop = new AtomicBoolean(false);

        GridFuture<?> fut1;
        GridFuture<?> fut2;

        try {
            // Start threads using cache concurrently with cleanup thread.
            fut1 = startAddPollThread(grid, stop, QUEUE_NAME1);
            fut2 = startAddPollThread(grid, stop, QUEUE_NAME2);

            U.sleep(3000); // Give some time for cleanup thread.
        }
        finally {
            stop.set(true);
        }

        fut1.get();
        fut2.get();

        ((IgniteKernal)grid).cache(null).dataStructures().removeQueue(QUEUE_NAME1);
        ((IgniteKernal)grid).cache(null).dataStructures().removeQueue(QUEUE_NAME2);

        assertTrue(GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() {
                for (int i = 0; i < gridCount(); i++) {
                    if (getTestGridName(i).equals(killGridName))
                        continue;

                    Iterator<GridCacheEntryEx<Object, Object>> entries =
                        ((GridKernal)grid(i)).context().cache().internalCache().map().allEntries0().iterator();

                    if (entries.hasNext()) {
                        log.info("Found cache entries, will wait: " + entries.next());

                        return false;
                    }
                }

                return true;
            }
        }, 5000));

        startGrid(killGridName);

        // Create queue again.
        queue = ((IgniteKernal)grid).cache(null).dataStructures().queue(QUEUE_NAME1, 0, false, true);
        */

        assertEquals(0, queue.size());

        for (int i = 0; i < 500; i++)
            queue.add(i);

        assertEquals(500, queue.size());

        // Remove queue and create queue with the same name.
        queue.close();

        queue = ignite.queue(QUEUE_NAME1, 0, config(false));

        assertEquals(0, queue.size());

        for (int i = 0; i < 500; i++)
            queue.add(i);

        assertEquals(500, queue.size());

        // Check that items of removed queue are removed, items of new queue not.
        assertTrue(GridTestUtils.waitForCondition(new PAX() {
            @SuppressWarnings("WhileLoopReplaceableByForEach")
            @Override public boolean applyx() {
                int cnt = 0;

                for (int i = 0; i < gridCount(); i++) {
                    GridCacheAdapter<Object, Object> cache =
                        ((IgniteKernal)grid(i)).context().cache().internalCache(queueCacheName);

                    Iterator<GridCacheEntryEx> entries = cache.map().allEntries0().iterator();

                    while (entries.hasNext()) {
                        cnt++;

                        entries.next();
                    }
                }

                if (cnt > 501) { // 500 items + header.
                    log.info("Found more cache entries than expected, will wait: " + cnt);

                    return false;
                }

                return true;
            }
        }, 5000));

        for (int i = 0; i < 500; i++)
            assertEquals((Integer)i, queue.poll());
    }

    /**
     * @param ignite Grid.
     * @param stop Stop flag.
     * @param queueName Queue name.
     * @return Future completing when thread finishes.
     */
    private IgniteInternalFuture<?> startAddPollThread(final Ignite ignite, final AtomicBoolean stop, final String queueName) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteQueue<Integer> queue = ignite.queue(queueName, 0, config(false));

                assertEquals(0, queue.size());

                for (int i = 0; i < 10; i++)
                    assertTrue(queue.add(i));

                while (!stop.get()) {
                    for (int i = 0; i < 100; i++)
                        assertTrue(queue.add(i));

                    for (int i = 0; i < 100; i++)
                        assertNotNull(queue.poll());
                }

                assertEquals(10, queue.size());

                return null;
            }
        });
    }
}