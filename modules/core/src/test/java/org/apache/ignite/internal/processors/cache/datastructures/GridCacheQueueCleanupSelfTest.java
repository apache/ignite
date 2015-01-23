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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests cleanup of orphaned queue items.
 */
public class GridCacheQueueCleanupSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String QUEUE_NAME1 = "CleanupTestQueue1";

    /** */
    private static final String QUEUE_NAME2 = "CleanupTestQueue2";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCleanup() throws Exception {
        GridCacheQueue<Integer> queue = cache().dataStructures().queue(QUEUE_NAME1, 0, false, true);

        ClusterNode node = grid(0).cache(null).affinity().mapKeyToNode(new GridCacheQueueHeaderKey(QUEUE_NAME1));

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

        final String killGridName = node.attribute(GridNodeAttributes.ATTR_GRID_NAME);

        stopGrid(killGridName);

        assertNull(grid.cache(null).dataStructures().queue(QUEUE_NAME1, 0, false, false));

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

        grid.cache(null).dataStructures().removeQueue(QUEUE_NAME1);
        grid.cache(null).dataStructures().removeQueue(QUEUE_NAME2);

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
        queue = grid.cache(null).dataStructures().queue(QUEUE_NAME1, 0, false, true);
        */

        assertEquals(0, queue.size());

        for (int i = 0; i < 500; i++)
            queue.add(i);

        assertEquals(500, queue.size());

        // Remove queue and create queue with the same name.
        ignite.cache(null).dataStructures().removeQueue(QUEUE_NAME1);

        queue = ignite.cache(null).dataStructures().queue(QUEUE_NAME1, 0, false, true);

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
                    Iterator<GridCacheEntryEx<Object, Object>> entries =
                        ((GridKernal)grid(i)).context().cache().internalCache().map().allEntries0().iterator();

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
    private IgniteFuture<?> startAddPollThread(final Ignite ignite, final AtomicBoolean stop, final String queueName) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                GridCacheQueue<Integer> queue = ignite.cache(null).dataStructures().queue(queueName, 0, false, true);

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
