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

import org.apache.commons.collections.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeAbstractSelfTest.*;

/**
 * Consistency test for cache queue in multi node environment.
 */
public class GridCacheQueueMultiNodeConsistencySelfTest extends IgniteCollectionAbstractTest {
    /** */
    protected static final int GRID_CNT = 3;

    /** */
    protected static final int RETRIES = 20;

    /** */
    private static final int PRELOAD_DELAY = 200;

    /** Indicates whether force repartitioning is needed or not. */
    private boolean forceRepartition;

    /** Indicates whether random grid stopping is needed or not. */
    private boolean stopRandomGrid;

    /** */
    private int backups;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
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
    @Override protected TestCollectionConfiguration collectionConfiguration() {
        TestCollectionConfiguration colCfg = super.collectionConfiguration();

        colCfg.setBackups(backups);

        return colCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfBackupDisabled() throws Exception {
        backups = 0;

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfNoPreloadingAndBackupDisabledAndRepartitionForced() throws Exception {
        backups = 0;

        forceRepartition = true;

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfBackupEnabled() throws Exception {
        backups = 1;

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfBackupEnabledAndOneNodeIsKilled() throws Exception {
        backups = 1;

        stopRandomGrid = true;

        checkCacheQueue();
    }

    /**
     * Starts {@code GRID_CNT} nodes, broadcasts {@code AddAllJob} to them then starts new grid and
     * reads cache queue content and finally asserts queue content is the same.
     *
     * @throws Exception If failed.
     */
    private void checkCacheQueue() throws Exception {
        startGrids(GRID_CNT);

        final String queueName = UUID.randomUUID().toString();

        IgniteQueue<Integer> queue0 = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        assertTrue(queue0.isEmpty());

        grid(0).compute().broadcast(new AddAllJob(queueName, RETRIES));

        assertEquals(GRID_CNT * RETRIES, queue0.size());

        if (stopRandomGrid)
            stopGrid(1 + new Random().nextInt(GRID_CNT));

        if (forceRepartition)
            for (int i = 0; i < GRID_CNT; i++)
                jcache(i).rebalance();

        Ignite newIgnite = startGrid(GRID_CNT + 1);

        // Intentionally commented code cause in this way inconsistent queue problem doesn't appear.
        // IgniteQueue<Integer> newQueue = newGrid.cache().queue(queueName);
        // assertTrue(CollectionUtils.isEqualCollection(queue0, newQueue));

        Collection<Integer> locQueueContent = compute(newIgnite.cluster().forLocal()).call(new IgniteCallable<Collection<Integer>>() {
            @IgniteInstanceResource
            private Ignite grid;

            /** {@inheritDoc} */
            @Override public Collection<Integer> call() throws Exception {
                Collection<Integer> values = new ArrayList<>();

                grid.log().info("Running job [node=" + grid.cluster().localNode().id() + ", job=" + this + "]");

                IgniteQueue<Integer> locQueue = grid.queue(queueName, QUEUE_CAPACITY, config(false));

                grid.log().info("Queue size " + locQueue.size());

                for (Integer element : locQueue)
                    values.add(element);

                return values;
            }
        });

        assertTrue(CollectionUtils.isEqualCollection(queue0, locQueueContent));

        queue0.close();
    }
}
