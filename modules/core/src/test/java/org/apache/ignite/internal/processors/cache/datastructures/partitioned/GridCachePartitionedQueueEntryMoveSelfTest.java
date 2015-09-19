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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCollectionAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Cache queue test with changing topology.
 */
public class GridCachePartitionedQueueEntryMoveSelfTest extends IgniteCollectionAbstractTest {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 5;

    /** Grids count. */
    private static final int GRID_CNT = 4;

    /** Backups count. */
    private static final int BACKUP_CNT = 1;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
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

        colCfg.setBackups(BACKUP_CNT);

        return colCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueue() throws Exception {
        final String queueName = "queue-test-name";

        System.out.println(U.filler(20, '\n'));

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws IgniteInterruptedCheckedException {
                Ignite ignite = grid(0);

                IgniteQueue<Integer> queue = ignite.queue(queueName, QUEUE_CAP, config(true));

                for (int i = 0; i < QUEUE_CAP * 2; i++) {
                    if (i == QUEUE_CAP) {
                        latch1.countDown();

                        U.await(latch2);
                    }

                    try {
                        info(">>> Putting value: " + i);

                        queue.put(i);

                        info(">>> Value is in queue: " + i);
                    }
                    catch (Error | RuntimeException e) {
                        error("Failed to put value: " + i, e);

                        throw e;
                    }
                }

                return null;
            }
        });

        latch1.await();

        startAdditionalNodes(BACKUP_CNT + 2, queueName);

        System.out.println(U.filler(20, '\n'));

        latch2.countDown();

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws IgniteCheckedException {
                Ignite ignite = grid(GRID_CNT);

                IgniteQueue<Integer> queue = ignite.queue(queueName, QUEUE_CAP, config(true));

                int cnt = 0;

                do {
                    try {
                        Integer i = queue.poll();

                        if (i != null) {
                            info(">>> Polled value: " + cnt);

                            cnt++;
                        }
                        else {
                            info(">>> Waiting for value...");

                            U.sleep(2000);
                        }
                    }
                    catch (Error | RuntimeException e) {
                        error("Failed to poll value.", e);

                        throw e;
                    }
                }
                while (cnt < QUEUE_CAP * 2);

                return null;
            }
        });

        fut1.get();
        fut2.get();
    }

    /**
     * Start additional nodes above {@link #GRID_CNT}.
     *
     * @param cnt Number of additional nodes.
     * @param queueName Queue name.
     * @throws Exception If failed.
     */
    private void startAdditionalNodes(int cnt, String queueName) throws Exception {
        IgniteQueue queue = ignite(0).queue(queueName, 0, null);

        CacheConfiguration cCfg = getQueueCache(queue);

        Collection<ClusterNode> aff1 = ignite(0).affinity(cCfg.getName()).mapKeyToPrimaryAndBackups(queueName);

        for (int i = 0, id = GRID_CNT; i < cnt; i++) {
            startGrid(id++);

            awaitPartitionMapExchange();

            Collection<ClusterNode> aff2 = ignite(0).affinity(cCfg.getName()).mapKeyToPrimaryAndBackups(queueName);

            if (!aff1.iterator().next().equals(aff2.iterator().next())) {
                info("Moved queue to new primary node [oldAff=" + aff1 + ", newAff=" + aff2 + ']');

                return;
            }
        }

        throw new IgniteCheckedException("Unable to move the queue to a new primary node");
    }

    /**
     * @param aff Affinity function.
     * @param part Partition.
     * @param nodes Topology nodes.
     * @return Affinity nodes for partition.
     */
    private Collection<ClusterNode> nodes(AffinityFunction aff, int part, Collection<ClusterNode> nodes) {
        List<List<ClusterNode>> assignment = aff.assignPartitions(
            new GridAffinityFunctionContextImpl(new ArrayList<>(nodes), null, null, new AffinityTopologyVersion(1),
                BACKUP_CNT));

        return assignment.get(part);
    }
}