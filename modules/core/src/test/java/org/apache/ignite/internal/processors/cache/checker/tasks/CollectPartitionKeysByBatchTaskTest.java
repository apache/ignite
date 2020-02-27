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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.diagnostic.ReconciliationExecutionContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Tests that collecting by batch work fine.
 */
public class CollectPartitionKeysByBatchTaskTest extends CollectPartitionInfoAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(2));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that pre-filtrating works with batching.
     */
    @Test
    public void testShouldReduceAndReturnOnlyRecheckKeys() throws Exception {
        IgniteEx node = startGrid(0);

        node.cluster().active(true);

        AffinityTopologyVersion ver = lastTopologyVersion(node);

        CacheObjectContext ctxo = node.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheObjectContext();

        CollectPartitionKeysByBatchTask task = new CollectPartitionKeysByBatchTask();
        task.map(Collections.EMPTY_LIST, new PartitionBatchRequest(
            ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            DEFAULT_CACHE_NAME, 1, 1000, null, ver));
        Field igniteField = U.findField(task.getClass(), "ignite");
        igniteField.set(task, node);

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(new int[][] {{EMPTY}, {EMPTY}, {EMPTY}}, ctxo))
                .result();

            assertEquals(null, reduce.get1());
            assertEquals(0, reduce.get2().size());
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(new int[][] {{EMPTY}, {132}, {EMPTY}}, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(132), reduce.get1().value(null, false));
            assertEquals(1, rechecks.size());
            assertTrue(rechecks.containsKey(key(132, ctxo)));
            assertEquals(1, rechecks.get(key(132, ctxo)).size());
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(new int[][] {{500}, {500}, {EMPTY}}, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(500), reduce.get1().value(null, false));
            assertEquals(1, rechecks.size());
            assertTrue(rechecks.containsKey(key(500, ctxo)));
            assertEquals(2, rechecks.get(key(500, ctxo)).size());
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(new int[][] {{100}, {200}, {EMPTY}}, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(200), reduce.get1().value(null, false));
            assertEquals(2, rechecks.size());
            assertTrue(rechecks.containsKey(key(100, ctxo)));
            assertTrue(rechecks.containsKey(key(200, ctxo)));
            assertEquals(1, rechecks.get(key(100, ctxo)).size());
            assertEquals(1, rechecks.get(key(200, ctxo)).size());
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(new int[][] {{9}, {9}, {9}}, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(9), reduce.get1().value(null, false));
            assertEquals(0, rechecks.size());
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(valuesDiffVersion(new int[][] {{1}, {1}, {1}}, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(1), reduce.get1().value(null, false));
            assertEquals(1, rechecks.size());
            assertEquals(3, rechecks.get(key(1, ctxo)).size());
        }

        int[][] bigDataset = {
            {1, 2, 3, 6},
            {2, 3, 4, 6},
            {3, 4, 5, EMPTY}
        };

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(valuesDiffVersion(bigDataset, ctxo))
                .result();

            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> rechecks = reduce.get2();

            assertEquals(Integer.valueOf(6), reduce.get1().value(null, false));

            assertEquals(6, rechecks.size());
            assertEquals(1, rechecks.get(key(1, ctxo)).size()); // 1
            assertEquals(2, rechecks.get(key(2, ctxo)).size()); // 2, 2
            assertEquals(3, rechecks.get(key(3, ctxo)).size()); // 3, 3, 3
            assertEquals(2, rechecks.get(key(4, ctxo)).size()); // 4, 4
            assertEquals(1, rechecks.get(key(5, ctxo)).size()); // 5
            assertEquals(2, rechecks.get(key(6, ctxo)).size()); // 6, 6
        }

        {
            T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce = task
                .reduce(values(bigDataset, ctxo))
                .result();

            assertEquals(5, reduce.get2().size());
        }
    }

    /**
     * Checks that returns keys by batch.
     */
    @Test
    public void testShouldReturnKeysByBatches() throws Exception {
        IgniteEx node = startGrids(3);

        node.cluster().active(true);

        AffinityTopologyVersion ver = lastTopologyVersion(node);

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);
        List<Integer> keys = new ArrayList<>();

        int elements = 0;
        for (int i = 0; i < 100_000; i++) {
            if (node.affinity(DEFAULT_CACHE_NAME).partition(i) == FIRST_PARTITION) {
                cache.put(i, i);
                keys.add(i);

                elements++;
            }
        }

        for (Integer key : keys) {
            corruptDataEntry(
                node.cachex(DEFAULT_CACHE_NAME).context(),
                key
            );
        }

        Collection<ClusterNode> nodes = node.affinity(DEFAULT_CACHE_NAME)
            .mapPartitionToPrimaryAndBackups(FIRST_PARTITION);

        int batchSize = (elements + 1) / 2; // two batch for all rows

        Set<KeyCacheObject> fetched = new HashSet<>();

        T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> firstBatch = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                DEFAULT_CACHE_NAME, FIRST_PARTITION, batchSize, null, ver)
        ).result();

        fetched.addAll(firstBatch.get2().keySet());

        KeyCacheObject firstMaxKey = firstBatch.get1();

        T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> secondBatch = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                DEFAULT_CACHE_NAME, FIRST_PARTITION, batchSize, firstMaxKey, ver)
        ).result();

        KeyCacheObject secondMaxKey = secondBatch.get1();

        fetched.addAll(secondBatch.get2().keySet());

        T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> thirdBatch = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                DEFAULT_CACHE_NAME, FIRST_PARTITION, batchSize, secondMaxKey, ver)
        ).result();

        assertNull(thirdBatch.get1());
        assertEquals(0, thirdBatch.get2().size());

        assertEquals(elements, fetched.size());
    }
}
