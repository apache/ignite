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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Ignore;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Test partitions consistency for atomic caches trying to reuse tx scenarios as much as possible.
 */
public class AtomicPartitionCounterStateConsistencyTest extends TxPartitionCounterStateConsistencyTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        return super.cacheConfiguration(name).setAtomicityMode(ATOMIC);
    }

    /** {@inheritDoc} */
    @Ignore
    @Override public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_SameAffinityPME() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Ignore
    @Override public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_TxDuringPME() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Ignore
    @Override public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_LateAffinitySwitch() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> primaryKeys,
        IgniteCache<Object, Object> cache, BooleanSupplier stopClo) throws Exception {
        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        final int max = 100;

        return multithreadedAsync(() -> {
            while (!stopClo.getAsBoolean()) {
                int rangeStart = r.nextInt(primaryKeys.size() - max);
                int range = 5 + r.nextInt(max - 5);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                final boolean batch = r.nextBoolean();

                try {
                    List<Integer> insertedKeys = new ArrayList<>();
                    List<Integer> rmvKeys = new ArrayList<>();

                    for (Integer key : keys) {
                        if (!batch)
                            cache.put(key, key);

                        insertedKeys.add(key);

                        puts.increment();

                        boolean rmv = r.nextFloat() < 0.5;
                        if (rmv) {
                            key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                            if (!batch)
                                cache.remove(key);
                            else
                                rmvKeys.add(key);

                            removes.increment();
                        }
                    }

                    if (batch) {
                        cache.putAll(insertedKeys.stream().collect(toMap(k -> k, v -> v, (k, v) -> v, LinkedHashMap::new)));

                        Collections.sort(rmvKeys);

                        cache.removeAll(new LinkedHashSet<>(rmvKeys));
                    }
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                        X.hasCause(e, ClusterTopologyCheckedException.class) ||
                        X.hasCause(e, CacheInvalidStateException.class));
                }
            }

            log.info("ATOMIC: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        }, Runtime.getRuntime().availableProcessors() * 2, "tx-update-thread");
    }
}
