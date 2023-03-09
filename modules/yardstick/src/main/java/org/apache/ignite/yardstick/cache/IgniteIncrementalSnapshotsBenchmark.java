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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/** Starts incremental snapshots concurrently with transaction load. */
public class IgniteIncrementalSnapshotsBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Full snapshot name. */
    private static final String SNP = "testSnapshot";

    /** Coordinate threads to start incremental snapshot. */
    private volatile AtomicBoolean busyLock;

    /** Schedules time for next incremental snapshot creation. */
    private volatile long nextStartTime;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        // Choose single node to coordinate snapshot creation.
        IgniteAtomicReference<Boolean> snpCrdNode = ignite().atomicReference("snpCrdNodeChoosed", false, true);

        if (snpCrdNode.compareAndSet(false, true)) {
            BenchmarkUtils.println("This node will coordinate snapshots creation.");

            ignite().snapshot().createSnapshot(SNP).get();

            nextStartTime = System.currentTimeMillis();

            busyLock = new AtomicBoolean();
        }
        else
            BenchmarkUtils.println("This node wasn't choosed to coordinate snapshots creation.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (busyLock != null && System.currentTimeMillis() > nextStartTime && busyLock.compareAndSet(false, true))
            createIncrementalSnapshot(args.getIntParameter("incSnpPeriod", 60 * 1_000));

        try (Transaction tx = ignite().transactions().txStart(args.txConcurrency(), args.txIsolation())) {
            for (int i = 0; i < args.scaleFactor(); i++)
                cache().put(ThreadLocalRandom.current().nextInt(), 0);

            tx.commit();
        }

        return true;
    }

    /** Creates incremental snapshot. */
    private void createIncrementalSnapshot(long incSnpPeriod) {
        nextStartTime = System.currentTimeMillis() + incSnpPeriod;

        ignite().snapshot().createIncrementalSnapshot(SNP).listen((snpFut) -> {
            try {
                snpFut.get();

                BenchmarkUtils.println("Incremental snapshot succeed");
            }
            catch (Throwable th) {
                BenchmarkUtils.error("Incremental snapshot failed", th);
            }

            busyLock.set(false);
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(new CacheConfiguration<Integer, Object>()
            .setName("testTxCache")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }
}
