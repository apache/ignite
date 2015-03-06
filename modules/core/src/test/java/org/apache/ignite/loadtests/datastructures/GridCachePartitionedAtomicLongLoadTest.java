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

package org.apache.ignite.loadtests.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Load test for atomic long.
 */
public class GridCachePartitionedAtomicLongLoadTest extends GridCommonAbstractTest {
    /** Test duration. */
    private static final long DURATION = 8 * 60 * 60 * 1000;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger idx = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(PARTITIONED);
        atomicCfg.setBackups(1);
        atomicCfg.setAtomicSequenceReserveSize(10);

        c.setAtomicConfiguration(atomicCfg);

        c.getTransactionConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(REPEATABLE_READ);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setStartSize(200);
        cc.setRebalanceMode(CacheRebalanceMode.SYNC);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setEvictionPolicy(new CacheLruEvictionPolicy<>(1000));
        cc.setBackups(1);
        cc.setAffinity(new CacheRendezvousAffinityFunction(true));
        cc.setEvictSynchronized(true);
        cc.setEvictNearSynchronized(true);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoad() throws Exception {
        startGrid();

        try {
            multithreaded(new AtomicCallable(), 50);
        }
        finally {
            stopGrid();
        }
    }

    /**
     *
     */
    private class AtomicCallable implements Callable<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            Ignite ignite = grid();

            IgniteCache cache = ignite.jcache(null);

            assert cache != null;

            IgniteAtomicSequence seq = ignite.atomicSequence("SEQUENCE", 0, true);

            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < DURATION && !Thread.currentThread().isInterrupted()) {
                Transaction tx = ignite.transactions().txStart();

                long seqVal = seq.incrementAndGet();

                int curIdx = idx.incrementAndGet();

                if (curIdx % 1000 == 0)
                    info("Sequence value [seq=" + seqVal + ", idx=" + curIdx + ']');

                tx.commit();
            }

            return true;
        }
    }
}
