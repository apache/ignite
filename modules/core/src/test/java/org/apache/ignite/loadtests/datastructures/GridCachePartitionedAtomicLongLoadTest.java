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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(1000);

        cc.setEvictionPolicy(plc);
        cc.setBackups(1);
        cc.setAffinity(new RendezvousAffinityFunction(true));
        cc.setEvictSynchronized(true);

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

            IgniteCache cache = ignite.cache(null);

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