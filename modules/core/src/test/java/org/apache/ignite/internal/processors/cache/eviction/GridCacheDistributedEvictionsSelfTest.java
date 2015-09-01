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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
public class GridCacheDistributedEvictionsSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private int gridCnt = 2;

    /** */
    private CacheMode mode;

    /** */
    private boolean nearEnabled;

    /** */
    private boolean evictSync;

    /** */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TransactionConfiguration tCfg = new TransactionConfiguration();

        tCfg.setDefaultTxConcurrency(PESSIMISTIC);
        tCfg.setDefaultTxIsolation(READ_COMMITTED);

        c.setTransactionConfiguration(tCfg);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setAtomicityMode(TRANSACTIONAL);

        if (nearEnabled) {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            cc.setNearConfiguration(nearCfg);
        }

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        // Set only DHT policy, leave default near policy.
        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(10);

        cc.setEvictionPolicy(plc);
        cc.setEvictSynchronized(evictSync);
        cc.setEvictSynchronizedKeyBufferSize(1);

        cc.setAffinity(new GridCacheModuloAffinityFunction(gridCnt, 1));

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, idxGen.getAndIncrement()));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Throwable If failed. */
    public void testNearSyncBackupSync() throws Throwable {
        gridCnt = 3;
        mode = PARTITIONED;
        evictSync = true;
        nearEnabled = true;

        checkEvictions();
    }

    /**
     * @throws Throwable If failed.
     */
    public void testLocalSync() throws Throwable {
        gridCnt = 1;
        mode = LOCAL;
        evictSync = true;
        nearEnabled = true;

        Ignite g = startGrid(0);

        final IgniteCache<Integer, Integer> cache = g.cache(null);

        for (int i = 1; i < 20; i++) {
            cache.put(i * gridCnt, i * gridCnt);

            info("Put to cache: " + i * gridCnt);
        }
    }

    /** @throws Throwable If failed. */
    private void checkEvictions() throws Throwable {
        try {
            startGrids(gridCnt);

            Ignite ignite = grid(0);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            // Put 1 entry to primary node.
            cache.put(0, 0);

            Integer nearVal = this.<Integer, Integer>jcache(2).get(0);

            assert nearVal == 0 : "Unexpected near value: " + nearVal;

            // Put several vals to primary node.
            for (int i = 1; i < 20; i++) {
                cache.put(i * gridCnt, i * gridCnt);

                info("Put to cache: " + i * gridCnt);
            }

            for (int i = 0; i < 3; i++) {
                try {
                    assert jcache(2).get(0) == null : "Entry has not been evicted from near node for key: " + 0;
                    assert jcache(1).get(0) == null : "Entry has not been evicted from backup node for key: " + 0;
                    assert cache.get(0) == null : "Entry has not been evicted from primary node for key: " + 0;
                }
                catch (Throwable e) {
                    if (i == 2)
                        // No attempts left.
                        throw e;

                    U.warn(log, "Check failed (will retry in 2000 ms): " + e);

                    // Unwind evicts?
                    cache.get(0);

                    U.sleep(2000);
                }
            }

            for (int i = 0; i < 3; i++) {
                info("Primary key set: " + new TreeSet<>(this.<Integer, Integer>dht(0).keySet()));
                info("Primary near key set: " + new TreeSet<>(this.<Integer, Integer>near(0).keySet()));

                info("Backup key set: " + new TreeSet<>(this.<Integer, Integer>dht(1).keySet()));
                info("Backup near key set: " + new TreeSet<>(this.<Integer, Integer>near(1).keySet()));

                info("Near key set: " + new TreeSet<>(this.<Integer, Integer>dht(2).keySet()));
                info("Near node near key set: " + new TreeSet<>(this.<Integer, Integer>near(2).keySet()));

                try {
                    assert cache.localSize() == 10 : "Invalid cache size [size=" + cache.localSize() + ']';
                    assert cache.localSize() == 10 : "Invalid key size [size=" + cache.localSize() + ']';

                    assert jcache(2).localSize() == 0;

                    break;
                }
                catch (Throwable e) {
                    if (i == 2)
                        // No attempts left.
                        throw e;

                    U.warn(log, "Check failed (will retry in 2000 ms): " + e);

                    // Unwind evicts?
                    cache.get(0);

                    U.sleep(2000);
                }
            }
        }
        catch (Throwable t) {
            error("Test failed.", t);

            throw t;
        }
        finally {
            stopAllGrids();
        }
    }
}