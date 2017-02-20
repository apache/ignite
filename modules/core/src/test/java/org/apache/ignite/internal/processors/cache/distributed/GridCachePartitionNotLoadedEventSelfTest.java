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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.TestTcpCommunicationSpi;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCachePartitionNotLoadedEventSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private int backupCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        if (gridName.matches(".*\\d")) {
            String idStr = UUID.randomUUID().toString();

            char[] chars = idStr.toCharArray();

            chars[chars.length - 3] = '0';
            chars[chars.length - 2] = '0';
            chars[chars.length - 1] = gridName.charAt(gridName.length() - 1);

            cfg.setNodeId(UUID.fromString(new String(chars)));
        }

        cfg.setCommunicationSpi(new TestTcpCommunicationSpi());

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backupCnt);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryAndBackupDead() throws Exception {
        backupCnt = 1;

        startGrid(0);
        startGrid(1);
        startGrid(2);

        final PartitionNotFullyLoadedListener lsnr = new PartitionNotFullyLoadedListener();

        ignite(2).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        Affinity<Integer> aff = ignite(0).affinity(null);

        int key = 0;

        while (!aff.isPrimary(ignite(0).cluster().localNode(), key)
            || !aff.isBackup(ignite(1).cluster().localNode(), key))
            key++;

        IgniteCache<Integer, Integer> cache = jcache(2);

        cache.put(key, key);

        assert jcache(0).containsKey(key);
        assert jcache(1).containsKey(key);

        TestTcpCommunicationSpi.stop(ignite(0));
        TestTcpCommunicationSpi.stop(ignite(1));

        stopGrid(0, true);
        stopGrid(1, true);

        awaitPartitionMapExchange();

        assert !cache.containsKey(key);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !lsnr.lostParts.isEmpty();
            }
        }, getTestTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryDead() throws Exception {
        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        final PartitionNotFullyLoadedListener lsnr = new PartitionNotFullyLoadedListener();

        ignite(1).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        int key = primaryKey(jcache(0));

        jcache(1).put(key, key);

        assert jcache(0).containsKey(key);

        TestTcpCommunicationSpi.stop(ignite(0));

        stopGrid(0, true);

        awaitPartitionMapExchange();

        assert !jcache(1).containsKey(key);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !lsnr.lostParts.isEmpty();
            }
        }, getTestTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStableTopology() throws Exception {
        backupCnt = 1;

        startGrid(1);

        awaitPartitionMapExchange();

        startGrid(0);

        final PartitionNotFullyLoadedListener lsnr = new PartitionNotFullyLoadedListener();

        grid(1).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        int key = primaryKey(cache0);

        jcache(1).put(key, key);

        assert cache0.containsKey(key);

        TestTcpCommunicationSpi.stop(ignite(0));

        stopGrid(0, true);

        awaitPartitionMapExchange();

        assert jcache(1).containsKey(key);

        assert lsnr.lostParts.isEmpty();
    }


    /**
     * @throws Exception If failed.
     */
    public void testMapPartitioned() throws Exception {
        backupCnt = 0;

        startGrid(0);

        startGrid(1);

        final PartitionNotFullyLoadedListener lsnr = new PartitionNotFullyLoadedListener();

        grid(1).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        TestTcpCommunicationSpi.skipMsgType(ignite(0), GridDhtPartitionsFullMessage.class);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        });

        boolean timeout = false;

        try {
            fut.get(1, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException ignored) {
            timeout = true;
        }

        assert timeout;

        stopGrid(0, true);

        awaitPartitionMapExchange();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !lsnr.lostParts.isEmpty();
            }
        }, getTestTimeout());
    }

    /**
     *
     */
    private static class PartitionNotFullyLoadedListener implements IgnitePredicate<Event> {
        /** */
        private Collection<Integer> lostParts = new ConcurrentHashSet<>();

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            lostParts.add(((CacheRebalancingEvent)evt).partition());

            return true;
        }
    }
}
