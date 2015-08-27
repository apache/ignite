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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.util.*;
import org.eclipse.jetty.util.*;

import java.util.*;
import java.util.concurrent.*;

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

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(backupCnt);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
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
     *
     */
    public void testPrimaryDead() throws Exception {
        startGrid(0);
        startGrid(1);

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
        catch (IgniteFutureTimeoutCheckedException e) {
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
