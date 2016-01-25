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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.DataLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.TestTcpCommunicationSpi;

/**
 * Tests {@link DataLossPolicy#FAIL_OPS}
 */
public class GridCacheDataLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    protected static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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
        cacheCfg.setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.getCacheConfiguration()[0].setDataLossPolicy(DataLossPolicy.FAIL_OPS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartLostReset() throws Exception {
        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        final GridCachePartitionNotLoadedEventSelfTest.PartitionNotFullyLoadedListener lsnr =
            new GridCachePartitionNotLoadedEventSelfTest.PartitionNotFullyLoadedListener();

        ignite(1).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        final int key = primaryKey(jcache(0));
        final int keyAfterLost = primaryKeys(jcache(0), 1, key + 1).get(0);
        final int keyAtFututre = primaryKeys(jcache(0), 1, keyAfterLost + 1).get(0);

        jcache(1).put(key, key);

        assert jcache(0).containsKey(key);

        TestTcpCommunicationSpi.stop(ignite(0));

        stopGrid(0, true);

        awaitPartitionMapExchange();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !lsnr.getLostParts().isEmpty();
            }
        }, getTestTimeout());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                jcache(1).put(key, key);
                return null;
            }
        }, IgniteCheckedException.class, "Failed to perform cache operation (lost partition)");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                jcache(1).containsKey(key);
                return null;
            }
        }, IgniteException.class, "Partition is lost");

        IgniteFuture fut = ignite(1).resetLostParts(Collections.<String>singleton(null));

        fut.listen(new IgniteInClosure<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> future) {
                jcache(1).put(keyAtFututre, keyAtFututre);
            }
        });

        fut.get(getTestTimeout());

        jcache(1).put(keyAfterLost, keyAfterLost);

        assert !jcache(1).containsKey(key);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return jcache(1).containsKey(keyAfterLost);
            }
        }, 10000);
    }

    /**
     * For case, when all nodes except client node have left the claster. Thus there is no an oldest node for the last
     * client node.
     *
     * @throws Exception If failed.
     */
    public void testClientNodeWithoutOldest() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration("testClientNodeWithoutOldest");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);
        cfg.setClientMode(true);
        disco.setForceServerMode(true);

        cfg.setCommunicationSpi(new TestTcpCommunicationSpi());

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.getCacheConfiguration()[0].setDataLossPolicy(DataLossPolicy.FAIL_OPS);

        Ignition.start(cfg);
    }
}