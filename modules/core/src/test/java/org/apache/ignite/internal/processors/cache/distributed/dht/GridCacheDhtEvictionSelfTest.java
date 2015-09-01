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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 * Tests for dht cache eviction.
 */
public class GridCacheDhtEvictionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Default constructor. */
    public GridCacheDhtEvictionSelfTest() {
        super(false /* don't start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceMode(NONE);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setBackups(1);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        FifoEvictionPolicy nearPlc = new FifoEvictionPolicy();
        nearPlc.setMaxSize(10000);

        nearCfg.setNearEvictionPolicy(nearPlc);
        cacheCfg.setNearConfiguration(nearCfg);

        // Set eviction queue size explicitly.
        cacheCfg.setEvictMaxOverflowRatio(0);
        cacheCfg.setEvictSynchronizedKeyBufferSize(1);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(10000);

        cacheCfg.setEvictionPolicy(plc);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (GRID_CNT < 2)
            throw new IgniteCheckedException("GRID_CNT must not be less than 2.");

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            assert near(grid(i)).size() == 0;
            assert dht(grid(i)).size() == 0;

            assert near(grid(i)).isEmpty();
            assert dht(grid(i)).isEmpty();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            near(grid(i)).removeAll();

            assert near(grid(i)).isEmpty() : "Near cache is not empty [idx=" + i + "]";
            assert dht(grid(i)).isEmpty() : "Dht cache is not empty [idx=" + i + "]";
        }
    }

    /**
     * @param g Grid.
     * @return Near cache.
     */
    @SuppressWarnings({"unchecked"})
    private GridNearCacheAdapter<Integer, String> near(Ignite g) {
        return (GridNearCacheAdapter)((IgniteKernal)g).internalCache();
    }

    /**
     * @param g Grid.
     * @return Dht cache.
     */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    private GridDhtCacheAdapter<Integer, String> dht(Ignite g) {
        return ((GridNearCacheAdapter)((IgniteKernal)g).internalCache()).dht();
    }

    /**
     * @param key Key.
     * @return Primary node for the given key.
     */
    private Collection<ClusterNode> keyNodes(Object key) {
        return grid(0).affinity(null).mapKeyToPrimaryAndBackups(key);
    }

    /**
     * @param nodeId Node id.
     * @return Predicate for events belonging to specified node.
     */
    private IgnitePredicate<Event> nodeEvent(final UUID nodeId) {
        assert nodeId != null;

        return new P1<Event>() {
            @Override public boolean apply(Event e) {
                info("Predicate called [e.nodeId()=" + e.node().id() + ", nodeId=" + nodeId + ']');

                return e.node().id().equals(nodeId);
            }
        };
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullArgumentToVariableArgMethod")
    public void testSingleKey() throws Exception {
        Integer key = 1;

        Collection<ClusterNode> nodes = new ArrayList<>(keyNodes(key));

        ClusterNode primary = F.first(nodes);

        assert primary != null;

        nodes.remove(primary);

        ClusterNode backup = F.first(nodes);

        assert backup != null;

        assert !F.eqNodes(primary, backup);

        info("Key primary node: " + primary.id());
        info("Key backup node: " + backup.id());

        GridNearCacheAdapter<Integer, String> nearPrimary = near(grid(primary));
        GridDhtCacheAdapter<Integer, String> dhtPrimary = dht(grid(primary));

        GridNearCacheAdapter<Integer, String> nearBackup = near(grid(backup));
        GridDhtCacheAdapter<Integer, String> dhtBackup = dht(grid(backup));

        String val = "v1";

        // Put on primary node.
        nearPrimary.getAndPut(key, val, null);

        assertEquals(val, localPeek(nearPrimary, key));
        assertEquals(val, localPeek(dhtPrimary, key));

        assertEquals(val, localPeek(nearBackup, key));
        assertEquals(val, localPeek(dhtBackup, key));

        GridDhtCacheEntry entryPrimary = (GridDhtCacheEntry)dhtPrimary.peekEx(key);
        GridDhtCacheEntry entryBackup = (GridDhtCacheEntry)dhtBackup.peekEx(key);

        assert entryPrimary != null;
        assert entryBackup != null;

        assertTrue(entryPrimary.readers().isEmpty());
        assertTrue(entryBackup.readers().isEmpty());

        IgniteFuture<Event> futBackup =
            waitForLocalEvent(grid(backup).events(), nodeEvent(backup.id()), EVT_CACHE_ENTRY_EVICTED);

        IgniteFuture<Event> futPrimary =
            waitForLocalEvent(grid(primary).events(), nodeEvent(primary.id()), EVT_CACHE_ENTRY_EVICTED);

        // Evict on primary node.
        // It should trigger dht eviction and eviction on backup node.
        grid(primary).cache(null).localEvict(Collections.<Object>singleton(key));

        // Give 5 seconds for eviction event to occur on backup and primary node.
        futBackup.get(3000);
        futPrimary.get(3000);

        assertEquals(0, nearPrimary.size());

        assertNull(nearPrimary.peekEx(key));
        assertNull(dhtPrimary.peekEx(key));

        assertNull(nearBackup.peekEx(key));
        assertNull(dhtBackup.peekEx(key));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullArgumentToVariableArgMethod")
    public void testMultipleKeys() throws Exception {
        final int keyCnt = 1000;

        final Ignite primaryIgnite = grid(0);
        final Ignite backupIgnite = grid(1);

        GridNearCacheAdapter<Integer, String> nearPrimary = near(primaryIgnite);
        GridDhtCacheAdapter<Integer, String> dhtPrimary = dht(primaryIgnite);

        GridNearCacheAdapter<Integer, String> nearBackup = near(backupIgnite);
        GridDhtCacheAdapter<Integer, String> dhtBackup = dht(backupIgnite);

        Collection<Integer> keys = new ArrayList<>(keyCnt);

        for (int key = 0; keys.size() < keyCnt; key++)
            if (F.eqNodes(primaryIgnite.cluster().localNode(), F.first(keyNodes(key))))
                keys.add(key++);

        info("Test keys: " + keys);

        // Put on primary node.
        for (Integer key : keys)
            nearPrimary.getAndPut(key, "v" + key, null);

        for (Integer key : keys) {
            String val = "v" + key;

            assertEquals(val, localPeek(nearPrimary, key));
            assertEquals(val, localPeek(dhtPrimary, key));

            assertEquals(val, localPeek(nearBackup, key));
            assertEquals(val, localPeek(dhtBackup, key));
        }

        final AtomicInteger cntBackup = new AtomicInteger();

        IgniteFuture<Event> futBackup = waitForLocalEvent(backupIgnite.events(), new P1<Event>() {
            @Override public boolean apply(Event e) {
                return e.node().id().equals(backupIgnite.cluster().localNode().id()) &&
                    cntBackup.incrementAndGet() == keyCnt;
            }
        }, EVT_CACHE_ENTRY_EVICTED);

        final AtomicInteger cntPrimary = new AtomicInteger();

        IgniteFuture<Event> futPrimary = waitForLocalEvent(primaryIgnite.events(), new P1<Event>() {
            @Override public boolean apply(Event e) {
                return e.node().id().equals(primaryIgnite.cluster().localNode().id()) &&
                    cntPrimary.incrementAndGet() == keyCnt;
            }
        }, EVT_CACHE_ENTRY_EVICTED);

        // Evict on primary node.
        // Eviction of the last key should trigger queue processing.
        for (Integer key : keys)
            primaryIgnite.cache(null).localEvict(Collections.<Object>singleton(key));

        // Give 5 seconds for eviction events to occur on backup and primary node.
        futBackup.get(3000);
        futPrimary.get(3000);

        info("nearBackupSize: " + nearBackup.size());
        info("dhtBackupSize: " + dhtBackup.size());
        info("nearPrimarySize: " + nearPrimary.size());
        info("dhtPrimarySize: " + dhtPrimary.size());

        // Check backup node first.
        for (Integer key : keys) {
            String msg = "Failed key: " + key;

            assertNull(msg, localPeek(nearBackup, key));
            assertNull(msg, localPeek(dhtBackup, key));
            assertNull(msg, nearBackup.peekEx(key));
            assertNull(msg, dhtBackup.peekEx(key));
        }

        for (Integer key : keys) {
            String msg = "Failed key: " + key;

            assertNull(msg, localPeek(nearPrimary, key));
            assertNull(msg, localPeek(dhtPrimary, key));
            assertNull(msg, nearPrimary.peekEx(key));
            assertNull(dhtPrimary.peekEx(key));
        }
    }
}