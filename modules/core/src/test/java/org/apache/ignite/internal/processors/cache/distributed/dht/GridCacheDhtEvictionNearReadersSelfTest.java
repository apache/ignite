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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
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
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 * Tests for dht cache eviction.
 */
public class GridCacheDhtEvictionNearReadersSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Default constructor. */
    public GridCacheDhtEvictionNearReadersSelfTest() {
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
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setBackups(1);

        // Set eviction queue size explicitly.
        cacheCfg.setEvictSynchronizedKeyBufferSize(1);
        cacheCfg.setEvictMaxOverflowRatio(0);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(10);

        cacheCfg.setEvictionPolicy(plc);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        FifoEvictionPolicy nearPlc = new FifoEvictionPolicy();
        nearPlc.setMaxSize(10);

        nearCfg.setNearEvictionPolicy(nearPlc);

        cacheCfg.setNearConfiguration(nearCfg);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (GRID_CNT < 2)
            throw new IgniteCheckedException("GRID_CNT must not be less than 2.");

        startGridsMultiThreaded(GRID_CNT);
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
    public void testReaders() throws Exception {
        Integer key = 1;

        Collection<ClusterNode> nodes = new ArrayList<>(keyNodes(key));

        ClusterNode primary = F.first(nodes);

        assert primary != null;

        nodes.remove(primary);

        ClusterNode backup = F.first(nodes);

        assert backup != null;

        // Now calculate other node that doesn't own the key.
        nodes = new ArrayList<>(grid(0).cluster().nodes());

        nodes.remove(primary);
        nodes.remove(backup);

        ClusterNode other = F.first(nodes);

        assert !F.eqNodes(primary, backup);
        assert !F.eqNodes(primary, other);
        assert !F.eqNodes(backup, other);

        info("Primary node: " + primary.id());
        info("Backup node: " + backup.id());
        info("Other node: " + other.id());

        GridNearCacheAdapter<Integer, String> nearPrimary = near(grid(primary));
        GridDhtCacheAdapter<Integer, String> dhtPrimary = dht(grid(primary));

        GridNearCacheAdapter<Integer, String> nearBackup = near(grid(backup));
        GridDhtCacheAdapter<Integer, String> dhtBackup = dht(grid(backup));

        GridNearCacheAdapter<Integer, String> nearOther = near(grid(other));
        GridDhtCacheAdapter<Integer, String> dhtOther = dht(grid(other));

        String val = "v1";

        // Put on primary node.
        nearPrimary.getAndPut(key, val);

        GridDhtCacheEntry entryPrimary = (GridDhtCacheEntry)dhtPrimary.peekEx(key);
        GridDhtCacheEntry entryBackup = (GridDhtCacheEntry)dhtBackup.peekEx(key);

        assert entryPrimary != null;
        assert entryBackup != null;
        assert nearOther.peekEx(key) == null;
        assert dhtOther.peekEx(key) == null;

        IgniteFuture<Event> futOther =
            waitForLocalEvent(grid(other).events(), nodeEvent(other.id()), EVT_CACHE_ENTRY_EVICTED);

        IgniteFuture<Event> futBackup =
            waitForLocalEvent(grid(backup).events(), nodeEvent(backup.id()), EVT_CACHE_ENTRY_EVICTED);

        IgniteFuture<Event> futPrimary =
            waitForLocalEvent(grid(primary).events(), nodeEvent(primary.id()), EVT_CACHE_ENTRY_EVICTED);

        // Get value on other node, it should be loaded to near cache.
        assertEquals(val, nearOther.get(key, true));

        entryPrimary = (GridDhtCacheEntry)dhtPrimary.peekEx(key);
        entryBackup = (GridDhtCacheEntry)dhtBackup.peekEx(key);

        assert entryPrimary != null;
        assert entryBackup != null;

        assertEquals(val, localPeek(nearOther, key));

        assertTrue(!entryPrimary.readers().isEmpty());

        // Evict on primary node.
        // It will trigger dht eviction and eviction on backup node.
        grid(primary).cache(null).localEvict(Collections.<Object>singleton(key));

        futOther.get(3000);
        futBackup.get(3000);
        futPrimary.get(3000);

        assertNull(localPeek(dhtPrimary, key));
        assertNull(localPeek(nearPrimary, key));

        assertNull(localPeek(dhtBackup, key));
        assertNull(localPeek(nearBackup, key));

        assertNull(localPeek(dhtOther, key));
        assertNull(localPeek(nearOther, key));
    }
}