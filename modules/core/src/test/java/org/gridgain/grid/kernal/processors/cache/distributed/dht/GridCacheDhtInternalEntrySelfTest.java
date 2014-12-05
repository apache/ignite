/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Tests for internal DHT entry.
 */
public class GridCacheDhtInternalEntrySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Atomic long name. */
    private static final String ATOMIC_LONG_NAME = "test.atomic.long";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 2));
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.NEAR_PARTITIONED);
        cacheCfg.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    public void testInternalKeyReaders() throws Exception {
        IgniteBiTuple<ClusterNode, ClusterNode> nodes = getNodes(ATOMIC_LONG_NAME);

        ClusterNode primary = nodes.get1();
        ClusterNode other = nodes.get2();

        // Create on non-primary node.
        GridCacheAtomicLong l = grid(other).cache(null).dataStructures().atomicLong(ATOMIC_LONG_NAME, 1, true);

        assert l != null;
        assert l.get() == 1;

        check(primary, other, true);

        // Update on primary.
        l = grid(primary).cache(null).dataStructures().atomicLong(ATOMIC_LONG_NAME, 1, true);

        assert l != null;
        assert l.get() == 1;

        l.incrementAndGet();

        assert l.get() == 2;

        // Check on non-primary.
        l = grid(other).cache(null).dataStructures().atomicLong(ATOMIC_LONG_NAME, 1, true);

        assert l != null;
        assert l.get() == 2;

        check(primary, other, true);

        // Remove.
        assert grid(other).cache(null).dataStructures().removeAtomicLong(ATOMIC_LONG_NAME);

        check(primary, other, false);
    }

    /**
     * @param primary Primary node.
     * @param other Non-primary node.
     * @param exists Whether entry is expected to exist.
     * @throws Exception In case of error.
     */
    private void check(ClusterNode primary, ClusterNode other, boolean exists) throws Exception {
        if (exists) {
            // Check primary node has entry in DHT cache.
            assert peekNear(primary) == null;
            assert peekDht(primary) != null;

            // Check non-primary node has entry in near cache.
            assert peekNear(other) != null;
            assert peekDht(other) == null;

            // Check primary node has reader for non-primary node.
            assert peekDhtEntry(primary).readers().contains(other.id());
        }
        else {
            assert peekGlobal(primary) == null;
            assert peekGlobal(other) == null;
        }
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     */
    private GridCacheAtomicLongValue peekGlobal(ClusterNode node) {
        return (GridCacheAtomicLongValue)grid(node).cache(null).peek(
            new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME));
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     * @throws GridException In case of error.
     */
    private GridCacheAtomicLongValue peekNear(ClusterNode node) throws GridException {
        return (GridCacheAtomicLongValue)grid(node).cache(null).peek(
            new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), Collections.singleton(NEAR_ONLY));
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     * @throws GridException In case of error.
     */
    private GridCacheAtomicLongValue peekDht(ClusterNode node) throws GridException {
        return (GridCacheAtomicLongValue)grid(node).cache(null).peek(
            new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), Collections.singleton(PARTITIONED_ONLY));
    }

    /**
     * @param node Node.
     * @return DHT entry.
     */
    private GridDhtCacheEntry<Object, Object> peekDhtEntry(ClusterNode node) {
        return (GridDhtCacheEntry<Object, Object>)dht(grid(node).cache(null)).peekEx(
            new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME));
    }

    /**
     * @param key Key.
     * @return Pair {primary node, some other node}.
     */
    private IgniteBiTuple<ClusterNode, ClusterNode> getNodes(String key) {
        GridCacheAffinity<Object> aff = grid(0).cache(null).affinity();

        ClusterNode primary = aff.mapKeyToNode(key);

        assert primary != null;

        Collection<ClusterNode> nodes = new ArrayList<>(grid(0).nodes());

        nodes.remove(primary);

        ClusterNode other = F.first(nodes);

        assert other != null;

        assert !F.eqNodes(primary, other);

        return F.t(primary, other);
    }

    /**
     * @param node Node.
     * @return Grid.
     */
    private Ignite grid(ClusterNode node) {
        return G.grid(node.id());
    }
}
