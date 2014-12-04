/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Test cases for multi-threaded tests in partitioned cache.
 */
public class GridCacheMvccPartitionedSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private GridKernal grid;

    /** VM ip finder for TCP discovery. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheMvccPartitionedSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = (GridKernal)grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocalsWithPending() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate<String> c1 = entry.addRemote(node1, 1, ver1, 0, false, true);
        GridCacheMvccCandidate<String> c2 = entry.addNearLocal(node1, 1, ver2, 0, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver2, nearLocCands.iterator().next().version());

        assertEquals(1, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.readyNearLocal(ver2, ver2, empty(), empty(), Arrays.asList(ver1));

        checkLocalOwner(c2, ver2, false);
        checkRemote(c1, ver1, false, false);

        assertNotNull(entry.anyOwner());
        assertEquals(ver2, entry.anyOwner().version());
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocalsWithCommitted() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate<String> c1 = entry.addNearLocal(node1, 1, ver1, 0, true);
        GridCacheMvccCandidate<String> c2 = entry.addRemote(node1, 1, ver2, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver1, nearLocCands.iterator().next().version());

        assertEquals(1, rmtCands.size());
        assertEquals(ver2, rmtCands.iterator().next().version());

        entry.readyNearLocal(ver1, ver1, Arrays.asList(ver2), empty(), empty());

        checkLocal(c1, ver1, true, false, false);
        checkRemote(c2, ver2, true, false);

        assertNull(entry.anyOwner());
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocalsWithRolledback() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate<String> c1 = entry.addNearLocal(node1, 1, ver1, 0, true);
        GridCacheMvccCandidate<String> c2 = entry.addRemote(node1, 1, ver2, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver1, nearLocCands.iterator().next().version());

        assertEquals(1, rmtCands.size());
        assertEquals(ver2, rmtCands.iterator().next().version());

        entry.readyNearLocal(ver1, ver1, empty(), Arrays.asList(ver2), empty());

        checkLocal(c1, ver1, true, false, false);
        checkRemote(c2, ver2, true, false);

        assertNull(entry.anyOwner());
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocals() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate<String> c1 = entry.addNearLocal(node1, 1, ver1, 0, true);
        GridCacheMvccCandidate<String> c2 = entry.addNearLocal(node1, 1, ver2, 0, true);

        entry.readyNearLocal(ver2, ver2,  empty(), empty(), empty());

        checkLocalOwner(c2, ver2, false);
        checkLocal(c1, ver1, false, false, false);

        Collection<GridCacheMvccCandidate<String>> cands = entry.localCandidates();

        assert cands.size() == 2;
        assert cands.iterator().next().version().equals(ver2);

        checkLocalOwner(c2, ver2, false);
        checkLocal(c1, ver1, false, false, false);
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocalsWithOwned() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate<String> c1 = entry.addRemote(node1, 1, ver1, 0, false, true);
        GridCacheMvccCandidate<String> c2 = entry.addNearLocal(node1, 1, ver2, 0, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver2, nearLocCands.iterator().next().version());

        assertEquals(1, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.orderOwned(ver1, ver2);

        entry.readyNearLocal(ver2, ver2,  empty(), empty(), empty());

        checkRemote(c1, ver1, false, false);

        assertFalse(c1.owner());

        checkLocalOwner(c2, ver2, false);

        assertNotNull(entry.anyOwner());
        assertEquals(ver2, entry.anyOwner().version());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddPendingRemote0() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);

        entry.addNearLocal(node1, 1, ver1, 0, true);

        entry.readyNearLocal(ver1, ver1, empty(), empty(), Collections.<GridCacheVersion>singletonList(ver0));

        entry.addRemote(node1, 1, ver0, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver1, nearLocCands.iterator().next().version());

        assertEquals(1, rmtCands.size());
        assertEquals(ver0, rmtCands.iterator().next().version());

        assertNotNull(entry.anyOwner());
        assertEquals(ver1, entry.anyOwner().version());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddPendingRemote1() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);

        GridCacheMvccCandidate<String> c3 = entry.addNearLocal(node1, 1, ver3, 0, true);

        entry.readyNearLocal(ver3, ver3, empty(), empty(), Arrays.asList(ver0, ver1, ver2));

        GridCacheMvccCandidate<String> c2 = entry.addRemote(node1, 1, ver2, 0, false, true);
        GridCacheMvccCandidate<String> c1 = entry.addRemote(node1, 1, ver1, 0, false, true);
        GridCacheMvccCandidate<String> c0 = entry.addRemote(node1, 1, ver0, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();

        assert rmtCands.size() == 3;

        // DHT remote candidates are not reordered and sorted.
        GridCacheMvccCandidate[] candArr = new GridCacheMvccCandidate[] {c2, c1, c0};

        rmtCands = entry.remoteMvccSnapshot();

        int i = 0;

        for (GridCacheMvccCandidate<String> cand : rmtCands) {
            assert cand == candArr[i] : "Invalid candidate in position " + i;

            i++;
        }

        assertEquals(c3, entry.anyOwner());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddPendingRemote2() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);

        GridCacheMvccCandidate<String> c3 = entry.addNearLocal(node1, 1, ver3, 0, true);
        entry.addNearLocal(node1, 1, ver2, 0, true);

        entry.readyNearLocal(ver3, ver3, empty(), empty(), Arrays.asList(ver0, ver1, ver2));

        GridCacheMvccCandidate<String> c1 = entry.addRemote(node1, 1, ver1, 0, false, true);
        GridCacheMvccCandidate<String> c0 = entry.addRemote(node1, 1, ver0, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();

        assertEquals(2, rmtCands.size());

        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(2, nearLocCands.size());

        GridCacheMvccCandidate[] candArr = new GridCacheMvccCandidate[] {c1, c0};

        int i = 0;

        for (GridCacheMvccCandidate<String> cand : rmtCands) {
            assert cand == candArr[i] : "Invalid candidate in position " + i;

            i++;
        }

        assertEquals(c3, entry.anyOwner());
    }

    /**
     * Tests salvageRemote method
     */
    public void testSalvageRemote() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);
        GridCacheVersion ver4 = version(4);
        GridCacheVersion ver5 = version(5);
        GridCacheVersion ver6 = version(6);

        entry.addRemote(node1, 1, ver1, 0, false, true);
        entry.addRemote(node1, 1, ver2, 0, false, true);
        GridCacheMvccCandidate<String> c3 = entry.addNearLocal(node1, 1, ver3, 0, true);
        GridCacheMvccCandidate<String> c4 = entry.addRemote(node1, 1, ver4, 0, false, true);
        entry.addRemote(node1, 1, ver5, 0, false, true);
        entry.addRemote(node1, 1, ver6, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();

        assertEquals(5, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver3, nearLocCands.iterator().next().version());

        entry.salvageRemote(ver4);

        rmtCands = entry.remoteMvccSnapshot();

        boolean before = true;

        for (GridCacheMvccCandidate<String> cand : rmtCands) {
            if (cand == c4) {
                before = false;

                continue;
            }

            if (before && cand != c3) {
                assertTrue(cand.owner());
                assertTrue(cand.used());
            }
            else {
                assertFalse(cand.owner());
                assertFalse(cand.used());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering0() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, 0, false, true);
        entry.addNearLocal(node1, 1, nearVer2, 0, true);
        entry.addRemote(node1, 1, ver3, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(nearVer2, nearLocCands.iterator().next().version());

        assertEquals(2, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.readyNearLocal(nearVer2, ver2, empty(), empty(), empty());

        assertNull(entry.anyOwner());

        rmtCands = entry.remoteMvccSnapshot();

        assertEquals(ver1, rmtCands.iterator().next().version());
        assertTrue(rmtCands.iterator().next().owner());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering1() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, 0, false, true);
        entry.addNearLocal(node1, 1, nearVer2, 0, true);
        entry.addRemote(node1, 1, ver3, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(nearVer2, nearLocCands.iterator().next().version());

        assertEquals(2, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.orderCompleted(nearVer2, Arrays.asList(ver3), empty());
        entry.readyNearLocal(nearVer2, ver2, empty(), empty(), Arrays.asList(ver1));

        nearLocCands = entry.localCandidates();
        rmtCands = entry.remoteMvccSnapshot();

        assertNull(entry.anyOwner());
        assertEquals(ver3, rmtCands.iterator().next().version());
        assertTrue(rmtCands.iterator().next().owner());

        GridCacheMvccCandidate<String> cand = nearLocCands.iterator().next();

        assertTrue(cand.ready());
        assertFalse(cand.owner());
        assertFalse(cand.used());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering2() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, 0, false, true);
        entry.addNearLocal(node1, 1, nearVer2, 0, true);
        entry.addRemote(node1, 1, ver3, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(nearVer2, nearLocCands.iterator().next().version());

        assertEquals(2, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.orderCompleted(nearVer2, empty(), empty());
        entry.readyNearLocal(nearVer2, ver2, empty(), empty(), empty());

        nearLocCands = entry.localCandidates();
        rmtCands = entry.remoteMvccSnapshot();

        assertNull(entry.anyOwner());
        assertEquals(ver1, rmtCands.iterator().next().version());
        assertTrue(rmtCands.iterator().next().owner());

        GridCacheMvccCandidate<String> cand = nearLocCands.iterator().next();

        assertTrue(cand.ready());
        assertFalse(cand.used());
        assertFalse(cand.owner());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering3() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, 0, false, true);
        entry.addNearLocal(node1, 1, nearVer2, 0, true);
        entry.addRemote(node1, 1, ver3, 0, false, true);

        Collection<GridCacheMvccCandidate<String>> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate<String>> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(nearVer2, nearLocCands.iterator().next().version());

        assertEquals(2, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        entry.orderCompleted(nearVer2, empty(), empty());
        entry.readyNearLocal(nearVer2, ver2, empty(), empty(), Arrays.asList(ver1));

        rmtCands = entry.remoteMvccSnapshot();

        assertNotNull(entry.anyOwner());
        checkLocalOwner(entry.anyOwner(), nearVer2, false);

        assertEquals(ver1, rmtCands.iterator().next().version());
    }

    /**
     * Gets version based on order.
     *
     * @param order Order.
     * @return Version.
     */
    private GridCacheVersion version(int order) {
        return new GridCacheVersion(1, 0, order, order, 0);
    }

    /**
     * Creates an empty list of {@code GridCacheVersion}.
     *
     * @return Empty list.
     */
    private Collection<GridCacheVersion> empty() {
        return Collections.emptyList();
    }

    /**
     * Checks flags on local owner candidate.
     *
     * @param cand Candidate to check.
     * @param ver Cache version.
     * @param reentry Reentry flag.
     */
    private void checkLocalOwner(GridCacheMvccCandidate<String> cand, GridCacheVersion ver, boolean reentry) {
        assert cand != null;

        info("Done candidate: " + cand);

        assert cand.version().equals(ver);

        // Check flags.
        assert cand.reentry() == reentry;

        assert !cand.used();

        assert cand.ready();
        assert cand.owner();
        assert cand.local();
    }

    /**
     * @param cand Candidate to check.
     * @param ver Version.
     * @param owner Owner flag.
     * @param used Done flag.
     */
    private void checkRemote(GridCacheMvccCandidate<String> cand, GridCacheVersion ver, boolean owner, boolean used) {
        assert cand != null;

        info("Done candidate: " + cand);

        assert cand.version().equals(ver);

        // Check flags.
        assert cand.used() == used;
        assert cand.owner() == owner;

        assert !cand.ready();
        assert !cand.reentry();
        assert !cand.local();
    }

    /**
     * Checks flags on local candidate.
     *
     * @param cand Candidate to check.
     * @param ver Cache version.
     * @param ready Ready flag.
     * @param owner Lock owner.
     * @param reentry Reentry flag.
     */
    private void checkLocal(GridCacheMvccCandidate<String> cand, GridCacheVersion ver, boolean ready,
        boolean owner, boolean reentry) {
        assert cand != null;

        info("Done candidate: " + cand);

        assert cand.version().equals(ver);

        // Check flags.
        assert cand.ready() == ready;
        assert cand.owner() == owner;
        assert cand.reentry() == reentry;

        assert !cand.used();

        assert cand.local();
    }
}
