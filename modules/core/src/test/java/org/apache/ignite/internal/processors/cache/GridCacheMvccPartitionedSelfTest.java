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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test cases for multi-threaded tests in partitioned cache.
 */
public class GridCacheMvccPartitionedSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private IgniteKernal grid;

    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheMvccPartitionedSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = (IgniteKernal)grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

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

        entry.readyNearLocal(ver2, ver2);

        checkLocal(c2, ver2, true, false, false);
        checkRemote(c1, ver1, false, false);

        entry.doneRemote(ver1);

        checkLocal(c2, ver2, true, false, false);
        checkRemote(c1, ver1, true, true);

        entry.removeLock(ver1);

        assertTrue(entry.remoteMvccSnapshot().isEmpty());

        checkLocal(c2, ver2, true, true, false);

        assertNotNull(entry.anyOwner());
        assertEquals(ver2, entry.anyOwner().version());
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

        entry.readyNearLocal(ver2, ver2);

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

        entry.readyNearLocal(ver2, ver2);

        checkRemote(c1, ver1, false, false);

        assertFalse(c1.owner());

        checkLocal(c2, ver2, true, false, false);

        assertNull(entry.anyOwner());
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

        entry.readyNearLocal(nearVer2, ver2);

        assertNull(entry.anyOwner());

        entry.doneRemote(ver1);

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

        entry.readyNearLocal(nearVer2, ver2);

        nearLocCands = entry.localCandidates();
        rmtCands = entry.remoteMvccSnapshot();

        assertNull(entry.anyOwner());

        entry.doneRemote(ver1);

        assertEquals(ver1, rmtCands.iterator().next().version());
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

        entry.readyNearLocal(nearVer2, ver2);

        nearLocCands = entry.localCandidates();
        rmtCands = entry.remoteMvccSnapshot();

        assertNull(entry.anyOwner());
        assertEquals(ver1, rmtCands.iterator().next().version());
        assertFalse(rmtCands.iterator().next().owner());

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

        entry.readyNearLocal(nearVer2, ver2);

        rmtCands = entry.remoteMvccSnapshot();

        assertNull(entry.anyOwner());
        checkLocal(entry.candidate(nearVer2), nearVer2, true, false, false);

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
