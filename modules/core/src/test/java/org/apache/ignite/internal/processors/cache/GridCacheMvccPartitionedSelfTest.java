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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test cases for multi-threaded tests in partitioned cache.
 */
public class GridCacheMvccPartitionedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final UUID nodeId = UUID.randomUUID();

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

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Tests remote candidates.
     */
    public void testNearLocalsWithPending() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate c1 = entry.addRemote(node1, 1, ver1, true);
        GridCacheMvccCandidate c2 = entry.addNearLocal(node1, 1, ver2, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate c1 = entry.addNearLocal(node1, 1, ver1, true);
        GridCacheMvccCandidate c2 = entry.addRemote(node1, 1, ver2, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate c1 = entry.addNearLocal(node1, 1, ver1, true);
        GridCacheMvccCandidate c2 = entry.addRemote(node1, 1, ver2, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate c1 = entry.addNearLocal(node1, 1, ver1, true);
        GridCacheMvccCandidate c2 = entry.addNearLocal(node1, 1, ver2, true);

        entry.readyNearLocal(ver2, ver2,  empty(), empty(), empty());

        checkLocalOwner(c2, ver2, false);
        checkLocal(c1, ver1, false, false, false);

        Collection<GridCacheMvccCandidate> cands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);

        GridCacheMvccCandidate c1 = entry.addRemote(node1, 1, ver1, true);
        GridCacheMvccCandidate c2 = entry.addNearLocal(node1, 1, ver2, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);

        entry.addNearLocal(node1, 1, ver1, true);

        entry.readyNearLocal(ver1, ver1, empty(), empty(), Collections.singletonList(ver0));

        entry.addRemote(node1, 1, ver0, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);

        GridCacheMvccCandidate c3 = entry.addNearLocal(node1, 1, ver3, true);

        entry.readyNearLocal(ver3, ver3, empty(), empty(), Arrays.asList(ver0, ver1, ver2));

        GridCacheMvccCandidate c2 = entry.addRemote(node1, 1, ver2, true);
        GridCacheMvccCandidate c1 = entry.addRemote(node1, 1, ver1, true);
        GridCacheMvccCandidate c0 = entry.addRemote(node1, 1, ver0, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();

        assert rmtCands.size() == 3;

        // DHT remote candidates are not reordered and sorted.
        GridCacheMvccCandidate[] candArr = new GridCacheMvccCandidate[] {c2, c1, c0};

        rmtCands = entry.remoteMvccSnapshot();

        int i = 0;

        for (GridCacheMvccCandidate cand : rmtCands) {
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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver0 = version(0);
        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);

        GridCacheMvccCandidate c3 = entry.addNearLocal(node1, 1, ver3, true);
        entry.addNearLocal(node1, 1, ver2, true);

        entry.readyNearLocal(ver3, ver3, empty(), empty(), Arrays.asList(ver0, ver1, ver2));

        GridCacheMvccCandidate c1 = entry.addRemote(node1, 1, ver1, true);
        GridCacheMvccCandidate c0 = entry.addRemote(node1, 1, ver0, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();

        assertEquals(2, rmtCands.size());

        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

        assertEquals(2, nearLocCands.size());

        GridCacheMvccCandidate[] candArr = new GridCacheMvccCandidate[] {c1, c0};

        int i = 0;

        for (GridCacheMvccCandidate cand : rmtCands) {
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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(1);
        GridCacheVersion ver2 = version(2);
        GridCacheVersion ver3 = version(3);
        GridCacheVersion ver4 = version(4);
        GridCacheVersion ver5 = version(5);
        GridCacheVersion ver6 = version(6);

        entry.addRemote(node1, 1, ver1, true);
        entry.addRemote(node1, 1, ver2, true);
        GridCacheMvccCandidate c3 = entry.addNearLocal(node1, 1, ver3, true);
        GridCacheMvccCandidate c4 = entry.addRemote(node1, 1, ver4, true);
        entry.addRemote(node1, 1, ver5, true);
        entry.addRemote(node1, 1, ver6, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();

        assertEquals(5, rmtCands.size());
        assertEquals(ver1, rmtCands.iterator().next().version());

        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

        assertEquals(1, nearLocCands.size());
        assertEquals(ver3, nearLocCands.iterator().next().version());

        entry.salvageRemote(ver4);

        rmtCands = entry.remoteMvccSnapshot();

        boolean before = true;

        for (GridCacheMvccCandidate cand : rmtCands) {
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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, true);
        entry.addNearLocal(node1, 1, nearVer2, true);
        entry.addRemote(node1, 1, ver3, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, true);
        entry.addNearLocal(node1, 1, nearVer2, true);
        entry.addRemote(node1, 1, ver3, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheMvccCandidate cand = nearLocCands.iterator().next();

        assertTrue(cand.ready());
        assertFalse(cand.owner());
        assertFalse(cand.used());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering2() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, true);
        entry.addNearLocal(node1, 1, nearVer2, true);
        entry.addRemote(node1, 1, ver3, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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

        GridCacheMvccCandidate cand = nearLocCands.iterator().next();

        assertTrue(cand.ready());
        assertFalse(cand.used());
        assertFalse(cand.owner());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRemoteConsistentOrdering3() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx entry = new GridCacheTestEntryEx(cache.context(), "1");

        UUID node1 = UUID.randomUUID();

        GridCacheVersion ver1 = version(10);
        GridCacheVersion nearVer2 = version(5);
        GridCacheVersion ver2 = version(20);
        GridCacheVersion ver3 = version(30);

        entry.addRemote(node1, 1, ver1, true);
        entry.addNearLocal(node1, 1, nearVer2, true);
        entry.addRemote(node1, 1, ver3, true);

        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();
        Collection<GridCacheMvccCandidate> nearLocCands = entry.localCandidates();

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
     * @throws Exception If failed.
     */
    public void testSerializableReadLocksAdd() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheVersion serOrder1 = new GridCacheVersion(0, 0, 10, 1);
        GridCacheVersion serOrder2 = new GridCacheVersion(0, 0, 20, 1);
        GridCacheVersion serOrder3 = new GridCacheVersion(0, 0, 15, 1);

        {
            GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

            GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

            GridCacheMvccCandidate cand1 = addLocal(mvcc, e, version(1), serOrder1, true);

            assertNotNull(cand1);

            GridCacheMvccCandidate cand2 = addLocal(mvcc, e, version(2), serOrder2, true);

            assertNotNull(cand2);

            GridCacheMvccCandidate cand3 = addLocal(mvcc, e, version(3), serOrder3, false);

            assertNull(cand3);

            cand3 = addLocal(mvcc, e, version(3), serOrder3, true);

            assertNotNull(cand3);
        }

        {
            GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

            GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

            GridCacheMvccCandidate cand1 = addLocal(mvcc, e, version(1), serOrder2, true);

            assertNotNull(cand1);

            GridCacheMvccCandidate cand2 = addLocal(mvcc, e, version(2), serOrder1, true);

            assertNotNull(cand2);

            GridCacheMvccCandidate cand3 = addLocal(mvcc, e, version(3), serOrder3, false);

            assertNull(cand3);

            cand3 = addLocal(mvcc, e, version(3), serOrder3, true);

            assertNotNull(cand3);
        }

        {
            GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

            GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

            GridCacheMvccCandidate cand1 = addLocal(mvcc, e, version(1), serOrder3, false);

            assertNotNull(cand1);

            GridCacheMvccCandidate cand2 = addLocal(mvcc, e, version(2), serOrder2, true);

            assertNotNull(cand2);

            GridCacheMvccCandidate cand3 = addLocal(mvcc, e, version(3), serOrder1, true);

            assertNull(cand3);

            cand3 = addLocal(mvcc, e, version(3), serOrder1, false);

            assertNull(cand3);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializableReadLocksAssign() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheVersion serOrder1 = new GridCacheVersion(0, 0, 10, 1);
        GridCacheVersion serOrder2 = new GridCacheVersion(0, 0, 20, 1);
        GridCacheVersion serOrder3 = new GridCacheVersion(0, 0, 15, 1);

        {
            GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

            GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

            GridCacheMvccCandidate cand1 = addLocal(mvcc, e, version(1), serOrder1, true);

            assertNotNull(cand1);

            GridCacheMvccCandidate cand2 = addLocal(mvcc, e, version(2), serOrder2, true);

            assertNotNull(cand2);

            GridCacheMvccCandidate cand3 = addLocal(mvcc, e, version(3), serOrder3, false);

            assertNull(cand3);

            cand3 = addLocal(mvcc, e, version(3), serOrder3, true);

            assertNotNull(cand3);

            CacheLockCandidates owners = mvcc.recheck();

            assertNull(owners);

            cand1.setReady();

            owners = mvcc.recheck();

            assertSame(cand1, owners);
            checkCandidates(owners, cand1.version());

            cand2.setReady();

            owners = mvcc.recheck();
            checkCandidates(owners, cand1.version(), cand2.version());

            mvcc.remove(cand1.version());

            owners = mvcc.recheck();
            assertSame(cand2, owners);
            checkCandidates(owners, cand2.version());
        }

        {
            GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

            GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

            GridCacheMvccCandidate cand1 = addLocal(mvcc, e, version(1), serOrder1, true);

            assertNotNull(cand1);

            GridCacheMvccCandidate cand2 = addLocal(mvcc, e, version(2), serOrder2, true);

            assertNotNull(cand2);

            GridCacheMvccCandidate cand3 = addLocal(mvcc, e, version(3), serOrder3, false);

            assertNull(cand3);

            cand3 = addLocal(mvcc, e, version(3), serOrder3, true);

            assertNotNull(cand3);

            CacheLockCandidates owners = mvcc.recheck();

            assertNull(owners);

            cand2.setReady();

            owners = mvcc.recheck();

            assertSame(cand2, owners);
            checkCandidates(owners, cand2.version());

            cand1.setReady();

            owners = mvcc.recheck();
            checkCandidates(owners, cand1.version(), cand2.version());

            mvcc.remove(cand2.version());

            owners = mvcc.recheck();
            assertSame(cand1, owners);
            checkCandidates(owners, cand1.version());
        }
    }

    /**
     * @param all Candidates list.
     * @param vers Expected candidates.
     */
    private void checkCandidates(CacheLockCandidates all, GridCacheVersion...vers) {
        assertNotNull(all);
        assertEquals(vers.length, all.size());

        for (GridCacheVersion ver : vers)
            assertTrue(all.hasCandidate(ver));
    }

    /**
     * @param mvcc Mvcc.
     * @param e Entry.
     * @param ver Version.
     * @param serOrder Serializable tx version.
     * @param read Read lock flag.
     * @return Candidate.
     */
    @Nullable private GridCacheMvccCandidate addLocal(GridCacheMvcc mvcc,
        GridCacheEntryEx e,
        GridCacheVersion ver,
        GridCacheVersion serOrder,
        boolean read) {
        return mvcc.addLocal(e,
            nodeId,
            null,
            1,
            ver,
            0,
            serOrder,
            false,
            true,
            false,
            true,
            read
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializableLocks() throws Exception {
        checkSerializableAdd(false);

        checkSerializableAdd(true);

        checkNonSerializableConflict();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNonSerializableConflict() throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        UUID nodeId = UUID.randomUUID();

        GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

        GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

        GridCacheMvccCandidate cand1 = mvcc.addLocal(e,
            nodeId,
            null,
            1,
            version(1),
            0,
            null,
            false,
            true,
            false,
            true,
            false
        );

        assertNotNull(cand1);

        GridCacheMvccCandidate cand2 = mvcc.addLocal(e,
            nodeId,
            null,
            1,
            version(2),
            0,
            new GridCacheVersion(0, 0, 30, 1),
            false,
            true,
            false,
            true,
            false
        );

        assertNull(cand2);
    }

    /**
     * @param incVer If {@code true} lock version is incremented.
     * @throws Exception If failed.
     */
    private void checkSerializableAdd(boolean incVer) throws Exception {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        UUID nodeId = UUID.randomUUID();

        GridCacheMvcc mvcc = new GridCacheMvcc(cache.context());

        GridCacheTestEntryEx e = new GridCacheTestEntryEx(cache.context(), "1");

        GridCacheVersion serOrder1 = new GridCacheVersion(0, 0, 10, 1);
        GridCacheVersion serOrder2 = new GridCacheVersion(0, 0, 20, 1);
        GridCacheVersion serOrder3 = new GridCacheVersion(0, 0, 15, 1);
        GridCacheVersion serOrder4 = new GridCacheVersion(0, 0, 30, 1);

        GridCacheVersion ver1 = incVer ? version(1) : version(4);
        GridCacheVersion ver2 = incVer ? version(2) : version(3);
        GridCacheVersion ver3 = incVer ? version(3) : version(2);
        GridCacheVersion ver4 = incVer ? version(4) : version(1);

        GridCacheMvccCandidate cand1 = mvcc.addLocal(e,
            nodeId,
            null,
            1,
            ver1,
            0,
            serOrder1,
            false,
            true,
            false,
            true,
            false
            );

        assertNotNull(cand1);

        GridCacheMvccCandidate cand2 = mvcc.addLocal(e,
            nodeId,
            null,
            2,
            ver2,
            0,
            serOrder2,
            false,
            true,
            false,
            true,
            false
        );

        assertNotNull(cand2);

        GridCacheMvccCandidate cand3 = mvcc.addLocal(e,
            nodeId,
            null,
            3,
            ver3,
            0,
            serOrder3,
            false,
            true,
            false,
            true,
            false
        );

        assertNull(cand3);

        GridCacheMvccCandidate cand4 = mvcc.addLocal(e,
            nodeId,
            null,
            4,
            ver4,
            0,
            serOrder4,
            false,
            true,
            false,
            true,
            false
        );

        assertNotNull(cand4);

        CacheLockCandidates owners = mvcc.recheck();

        assertNull(owners);

        cand2.setReady();

        owners = mvcc.recheck();

        assertNull(owners);

        cand1.setReady();

        owners = mvcc.recheck();

        assertSame(cand1, owners);

        owners = mvcc.recheck();

        assertSame(cand1, owners);

        mvcc.remove(cand1.version());

        owners = mvcc.recheck();

        assertSame(cand2, owners);
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
    private void checkLocalOwner(GridCacheMvccCandidate cand, GridCacheVersion ver, boolean reentry) {
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
    private void checkRemote(GridCacheMvccCandidate cand, GridCacheVersion ver, boolean owner, boolean used) {
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
    private void checkLocal(GridCacheMvccCandidate cand, GridCacheVersion ver, boolean ready,
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
