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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 *
 */
public abstract class AbstractReadRepairTest extends GridCommonAbstractTest {
    /** Events. */
    private static final ConcurrentLinkedDeque<CacheConsistencyViolationEvent> evtDeq = new ConcurrentLinkedDeque<>();

    /** Key. */
    protected static int iterableKey;

    /** Backups count. */
    protected Integer backupsCount() {
        return 3;
    }

    /** Cache mode. */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** Atomicy mode. */
    protected CacheAtomicityMode atomicyMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite ignite = startGrids(7); // Server nodes.

        grid(0).getOrCreateCache(cacheConfiguration());

        startClientGrid(G.allGrids().size() + 1); // Client node 1.
        startClientGrid(G.allGrids().size() + 1); // Client node 2.

        final IgniteEvents evts = ignite.events();

        evts.remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof CacheConsistencyViolationEvent;

                evtDeq.add((CacheConsistencyViolationEvent)e);

                return true;
            },
            EVT_CONSISTENCY_VIOLATION);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    @Before
    public void before() {
        evtDeq.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        log.info("Checked " + iterableKey + " keys");

        stopAllGrids();
    }

    /**
     *
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicyMode());
        cfg.setBackups(backupsCount());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     *
     */
    protected void checkEventMissed() {
        assertTrue(evtDeq.isEmpty());
    }

    /**
     *
     */
    protected void checkEvent(ReadRepairData data, boolean checkFixed) {
        Map<Object, Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo>> entries = new HashMap<>();

        while (!entries.keySet().equals(data.data.keySet())) {
            if (!evtDeq.isEmpty()) {
                CacheConsistencyViolationEvent evt = evtDeq.remove();

                entries.putAll(evt.getEntries()); // Optimistic and read committed transactions produce per key fixes.
            }
        }

        int fixedCnt = 0;

        for (Map.Entry<Integer, InconsistentMapping> mapping : data.data.entrySet()) {
            Integer key = mapping.getKey();
            Integer latest = mapping.getValue().latest;

            Object fixedVal = null;

            Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> values = entries.get(key);

            if (values != null)
                for (Map.Entry<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> infoEntry : values.entrySet()) {
                    ClusterNode node = infoEntry.getKey();
                    CacheConsistencyViolationEvent.EntryInfo info = infoEntry.getValue();

                    if (info.isCorrect()) {
                        assertNull(fixedVal);

                        fixedVal = info.getValue();

                        fixedCnt++;
                    }

                    if (info.isPrimary())
                        assertEquals(node, primaryNode(key, DEFAULT_CACHE_NAME).cluster().localNode());
                }

            if (latest != null)
                assertEquals(checkFixed ? latest : null, fixedVal);
        }

        assertEquals(checkFixed ? data.data.size() : 0, fixedCnt);

        assertTrue(evtDeq.isEmpty());
    }

    /**
     *
     */
    protected void prepareAndCheck(
        Ignite initiator,
        Integer cnt,
        boolean raw,
        boolean async,
        boolean misses,
        boolean nulls,
        Consumer<ReadRepairData> c)
        throws Exception {
        IgniteCache<Integer, Integer> cache = initiator.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 10); i++) {
            Map<Integer, InconsistentMapping> results = new TreeMap<>(); // Sorted to avoid warning.

            for (int j = 0; j < cnt; j++) {
                InconsistentMapping res = setDifferentValuesForSameKey(++iterableKey, misses, nulls);

                results.put(iterableKey, res);
            }

            for (Ignite node : G.allGrids()) { // Check that cache filled properly.
                Map<Integer, Integer> all =
                    node.<Integer, Integer>getOrCreateCache(DEFAULT_CACHE_NAME).getAll(results.keySet());

                for (Map.Entry<Integer, Integer> entry : all.entrySet()) {
                    Integer key = entry.getKey();
                    Integer val = entry.getValue();

                    Integer exp = results.get(key).mapping.get(node); // Should read from itself (backup or primary).

                    if (exp == null)
                        exp = results.get(key).primary; // Should read from primary (not a partition owner).

                    assertEquals(exp, val);
                }
            }

            c.accept(new ReadRepairData(cache, results, raw, async));
        }
    }

    /**
     *
     */
    private InconsistentMapping setDifferentValuesForSameKey(int key, boolean misses, boolean nulls) throws Exception {
        List<Ignite> nodes = new ArrayList<>();
        Map<Ignite, Integer> mapping = new HashMap<>();

        Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        if (rnd.nextBoolean()) { // Reversed order.
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
            nodes.add(primary);
        }
        else {
            nodes.add(primary);
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
        }

        if (rnd.nextBoolean()) // Random order.
            Collections.shuffle(nodes);

        GridCacheVersionManager mgr =
            ((GridCacheAdapter)(grid(1)).cachex(DEFAULT_CACHE_NAME).cache()).context().shared().versions();

        int incVal = 0;
        Integer primVal = null;

        if (misses)
            nodes = nodes.subList(0, rnd.nextInt(1, nodes.size()));

        int rmvLimit = rnd.nextInt(nodes.size());

        boolean incVer = rnd.nextBoolean();

        GridCacheVersion ver = null;

        for (Ignite node : nodes) {
            IgniteInternalCache cache = ((IgniteEx)node).cachex(DEFAULT_CACHE_NAME);

            GridCacheAdapter adapter = ((GridCacheAdapter)cache.cache());

            GridCacheEntryEx entry = adapter.entryEx(key);

            if (ver == null || incVer)
                ver = mgr.next(entry.context().kernalContext().discovery().topologyVersion()); // Incremental version.

            boolean rmv = nulls && rnd.nextBoolean() && --rmvLimit >= 0;

            Integer val = rmv ? null : ++incVal;

            boolean init = entry.initialValue(
                new CacheObjectImpl(rmv ? -1 : val, null), // Incremental value.
                ver,
                0,
                0,
                false,
                AffinityTopologyVersion.NONE,
                GridDrType.DR_NONE,
                false,
                false);

            if (rmv) {
                if (cache.configuration().getAtomicityMode() == ATOMIC)
                    entry.innerUpdate(
                        ver,
                        ((IgniteEx)node).localNode().id(),
                        ((IgniteEx)node).localNode().id(),
                        GridCacheOperation.DELETE,
                        null,
                        null,
                        false,
                        false,
                        false,
                        false,
                        null,
                        false,
                        false,
                        false,
                        false,
                        AffinityTopologyVersion.NONE,
                        null,
                        GridDrType.DR_NONE,
                        0,
                        0,
                        null,
                        false,
                        false,
                        null,
                        null,
                        null,
                        null,
                        false);
                else
                    entry.innerRemove(
                        null,
                        ((IgniteEx)node).localNode().id(),
                        ((IgniteEx)node).localNode().id(),
                        false,
                        false,
                        false,
                        false,
                        false,
                        null,
                        AffinityTopologyVersion.NONE,
                        CU.empty0(),
                        GridDrType.DR_NONE,
                        null,
                        null,
                        null,
                        1L);

                assertFalse(entry.hasValue());
            }
            else
                assertTrue(entry.hasValue());

            assertTrue("iterableKey " + key + " already inited", init);

            mapping.put(node, val);

            if (node.equals(primary))
                primVal = val;
        }

        if (!nulls)
            assertEquals(nodes.size(), new HashSet<>(mapping.values()).size()); // Each node have unique value.

        if (!misses && !nulls)
            assertTrue(primVal != null); // Primary value set.

        return new InconsistentMapping(mapping, primVal, incVer ? incVal : null /*Any*/);
    }

    /**
     *
     */
    protected static final class ReadRepairData {
        /** Initiator's cache. */
        IgniteCache<Integer, Integer> cache;

        /** Generated data across topology per key mapping. */
        Map<Integer, InconsistentMapping> data;

        /** Raw read flag. True means required GetEntry() instead of get(). */
        boolean raw;

        /** Async read flag. */
        boolean async;

        /**
         *
         */
        public ReadRepairData(
            IgniteCache<Integer, Integer> cache,
            Map<Integer, InconsistentMapping> data,
            boolean raw,
            boolean async) {
            this.cache = cache;
            this.data = data;
            this.raw = raw;
            this.async = async;
        }
    }

    /**
     *
     */
    protected static final class InconsistentMapping {
        /** Value per node. */
        Map<Ignite, Integer> mapping;

        /** Primary node's value. */
        Integer primary;

        /** Latest value across the topology. */
        Integer latest;

        /**
         *
         */
        public InconsistentMapping(Map<Ignite, Integer> mapping, Integer primary, Integer latest) {
            this.mapping = new HashMap<>(mapping);
            this.primary = primary;
            this.latest = latest;
        }
    }
}
