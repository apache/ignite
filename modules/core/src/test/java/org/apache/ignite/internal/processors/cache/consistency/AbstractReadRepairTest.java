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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 *
 */
public abstract class AbstractReadRepairTest extends GridCommonAbstractTest {
    /** Events. */
    private static ConcurrentLinkedDeque<CacheConsistencyViolationEvent> evtDeq = new ConcurrentLinkedDeque<>();

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
    protected static void checkEvent(ReadRepairData data) {
        Map<Object, Object> fixed = new HashMap<>();

        while (!evtDeq.isEmpty()) {
            CacheConsistencyViolationEvent evt = evtDeq.remove();

            fixed.putAll(evt.getRepairedEntries()); // Optimistic and read committed transactions produce per key fixes.
        }

        int misses = 0;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) {
            Integer key = entry.getKey();
            Integer latest = entry.getValue().latest;

            if (latest == null)
                misses++;

            assertEquals(latest, fixed.get(key));
        }

        assertEquals(data.data.size() - misses, fixed.size());

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
        Consumer<ReadRepairData> c)
        throws Exception {
        IgniteCache<Integer, Integer> cache = initiator.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 10); i++) {
            Map<Integer, InconsistentMapping> results = new HashMap<>();

            for (int j = 0; j < cnt; j++) {
                InconsistentMapping res = setDifferentValuesForSameKey(++iterableKey);

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
    private InconsistentMapping setDifferentValuesForSameKey(
        int key) throws Exception {
        List<Ignite> nodes = new ArrayList<>();
        Map<Ignite, Integer> mapping = new HashMap<>();

        Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);

        if (ThreadLocalRandom.current().nextBoolean()) { // Reversed order.
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
            nodes.add(primary);
        }
        else {
            nodes.add(primary);
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
        }

        if (ThreadLocalRandom.current().nextBoolean()) // Random order.
            Collections.shuffle(nodes);

        GridCacheVersionManager mgr =
            ((GridCacheAdapter)(grid(1)).cachex(DEFAULT_CACHE_NAME).cache()).context().shared().versions();

        int val = 0;
        int primVal = -1;

        for (Ignite node : nodes) {
            IgniteInternalCache cache = ((IgniteEx)node).cachex(DEFAULT_CACHE_NAME);

            GridCacheAdapter adapter = ((GridCacheAdapter)cache.cache());

            GridCacheEntryEx entry = adapter.entryEx(key);

            boolean init = entry.initialValue(
                new CacheObjectImpl(++val, null), // Incremental value.
                mgr.next(), // Incremental version.
                0,
                0,
                false,
                AffinityTopologyVersion.NONE,
                GridDrType.DR_NONE,
                false);

            assertTrue("iterableKey " + key + " already inited", init);

            mapping.put(node, val);

            if (node.equals(primary))
                primVal = val;
        }

        assertEquals(nodes.size(), new HashSet<>(mapping.values()).size()); // Each node have unique value.

        assertTrue(primVal != -1); // Primary value set.

        return new InconsistentMapping(mapping, primVal, val);
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
            this.data = new HashMap<>(data);
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
