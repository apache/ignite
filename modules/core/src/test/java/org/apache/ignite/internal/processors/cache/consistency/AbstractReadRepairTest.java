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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteIrreparableConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
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
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** Persistence enabled. */
    protected boolean persistenceEnabled() {
        return false;
    }

    /** Server nodes count. */
    private int serverNodesCount() {
        return backupsCount() + 1/*primary*/ + 1/*not an owner*/;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (persistenceEnabled())
            cleanPersistenceDir();

        Ignite ignite = startGrids(serverNodesCount()); // Server nodes.

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

        if (persistenceEnabled())
            ignite.cluster().state(ClusterState.ACTIVE);

        ignite.getOrCreateCache(cacheConfiguration());

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        log.info("Checked " + iterableKey + " keys");

        stopAllGrids();

        if (persistenceEnabled())
            cleanPersistenceDir();
    }

    /**
     *
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicityMode());
        cfg.setBackups(backupsCount());

        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(32));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        if (persistenceEnabled()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration());
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(100 * 1024 * 1024);
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        }

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
    protected void checkEvent(ReadRepairData data, IgniteIrreparableConsistencyViolationException e) {
        Map<Object, Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo>> evtEntries = new HashMap<>();
        Map<Object, Object> evtFixed = new HashMap<>();

        Map<Integer, InconsistentMapping> inconsistent = data.data.entrySet().stream()
            .filter(entry -> !entry.getValue().consistent)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        while (!evtEntries.keySet().equals(inconsistent.keySet())) {
            if (!evtDeq.isEmpty()) {
                CacheConsistencyViolationEvent evt = evtDeq.remove();

                // Optimistic and read committed transactions produce per key fixes.
                evtEntries.putAll(evt.getEntries());
                evtFixed.putAll(evt.getFixedEntries());
            }
        }

        for (Map.Entry<Integer, InconsistentMapping> mapping : inconsistent.entrySet()) {
            Integer key = mapping.getKey();
            Integer fixed = mapping.getValue().fixed;
            Integer primary = mapping.getValue().primary;
            boolean repairable = mapping.getValue().repairable;

            if (!repairable)
                assertNotNull(e);

            if (e == null) {
                assertTrue(repairable);
                assertTrue(evtFixed.containsKey(key));
                assertEquals(fixed, evtFixed.get(key));
            }
            // Repairable but not repaired (because of irreparable entry at the same tx) entries.
            else if (e.irreparableKeys().contains(key) || (e.repairableKeys() != null && e.repairableKeys().contains(key)))
                assertFalse(evtFixed.containsKey(key));

            Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> evtEntryInfos = evtEntries.get(key);

            if (evtEntryInfos != null)
                for (Map.Entry<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> evtEntryInfo : evtEntryInfos.entrySet()) {
                    ClusterNode node = evtEntryInfo.getKey();
                    CacheConsistencyViolationEvent.EntryInfo info = evtEntryInfo.getValue();

                    if (info.isCorrect())
                        assertEquals(fixed, info.getValue());

                    if (info.isPrimary()) {
                        assertEquals(primary, info.getValue());
                        assertEquals(node, primaryNode(key, DEFAULT_CACHE_NAME).cluster().localNode());
                    }
                }
        }

        int expectedFixedCnt = inconsistent.size() -
            (e != null ? (e.repairableKeys() != null ? e.repairableKeys().size() : 0) + e.irreparableKeys().size() : 0);

        assertEquals(expectedFixedCnt, evtFixed.size());

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

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < rnd.nextInt(1, 10); i++) {
            ReadRepairStrategy[] strategies = ReadRepairStrategy.values();

            ReadRepairStrategy strategy = strategies[rnd.nextInt(strategies.length)];

            Map<Integer, InconsistentMapping> results = new TreeMap<>(); // Sorted to avoid warning.

            try {
                for (int j = 0; j < cnt; j++) {
                    InconsistentMapping res = setDifferentValuesForSameKey(++iterableKey, misses, nulls, strategy);

                    results.put(iterableKey, res);
                }

                for (Ignite node : G.allGrids()) { // Check that cache filled properly.
                    Map<Integer, Integer> all =
                        node.<Integer, Integer>getOrCreateCache(DEFAULT_CACHE_NAME).getAll(results.keySet());

                    for (Map.Entry<Integer, Integer> entry : all.entrySet()) {
                        Integer key = entry.getKey();
                        Integer val = entry.getValue();

                        T2<Integer, GridCacheVersion> valVer = results.get(key).mapping.get(node);

                        Integer exp;

                        if (valVer != null)
                            exp = valVer.get1(); // Should read from itself (backup or primary).
                        else
                            exp = results.get(key).primary; // Or read from primary (when not a partition owner).

                        assertEquals(exp, val);
                    }
                }

                c.accept(new ReadRepairData(cache, results, raw, async, strategy));
            }
            catch (Throwable th) {
                StringBuilder sb = new StringBuilder();

                sb.append("Read Repair test failed [")
                    .append("cache=").append(cache.getName())
                    .append(", strategy=").append(strategy)
                    .append("]\n");

                for (Map.Entry<Integer, InconsistentMapping> entry : results.entrySet()) {
                    sb.append("Key: ").append(entry.getKey()).append("\n");

                    InconsistentMapping mapping = entry.getValue();

                    sb.append(" Random data [primary=").append(mapping.primary)
                        .append(", fixed=").append(mapping.fixed)
                        .append(", repairable=").append(mapping.repairable)
                        .append(", consistent=").append(mapping.consistent)
                        .append("]\n");

                    sb.append("  Distribution: \n");

                    for (Map.Entry<Ignite, T2<Integer, GridCacheVersion>> dist : mapping.mapping.entrySet()) {
                        sb.append("   Node: ").append(dist.getKey().name()).append("\n");
                        sb.append("    Value: ").append(dist.getValue().get1()).append("\n");
                        sb.append("    Version: ").append(dist.getValue().get2()).append("\n");
                    }

                    sb.append("\n");
                }

                throw new Exception(sb.toString(), th);
            }
        }
    }

    /**
     *
     */
    private InconsistentMapping setDifferentValuesForSameKey(int key, boolean misses, boolean nulls,
        ReadRepairStrategy strategy) throws Exception {
        List<Ignite> nodes = new ArrayList<>();
        Map<Ignite, T2<Integer, GridCacheVersion>> mapping = new HashMap<>();

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

        IgniteInternalCache<Integer, Integer> internalCache = (grid(1)).cachex(DEFAULT_CACHE_NAME);

        GridCacheVersionManager mgr = ((GridCacheAdapter)internalCache.cache()).context().shared().versions();

        int incVal = 0;
        Integer primVal = null;
        Collection<T2<Integer, GridCacheVersion>> vals = new ArrayList<>();

        if (misses) {
            List<Ignite> keeped = nodes.subList(0, rnd.nextInt(1, nodes.size()));

            nodes.stream()
                .filter(node -> !keeped.contains(node))
                .forEach(node -> {
                    T2<Integer, GridCacheVersion> nullT2 = new T2<>(null, null);

                    vals.add(nullT2);
                    mapping.put(node, nullT2);
                });  // Recording nulls (missed values).

            nodes = keeped;
        }

        boolean repairable = atomicityMode() != ATOMIC; // Currently, Atomic caches can not be repaired.

        boolean rmvd = false;

        boolean incVer = rnd.nextBoolean();

        GridCacheVersion ver = null;

        for (Ignite node : nodes) {
            IgniteInternalCache<Integer, Integer> cache = ((IgniteEx)node).cachex(DEFAULT_CACHE_NAME);

            GridCacheAdapter<Integer, Integer> adapter = (GridCacheAdapter)cache.cache();

            GridCacheEntryEx entry = adapter.entryEx(key);

            if (ver == null || incVer)
                ver = mgr.next(entry.context().kernalContext().discovery().topologyVersion()); // Incremental version.

            boolean rmv = nulls && (!rmvd || rnd.nextBoolean());

            Integer val = rmv ? null : rnd.nextBoolean()/*increment or same as previously*/ ? ++incVal : incVal;

            T2<Integer, GridCacheVersion> valVer = new T2<>(val, val != null ? ver : null);

            vals.add(valVer);
            mapping.put(node, valVer);

            GridKernalContext kctx = ((IgniteEx)node).context();

            byte[] bytes = kctx.cacheObjects().marshal(entry.context().cacheObjectContext(), rmv ? -1 : val); // Incremental value.

            try {
                kctx.cache().context().database().checkpointReadLock();

                boolean init = entry.initialValue(
                    new CacheObjectImpl(null, bytes),
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

                    rmvd = true;

                    assertFalse(entry.hasValue());
                }
                else
                    assertTrue(entry.hasValue());

                assertTrue("iterableKey " + key + " already inited", init);

                if (node.equals(primary))
                    primVal = val;
            }
            finally {
                ((IgniteEx)node).context().cache().context().database().checkpointReadUnlock();
            }
        }

        assertEquals(vals.size(), mapping.size());
        assertEquals(vals.size(),
            internalCache.configuration().getCacheMode() == REPLICATED ? serverNodesCount() : backupsCount() + 1);

        if (!misses && !nulls)
            assertTrue(primVal != null); // Primary value set.

        Integer fixed;

        boolean consistent;

        if (vals.stream().distinct().count() == 1) { // Consistent value.
            consistent = true;
            repairable = true;
            fixed = vals.iterator().next().getKey();
        }
        else {
            consistent = false;

            switch (strategy) {
                case LWW:
                    if (misses || rmvd || !incVer) {
                        repairable = false;

                        fixed = Integer.MIN_VALUE; // Should never be returned.
                    }
                    else
                        fixed = incVal;

                    break;

                case PRIMARY:
                    fixed = primVal;

                    break;

                case RELATIVE_MAJORITY:
                    fixed = Integer.MIN_VALUE; // Should never be returned.

                    Map<T2<Integer, GridCacheVersion>, Integer> counts = new HashMap<>();

                    for (T2<Integer, GridCacheVersion> val : vals) {
                        counts.putIfAbsent(val, 0);

                        counts.compute(val, (k, v) -> v + 1);
                    }

                    int[] sorted = counts.values().stream().sorted(Comparator.reverseOrder()).mapToInt(v -> v).toArray();

                    int max = sorted[0];

                    if (sorted.length > 1 && sorted[1] == max)
                        repairable = false;

                    if (repairable)
                        for (Map.Entry<T2<Integer, GridCacheVersion>, Integer> count : counts.entrySet())
                            if (count.getValue().equals(max)) {
                                fixed = count.getKey().getKey();

                                break;
                            }

                    break;

                case REMOVE:
                    fixed = null;

                    break;

                case CHECK_ONLY:
                    repairable = false;

                    fixed = Integer.MIN_VALUE; // Should never be returned.

                    break;

                default:
                    throw new UnsupportedOperationException(strategy.toString());
            }
        }

        return new InconsistentMapping(mapping, primVal, fixed, repairable, consistent);
    }

    /**
     *
     */
    protected static final class ReadRepairData {
        /** Initiator's cache. */
        final IgniteCache<Integer, Integer> cache;

        /** Generated data across topology per key mapping. */
        final Map<Integer, InconsistentMapping> data;

        /** Raw read flag. True means required GetEntry() instead of get(). */
        final boolean raw;

        /** Async read flag. */
        final boolean async;

        /** Strategy. */
        final ReadRepairStrategy strategy;

        /**
         *
         */
        public ReadRepairData(
            IgniteCache<Integer, Integer> cache,
            Map<Integer, InconsistentMapping> data,
            boolean raw,
            boolean async,
            ReadRepairStrategy strategy) {
            this.cache = cache;
            this.data = data;
            this.raw = raw;
            this.async = async;
            this.strategy = strategy;
        }

        /**
         *
         */
        boolean repairable() {
            return data.values().stream().allMatch(mapping -> mapping.repairable);
        }
    }

    /**
     *
     */
    protected static final class InconsistentMapping {
        /** Value per node. */
        final Map<Ignite, T2<Integer, GridCacheVersion>> mapping;

        /** Primary node's value. */
        final Integer primary;

        /** Expected fix result. */
        final Integer fixed;

        /** Inconsistency can be repaired using the specified strategy. */
        final boolean repairable;

        /** Consistent value. */
        final boolean consistent;

        /**
         *
         */
        public InconsistentMapping(Map<Ignite, T2<Integer, GridCacheVersion>> mapping, Integer primary, Integer fixed,
            boolean repairable, boolean consistent) {
            this.mapping = new HashMap<>(mapping);
            this.primary = primary;
            this.fixed = fixed;
            this.repairable = repairable;
            this.consistent = consistent;
        }
    }
}
