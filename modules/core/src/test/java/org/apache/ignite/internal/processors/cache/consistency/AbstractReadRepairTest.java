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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.InconsistentMapping;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteIrreparableConsistencyViolationException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 *
 */
public abstract class AbstractReadRepairTest extends GridCommonAbstractTest {
    /** Partitions. */
    protected static final int PARTITIONS = 32;

    /** Events. */
    private static final ConcurrentLinkedDeque<CacheConsistencyViolationEvent> evtDeq = new ConcurrentLinkedDeque<>();

    /** External class loader. */
    private static final ClassLoader extClsLdr = getExternalClassLoader();

    /** Use external class loader. */
    private static volatile boolean useExtClsLdr;

    /** Nodes aware of the entry value class. */
    protected static volatile List<Ignite> clsAwareNodes;

    /** Generator. */
    private static volatile ReadRepairDataGenerator gen;

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

        Ignite ignite = startGrids(serverNodesCount() / 2); // Server nodes.

        useExtClsLdr = true;

        List<Ignite> extClsLdrNodes = new ArrayList<>();

        while (G.allGrids().size() < serverNodesCount())
            extClsLdrNodes.add(startGrid(G.allGrids().size())); // Server nodes.

        extClsLdrNodes.add(startClientGrid(G.allGrids().size())); // Client node 1.

        clsAwareNodes = extClsLdrNodes;

        useExtClsLdr = false;

        startClientGrid(G.allGrids().size()); // Client node 2.

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

        gen = new ReadRepairDataGenerator(
            DEFAULT_CACHE_NAME,
            clsAwareNodes,
            extClsLdr,
            this::primaryNode,
            this::backupNodes,
            this::serverNodesCount,
            this::backupsCount);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        log.info("Checked " + gen.generated() + " keys");

        stopAllGrids();

        if (persistenceEnabled())
            cleanPersistenceDir();
    }

    /**
     *
     */
    protected CacheConfiguration<Integer, Object> cacheConfiguration() {
        CacheConfiguration<Integer, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicityMode());
        cfg.setBackups(backupsCount());

        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));

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

        if (useExtClsLdr)
            cfg.setClassLoader(extClsLdr);

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
    protected void checkEvent(ReadRepairData rrd, IgniteIrreparableConsistencyViolationException e) {
        Map<Object, Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo>> evtEntries = new HashMap<>();
        Map<Object, Object> evtRepaired = new HashMap<>();

        Map<Integer, InconsistentMapping> inconsistent = rrd.data.entrySet().stream()
            .filter(entry -> !entry.getValue().consistent)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        while (!evtEntries.keySet().equals(inconsistent.keySet())) {
            if (!evtDeq.isEmpty()) {
                CacheConsistencyViolationEvent evt = evtDeq.remove();

                assertEquals(rrd.strategy, evt.getStrategy());

                // Optimistic and read committed transactions produce per key repair.
                for (Map.Entry<Object, CacheConsistencyViolationEvent.EntriesInfo> entries : evt.getEntries().entrySet()) {
                    assertFalse(evtEntries.containsKey(entries.getKey())); // Checking that duplicates are absent.

                    evtEntries.put(entries.getKey(), entries.getValue().getMapping());
                }

                evtRepaired.putAll(evt.getRepairedEntries());
            }
        }

        for (Map.Entry<Integer, InconsistentMapping> mapping : inconsistent.entrySet()) {
            Integer key = mapping.getKey();
            Object repairedBin = mapping.getValue().repairedBin;
            Object primaryBin = mapping.getValue().primaryBin;
            boolean repairable = mapping.getValue().repairable;

            if (!repairable)
                assertNotNull(e);

            if (e == null) {
                assertTrue(repairable);
                assertTrue(evtRepaired.containsKey(key));

                assertEqualsArraysAware(repairedBin, evtRepaired.get(key));
            }
            // Repairable but not repaired (because of irreparable entry at the same tx) entries.
            else if (e.irreparableKeys().contains(key) || (e.repairableKeys() != null && e.repairableKeys().contains(key)))
                assertFalse(evtRepaired.containsKey(key));

            Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> evtEntryInfos = evtEntries.get(key);

            if (evtEntryInfos != null)
                for (Map.Entry<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> evtEntryInfo : evtEntryInfos.entrySet()) {
                    ClusterNode node = evtEntryInfo.getKey();
                    CacheConsistencyViolationEvent.EntryInfo info = evtEntryInfo.getValue();

                    Object val = info.getValue();

                    if (info.isCorrect())
                        assertEqualsArraysAware(repairedBin, val);

                    if (info.isPrimary()) {
                        assertEqualsArraysAware(primaryBin, val);
                        assertEquals(node, primaryNode(key, DEFAULT_CACHE_NAME).cluster().localNode());
                    }
                }
        }

        int irreparableCnt = 0;
        int repairableCnt = 0;

        if (e != null) {
            irreparableCnt = e.irreparableKeys().size();
            repairableCnt = e.repairableKeys() != null ? e.repairableKeys().size() : 0;
        }

        if (repairableCnt > 0)
            // Mentioned when pessimistic tx read-repair get contains irreparable entries,
            // and it's impossible to repair repairable entries during this call.
            assertEquals(TRANSACTIONAL, atomicityMode());

        int expectedRepairedCnt = inconsistent.size() - (irreparableCnt + repairableCnt);

        assertEquals(expectedRepairedCnt, evtRepaired.size());

        assertTrue(evtDeq.isEmpty());
    }

    /**
     * @param obj Object.
     */
    protected static Object unwrapBinaryIfNeeded(Object obj) {
        return ReadRepairDataGenerator.unwrapBinaryIfNeeded(obj);
    }

    /**
     *
     */
    protected void generateAndCheck(
        Ignite initiator,
        int cnt,
        boolean raw,
        boolean async,
        boolean misses,
        boolean nulls,
        boolean binary,
        Consumer<ReadRepairData> c) throws Exception {
        gen.generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            null,
            c);
    }
}
