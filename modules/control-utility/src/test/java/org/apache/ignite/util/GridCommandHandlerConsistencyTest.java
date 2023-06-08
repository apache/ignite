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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyStatusTask;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask.CONSISTENCY_VIOLATIONS_FOUND;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridCommandHandlerConsistencyTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Default cache name atomic. */
    private static final String DEFAULT_CACHE_NAME_ATOMIC = DEFAULT_CACHE_NAME + "Atomic";

    /** Default cache name tx. */
    private static final String DEFAULT_CACHE_NAME_TX = DEFAULT_CACHE_NAME + "Tx";

    /** Default cache name filtered. */
    private static final String DEFAULT_CACHE_NAME_FILTERED = DEFAULT_CACHE_NAME + "Filtered";

    /** Group postfix. */
    private static final String GRP_POSTFIX = "_grp";

    /** */
    public static final String CACHE = "--cache";

    /** */
    public static final String STRATEGY = "--strategy";

    /** */
    public static final String PARTITIONS_ARG = "--partitions";

    /** Partitions. */
    private static final int PARTITIONS = 32;

    /** Backups. */
    private static final int BACKUPS = 2;

    /** */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    @Parameterized.Parameters(name = "strategy={0}, explicitGrp={1}, callByGrp={2}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (ReadRepairStrategy strategy : ReadRepairStrategy.values()) {
            for (boolean explicitGrp : new boolean[] {false, true}) {
                if (explicitGrp)
                    res.add(new Object[] {strategy, explicitGrp, true});

                res.add(new Object[] {strategy, explicitGrp, false});
            }
        }

        return res;
    }

    /**
     *
     */
    @Parameterized.Parameter
    public ReadRepairStrategy strategy;

    /**
     * True when cache defined via group.
     */
    @Parameterized.Parameter(1)
    public boolean explicitGrp;

    /**
     * True when cache consistency repair called by group name.
     */
    @Parameterized.Parameter(2)
    public boolean callByGrp;

    /**
     *
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(boolean tx) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(
            tx ? DEFAULT_CACHE_NAME_TX : DEFAULT_CACHE_NAME_ATOMIC);

        cfg.setAtomicityMode(tx ? TRANSACTIONAL : ATOMIC);
        cfg.setBackups(BACKUPS);
        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));

        if (explicitGrp)
            cfg.setGroupName(cfg.getName() + GRP_POSTFIX);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(null);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testAtomicAndTxValueAndVersion() throws Exception {
        testAtomicAndTx(true);
    }

    /**
     *
     */
    @Test
    public void testAtomicAndTxVersionOnly() throws Exception {
        testAtomicAndTx(false);
    }

    /**
     * @param incVal Increment value.
     */
    private void testAtomicAndTx(boolean incVal) throws Exception {
        IgniteEx ignite = startGrids(4);

        String txCacheName = ignite.getOrCreateCache(cacheConfiguration(true)).getName();
        String atomicCacheName = ignite.getOrCreateCache(cacheConfiguration(false)).getName();

        fillCache(txCacheName, null, incVal);
        fillCache(atomicCacheName, null, incVal);

        AtomicInteger brokenParts = new AtomicInteger(PARTITIONS * 2);

        injectTestSystemOut();

        int repairsPerEntry = repairsPerEntry();

        int copies = BACKUPS + 1;

        int timesMultiplicator = repairsPerEntry == 0 ?
            copies : // N times, on each check.
            1; // Once, on fix.

        LogListener lsnrUnmaskedKey =
            matches("Key: 0 (cache: ").times(2/*tx + atomic*/ * timesMultiplicator).build();
        LogListener lsnrMaskedKey =
            matches("Key: [HIDDEN_KEY#").times(brokenParts.get() * timesMultiplicator).build();
        LogListener lsnrMaskedVal =
            matches("Value: [HIDDEN_VALUE#").times(brokenParts.get() * copies * timesMultiplicator).build();

        listeningLog.registerListener(lsnrUnmaskedKey);
        listeningLog.registerListener(lsnrMaskedKey);
        listeningLog.registerListener(lsnrMaskedVal);

        List<LogListener> listeners = new ArrayList<>();

        // It's unable to check just "Key:" count while https://issues.apache.org/jira/browse/IGNITE-15316 not fixed
        if (S.includeSensitive()) {
            for (int i = 0; i < PARTITIONS; i++) {
                LogListener keyListener = matches("Key: " + i + " (cache: ").build();

                listeningLog.registerListener(keyListener);

                listeners.add(keyListener);
            }
        }

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
        assertContains(log, testOut.toString(),
            "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts.get());

        readRepair(brokenParts, txCacheName, repairsPerEntry);

        if (S.includeSensitive()) {
            for (LogListener listener : listeners) {
                assertTrue(listener.check());

                listener.reset();
            }
        }

        if (repairsPerEntry > 0)
            assertEquals(PARTITIONS, brokenParts.get()); // Half repaired.

        readRepair(brokenParts, atomicCacheName, repairsPerEntry);

        if (S.includeSensitive()) {
            for (LogListener listener : listeners)
                assertTrue(listener.check());
        }

        if (repairsPerEntry > 0)
            assertEquals(0, brokenParts.get()); // Another half repaired.

        assertEquals(S.includeSensitive(), lsnrUnmaskedKey.check());
        assertEquals(S.includeSensitive(), !lsnrMaskedKey.check());
        assertEquals(S.includeSensitive(), !lsnrMaskedVal.check());
    }

    /**
     *
     */
    @Test
    public void testCacheFilter() throws Exception {
        IgniteEx ignite = startGrids(3);

        Ignite filtered = grid(2);
        Object filteredId = filtered.cluster().localNode().consistentId();

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME_FILTERED);

        cfg.setAtomicityMode(TRANSACTIONAL); // Possible to repair.
        cfg.setBackups(2);
        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));
        cfg.setNodeFilter(node -> !node.consistentId().equals(filteredId));

        if (explicitGrp)
            cfg.setGroupName(cfg.getName() + GRP_POSTFIX);

        String cacheName = ignite.getOrCreateCache(cfg).getName();

        fillCache(cacheName, filtered, true);

        // Another cache without nodeFilter required to perform idle_verify check.
        // See https://issues.apache.org/jira/browse/IGNITE-15327 for details.
        ignite.getOrCreateCache(cacheConfiguration(true)).getName();

        injectTestSystemOut();

        AtomicInteger brokenParts = new AtomicInteger(PARTITIONS);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
        assertContains(log, testOut.toString(),
            "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts.get());

        int repairsPerEntry = repairsPerEntry();

        readRepair(brokenParts, cacheName, repairsPerEntry);

        assertEquals(repairsPerEntry > 0 ? 0 : PARTITIONS, brokenParts.get());
    }

    /**
     *
     */
    @Test
    public void testRepairNonExistentCache() throws Exception {
        startGrids(3);

        injectTestSystemOut();

        for (int i = 0; i < PARTITIONS; i++) {
            assertEquals(EXIT_CODE_UNEXPECTED_ERROR,
                execute("--consistency", "repair",
                    CACHE, "non-existent",
                    PARTITIONS_ARG, String.valueOf(i),
                    STRATEGY, strategy.toString()));

            assertTrue(VisorConsistencyStatusTask.MAP.isEmpty());

            assertContains(log, testOut.toString(), "Cache (or cache group) not found");
        }
    }

    /**
     *
     */
    private void readRepair(AtomicInteger brokenParts, String cacheName, Integer repairsPerEntry) {
        int i = 0;

        while (i < PARTITIONS) {
            int from = i;

            i = Math.min(i + ThreadLocalRandom.current().nextInt(1, 10), PARTITIONS);

            assertEquals(EXIT_CODE_OK, execute("--consistency", "repair",
                CACHE, callByGrp ? cacheName + GRP_POSTFIX : cacheName,
                PARTITIONS_ARG,
                    IntStream.range(from, i).mapToObj(Integer::toString).collect(Collectors.joining(",")),
                STRATEGY, strategy.toString()));

            assertTrue(VisorConsistencyStatusTask.MAP.isEmpty());

            assertContains(log, testOut.toString(), CONSISTENCY_VIOLATIONS_FOUND);
            assertContains(log, testOut.toString(), "[found=1, repaired=" + repairsPerEntry.toString());

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

            if (repairsPerEntry > 0) {
                brokenParts.addAndGet(-(i - from));

                if (brokenParts.get() > 0)
                    assertContains(log, testOut.toString(),
                        "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts);
                else
                    assertContains(log, testOut.toString(), "no conflicts have been found");
            }
            else {
                assertContains(log, testOut.toString(),
                    "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts); // Nothing repaired.
            }
        }
    }

    /**
     *
     */
    private int repairsPerEntry() {
        switch (strategy) {
            case PRIMARY:
            case REMOVE:
            case LWW: // Each filled value has an incremental version. Last versioned value will win.
                return 1;

            case CHECK_ONLY:
            case RELATIVE_MAJORITY: // Each filled value has incremental version. Each value is unique. Winner is absent.
                return 0;

            default:
                throw new UnsupportedOperationException("Unsupported strategy");
        }
    }

    /**
     *
     */
    private void fillCache(String name, Ignite filtered, boolean incVal) throws Exception {
        for (Ignite node : G.allGrids()) {
            if (node.equals(filtered))
                continue;

            while (((IgniteEx)node).cachex(name) == null) // Waiting for cache internals to init.
                U.sleep(1);
        }

        GridCacheVersionManager mgr =
            ((GridCacheAdapter)(grid(1)).cachex(name).cache()).context().shared().versions();

        for (int key = 0; key < PARTITIONS; key++) {
            List<Ignite> nodes = new ArrayList<>();

            nodes.add(primaryNode(key, name));
            nodes.addAll(backupNodes(key, name));

            Collections.shuffle(nodes);

            int val = key;
            Object obj;

            for (Ignite node : nodes) {
                IgniteInternalCache cache = ((IgniteEx)node).cachex(name);

                GridCacheAdapter adapter = ((GridCacheAdapter)cache.cache());

                GridCacheEntryEx entry = adapter.entryEx(key);

                val = incVal ? ++val : val;

                if (binaryCache()) {
                    BinaryObjectBuilder builder = node.binary().builder("org.apache.ignite.TestValue");

                    builder.setField("val", val);

                    obj = builder.build();
                }
                else
                    obj = val;

                boolean init = entry.initialValue(
                    new CacheObjectImpl(obj, null), // Incremental or same value.
                    mgr.next(entry.context().kernalContext().discovery().topologyVersion()), // Incremental version.
                    0,
                    0,
                    false,
                    AffinityTopologyVersion.NONE,
                    GridDrType.DR_NONE,
                    false,
                    false);

                assertTrue("iterableKey " + key + " already inited", init);
            }
        }
    }

    /**
     * Cache should be filled with binary objects.
     */
    protected boolean binaryCache() {
        return false;
    }
}
