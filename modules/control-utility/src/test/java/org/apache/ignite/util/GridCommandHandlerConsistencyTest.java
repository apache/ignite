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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask.CONSISTENCY_VIOLATIONS_FOUND;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 *
 */
public class GridCommandHandlerConsistencyTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Default cache name atomic. */
    private static final String DEFAULT_CACHE_NAME_ATOMIC = DEFAULT_CACHE_NAME + "Atomic";

    /** Default cache name tx. */
    private static final String DEFAULT_CACHE_NAME_TX = DEFAULT_CACHE_NAME + "Tx";

    /** Default cache name filtered. */
    private static final String DEFAULT_CACHE_NAME_FILTERED = DEFAULT_CACHE_NAME + "Filtered";

    /** Partitions. */
    private static final int PARTITIONS = 32;

    /**
     *
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(boolean tx) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(
            tx ? DEFAULT_CACHE_NAME_TX : DEFAULT_CACHE_NAME_ATOMIC);

        cfg.setAtomicityMode(tx ? TRANSACTIONAL : ATOMIC);
        cfg.setBackups(2);
        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(null);
        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);

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

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
        assertContains(log, testOut.toString(),
            "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts.get());

        readRepairTx(brokenParts, txCacheName);

        assertEquals(PARTITIONS, brokenParts.get()); // Half fixed.

        readRepaitAtomic(brokenParts, atomicCacheName);

        assertEquals(PARTITIONS, brokenParts.get()); // Atomics still broken.
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

        readRepairTx(brokenParts, cacheName);

        assertEquals(0, brokenParts.get());
    }

    /**
     *
     */
    private void readRepairTx(AtomicInteger brokenParts, String cacheName) {
        for (int i = 0; i < PARTITIONS; i++) {
            assertEquals(EXIT_CODE_OK, execute("--consistency", "repair", cacheName, String.valueOf(i)));
            assertContains(log, testOut.toString(), CONSISTENCY_VIOLATIONS_FOUND);
            assertContains(log, testOut.toString(), "[found=1, fixed=1]");

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

            brokenParts.decrementAndGet();

            if (brokenParts.get() > 0)
                assertContains(log, testOut.toString(),
                    "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts);
            else
                assertContains(log, testOut.toString(), "no conflicts have been found");
        }
    }

    /**
     *
     */
    private void readRepaitAtomic(AtomicInteger brokenParts, String cacheName) {
        for (int i = 0; i < PARTITIONS; i++) { // This may be a copy of previous (tx case), implement atomic repair to make this happen :)
            assertEquals(EXIT_CODE_OK, execute("--consistency", "repair", cacheName, String.valueOf(i)));
            assertContains(log, testOut.toString(), CONSISTENCY_VIOLATIONS_FOUND);
            assertContains(log, testOut.toString(), "[found=1, fixed=0]"); // Nothing fixed.

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
            assertContains(log, testOut.toString(),
                "conflict partitions has been found: [counterConflicts=0, hashConflicts=" + brokenParts); // Nothing fixed.
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
