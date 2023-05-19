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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.InconsistentMapping;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.KillCommandsControlShTest.PARALLEL;

/**
 *
 */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "true")
public class GridCommandHandlerConsistencyRepairCorrectnessTransactionalTest extends GridCommandHandlerAbstractTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "misses={0}, nulls={1}, strategy={2}, parallel={3}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (boolean misses : new boolean[] {false, true}) {
            for (boolean nulls : new boolean[] {false, true}) {
                for (ReadRepairStrategy strategy : ReadRepairStrategy.values()) {
                    for (boolean parallel : new boolean[] {false, true}) {
                        if (parallel && strategy != ReadRepairStrategy.CHECK_ONLY)
                            continue; // see https://issues.apache.org/jira/browse/IGNITE-15316

                        res.add(new Object[] {misses, nulls, strategy, parallel});
                    }
                }
            }
        }

        return res;
    }

    /** Misses. */
    @Parameterized.Parameter
    public boolean misses;

    /** Nulls. */
    @Parameterized.Parameter(1)
    public boolean nulls;

    /** Strategy. */
    @Parameterized.Parameter(2)
    public ReadRepairStrategy strategy;

    /** Parallel consistency check. */
    @Parameterized.Parameter(3)
    public boolean parallel;

    /** Partitions. */
    private static final int PARTITIONS = 8;

    /** Caches. */
    private static final int CACHES = 3;

    /** Group name. */
    private static final String GROUP_NAME = "grp";

    /** External class loader. */
    private static final ClassLoader extClsLdr = getExternalClassLoader();

    /** Use external class loader. */
    private static volatile boolean useExtClsLdr;

    /** Nodes aware of the entry value class. */
    protected static volatile List<Ignite> clsAwareNodes;

    /** Generator. */
    private static volatile ReadRepairDataGenerator cacheGen;

    /** Generator. */
    private static volatile ReadRepairDataGenerator grpGen;

    /** Repair the whole group by its name. */
    private volatile boolean repairByGrp;

    /** Repair the whole group by a single cache name. */
    private volatile boolean repairBySingleCacheName;

    /** Listening logger. */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** Atomicy mode. */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** Backups count. */
    protected Integer backupsCount() {
        return 3;
    }

    /** Server nodes count. */
    private int serverNodesCount() {
        return backupsCount() + 1/*primary*/ + 1/*not an owner*/;
    }

    /**
     *
     */
    protected Collection<CacheConfiguration> cacheConfigurations() {
        Collection<CacheConfiguration> cfgs = new ArrayList<>();

        for (int i = 0; i < CACHES; i++) {
            CacheConfiguration<Integer, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + i);

            cfg.setAtomicityMode(atomicityMode());
            cfg.setBackups(backupsCount());
            cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));

            cfgs.add(cfg);
        }

        return cfgs;
    }

    /**
     *
     */
    protected Collection<CacheConfiguration> cacheWithGroupsConfigurations() {
        Collection<CacheConfiguration> cfgs = new ArrayList<>();

        for (int i = 0; i < CACHES; i++) {
            CacheConfiguration<Integer, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + "grp" + i);

            cfg.setAtomicityMode(atomicityMode());
            cfg.setBackups(backupsCount());
            cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));
            cfg.setGroupName(GROUP_NAME);

            cfgs.add(cfg);
        }

        return cfgs;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog);

        if (useExtClsLdr)
            cfg.setClassLoader(extClsLdr);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

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

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.getOrCreateCaches(cacheConfigurations());
        ignite.getOrCreateCaches(cacheWithGroupsConfigurations());

        awaitPartitionMapExchange();

        cacheGen = new ReadRepairDataGenerator(
            cacheConfigurations().stream().map(CacheConfiguration::getName).toArray(String[]::new),
            clsAwareNodes,
            extClsLdr,
            this::primaryNode,
            this::backupNodes,
            this::serverNodesCount,
            this::backupsCount);

        grpGen = new ReadRepairDataGenerator(
            cacheWithGroupsConfigurations().stream().map(CacheConfiguration::getName).toArray(String[]::new),
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

        log.info("Checked [cache] " + cacheGen.generated() + " keys");
        log.info("Checked [group] " + grpGen.generated() + " keys");

        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testCachesRepair() throws Exception {
        test(cacheGen);
    }

    /**
     *
     */
    @Test
    public void testGroupRepairByCacheNames() throws Exception {
        test(grpGen);
    }

    /**
     *
     */
    @Test
    public void testGroupRepairBySingleCacheName() throws Exception {
        repairBySingleCacheName = true;

        test(grpGen);
    }

    /**
     *
     */
    @Test
    public void testGroupRepairByItsName() throws Exception {
        repairByGrp = true;

        test(grpGen);
    }

    /**
     *
     */
    private void test(ReadRepairDataGenerator gen) throws Exception {
        assertFalse(clsAwareNodes.isEmpty());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (Ignite initiator : clsAwareNodes) {
            gen.generateAndCheck(
                initiator,
                rnd.nextInt(10, 100),
                false,
                false,
                misses,
                nulls,
                false,
                strategy,
                null,
                this::repairAndCheck);
        }
    }

    /**
     *
     */
    protected final void repairAndCheck(Collection<ReadRepairData> rrds) {
        assertTrue(rrds.size() >= 2); // Checking that generator provides the data.

        Set<String> caches = repairByGrp ?
            Collections.singleton(GROUP_NAME) :
            rrds.stream().map(data -> data.cache.getName()).collect(Collectors.toSet());

        if (misses || nulls) // Generation may cause inconsistent counters.
            // Performing finalization prior to the repair to check that repair cause no counters desync.
            assertEquals(EXIT_CODE_OK, execute("--consistency", "finalize")); // Fixing partitions update counters.

        for (int i = 0; i < PARTITIONS; i++) {
            for (String cacheName : caches) {
                List<String> cmd = new ArrayList<>(Arrays.asList(
                    "--consistency", "repair",
                    KillCommandsControlShTest.CACHE, cacheName,
                    KillCommandsControlShTest.PARTITIONS, String.valueOf(i),
                    KillCommandsControlShTest.STRATEGY, strategy.toString()));

                if (parallel)
                    cmd.add(PARALLEL);

                assertEquals(EXIT_CODE_OK, execute(cmd));

                if (repairBySingleCacheName)
                    break;
            }
        }

        for (ReadRepairData rrd : rrds) {
            IgniteCache<Object, Object> cache = rrd.cache;

            for (Map.Entry<Object, InconsistentMapping> dataEntry : rrd.data.entrySet()) {
                Object key = dataEntry.getKey();
                InconsistentMapping mapping = dataEntry.getValue();

                if (mapping.repairable) {
                    Object repaired = cacheGen.unwrapBinaryIfNeeded(mapping.repairedBin);

                    // Regular get (form primary or backup or client node).
                    assertEqualsArraysAware("Checking key=" + key, repaired, cache.get(key));

                    // All copies check.
                    assertEqualsArraysAware("Checking key=" + key, repaired,
                        cache.withReadRepair(ReadRepairStrategy.CHECK_ONLY).get(key));
                }
                else if (!mapping.consistent) {
                    // Removing irreparable.
                    // Otherwice subsequent consistency repairs over this partition will regenerate the warning.
                    cache.withReadRepair(ReadRepairStrategy.REMOVE).get(key);

                    assertNull("Checking key=" + key, cache.get(key));
                }
            }
        }

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");
    }
}
