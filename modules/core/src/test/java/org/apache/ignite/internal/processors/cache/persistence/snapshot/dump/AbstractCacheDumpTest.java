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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.Dump;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpedPartitionIterator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.management.api.CommandMBean.INVOKE;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_RUNNING_DIR_KEY;
import static org.apache.ignite.platform.model.AccessLevel.SUPER;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractCacheDumpTest extends GridCommonAbstractTest {
    /** */
    public static final String GRP = "grp";

    /** */
    public static final String CACHE_0 = "cache-0";

    /** */
    public static final String CACHE_1 = "cache-1";

    /** */
    public static final int KEYS_CNT = 1000;

    /** */
    public static final String DMP_NAME = "dump";

    /** */
    protected static final IntFunction<User> USER_FACTORY = i ->
        new User(i, ACL.values()[Math.abs(i) % ACL.values().length], new Role("Role" + i, SUPER));

    /** */
    @Parameterized.Parameter
    public int nodes;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean persistence;

    /** */
    @Parameterized.Parameter(3)
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameters(name = "nodes={0},backups={1},persistence={2},mode={3}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (int nodes : new int[]{1, 3})
            for (int backups : new int[]{0, 1})
                for (boolean persistence : new boolean[]{true, false})
                    for (CacheAtomicityMode mode : CacheAtomicityMode._values()) {
                        if (nodes == 1 && backups != 0)
                            continue;

                        params.add(new Object[]{nodes, backups, persistence, mode});
                    }

        return params;
    }

    /** */
    protected int snpPoolSz = 1;

    /** */
    protected IgniteEx cli;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSnapshotThreadPoolSize(snpPoolSz)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)))
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName(DEFAULT_CACHE_NAME)
                    .setBackups(backups)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20)),
                new CacheConfiguration<>()
                    .setGroupName(GRP)
                    .setName(CACHE_0)
                    .setBackups(backups)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20)),
                new CacheConfiguration<>()
                    .setGroupName(GRP)
                    .setName(CACHE_1)
                    .setBackups(backups)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20))
            );
    }

    /** */
    protected IgniteEx startGridAndFillCaches() throws Exception {
        IgniteEx ign = (IgniteEx)startGridsMultiThreaded(nodes);

        cli = startClientGrid(nodes);

        ign.cluster().state(ClusterState.ACTIVE);

        putData(ign.cache(DEFAULT_CACHE_NAME), ign.cache(CACHE_0), ign.cache(CACHE_1));

        return ign;
    }

    /** */
    protected T2<CountDownLatch, IgniteInternalFuture<?>> runDumpAsyncAndStopBeforeStart() throws IgniteInterruptedCheckedException {
        CountDownLatch latch = new CountDownLatch(1);

        List<Ignite> ignites = Ignition.allGrids();

        for (Ignite ign : ignites) {
            ((IgniteEx)ign).context().pools().getSnapshotExecutorService().submit(() -> {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            });
        }

        IgniteInternalFuture<Object> dumpFut = runAsync(() -> createDump((IgniteEx)ignites.get(0)));

        // Waiting while dump will be setup: task planned after change listener set.
        assertTrue(waitForCondition(() -> {
            for (Ignite ign : ignites) {
                if (ign.configuration().isClientMode() == Boolean.TRUE)
                    continue;

                if (((ThreadPoolExecutor)((IgniteEx)ign).context().pools().getSnapshotExecutorService()).getTaskCount() <= 1)
                    return false;
            }

            return true;
        }, 10 * 1000));

        return new T2<>(latch, dumpFut);
    }

    /** */
    protected void putData(
        IgniteCache<Object, Object> cache,
        IgniteCache<Object, Object> grpCache0,
        IgniteCache<Object, Object> grpCache1
    ) {
        IntStream.range(0, KEYS_CNT).forEach(i -> {
            cache.put(i, i);
            grpCache0.put(i, USER_FACTORY.apply(i));
            grpCache1.put(new Key(i), new Value(String.valueOf(i)));
        });
    }

    /** */
    void checkDump(IgniteEx ign) throws Exception {
        checkDump(ign, DMP_NAME);
    }

    /** */
    void checkDump(IgniteEx ign, String name) throws Exception {
        checkDumpWithCommand(ign, name);

        if (persistence)
            assertNull(ign.context().cache().context().database().metaStorage().read(SNP_RUNNING_DIR_KEY));

        Dump dump = dump(ign, name);

        List<SnapshotMetadata> metadata = dump.metadata();

        assertNotNull(metadata);
        assertEquals(nodes, metadata.size());

        for (SnapshotMetadata meta : metadata) {
            assertEquals(name, meta.snapshotName());
            assertTrue(meta.dump());
        }

        List<String> nodesDirs = dump.nodesDirectories();

        assertEquals(nodes, nodesDirs.size());

        Set<Integer> keys = new HashSet<>();
        int dfltDumpSz = 0;
        int grpDumpSz = 0;

        CacheObjectContext coCtx = ign.context().cache().context().cacheObjectContext(CU.cacheId(DEFAULT_CACHE_NAME));
        CacheObjectContext coCtx0 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_0));
        CacheObjectContext coCtx1 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_1));

        for (String nodeDir : nodesDirs) {
            List<StoredCacheData> ccfgs = dump.configs(nodeDir, CU.cacheId(DEFAULT_CACHE_NAME));

            assertNotNull(ccfgs);
            assertEquals(1, ccfgs.size());

            assertEquals(DEFAULT_CACHE_NAME, ccfgs.get(0).configuration().getName());
            assertFalse(ccfgs.get(0).sql());
            assertTrue(ccfgs.get(0).queryEntities().isEmpty());

            ccfgs = dump.configs(nodeDir, CU.cacheId(GRP));

            assertNotNull(ccfgs);
            assertEquals(2, ccfgs.size());

            ccfgs.sort(Comparator.comparing(d -> d.config().getName()));

            CacheConfiguration ccfg0 = ccfgs.get(0).configuration();
            CacheConfiguration ccfg1 = ccfgs.get(1).configuration();

            assertEquals(GRP, ccfg0.getGroupName());
            assertEquals(CACHE_0, ccfg0.getName());

            assertEquals(GRP, ccfg1.getGroupName());
            assertEquals(CACHE_1, ccfg1.getName());

            assertFalse(ccfgs.get(0).sql());
            assertFalse(ccfgs.get(1).sql());
            assertTrue(ccfgs.get(0).queryEntities().isEmpty());
            assertTrue(ccfgs.get(1).queryEntities().isEmpty());

            List<Integer> parts = dump.partitions(nodeDir, CU.cacheId(DEFAULT_CACHE_NAME));

            for (int part : parts) {
                try (DumpedPartitionIterator iter = dump.iterator(nodeDir, CU.cacheId(DEFAULT_CACHE_NAME), part)) {
                    while (iter.hasNext()) {
                        DumpEntry e = iter.next();

                        checkDefaultCacheEntry(e, coCtx);

                        keys.add(e.key().<Integer>value(coCtx, true));

                        dfltDumpSz++;
                    }
                }
            }

            parts = dump.partitions(nodeDir, CU.cacheId(GRP));

            for (int part : parts) {
                try (DumpedPartitionIterator iter = dump.iterator(nodeDir, CU.cacheId(GRP), part)) {
                    while (iter.hasNext()) {
                        DumpEntry e = iter.next();

                        checkGroupEntry(e, coCtx0, coCtx1);

                        grpDumpSz++;
                    }
                }
            }
        }

        assertEquals(KEYS_CNT + KEYS_CNT * backups, dfltDumpSz);
        assertEquals(2 * (KEYS_CNT + KEYS_CNT * backups), grpDumpSz);

        IntStream.range(0, KEYS_CNT).forEach(key -> assertTrue(keys.contains(key)));
    }

    /** */
    protected void checkDefaultCacheEntry(DumpEntry e, CacheObjectContext coCtx) {
        assertNotNull(e);

        Integer key = e.key().<Integer>value(coCtx, true);

        assertEquals(key, e.value().<Integer>value(coCtx, true));
    }

    /** */
    protected void checkGroupEntry(DumpEntry e, CacheObjectContext coCtx0, CacheObjectContext coCtx1) {
        assertNotNull(e);

        if (e.cacheId() == CU.cacheId(CACHE_0))
            assertEquals(USER_FACTORY.apply(e.key().value(coCtx0, true)), e.value().value(coCtx0, true));
        else {
            assertNotNull(e.key().<Key>value(coCtx1, true));
            assertNotNull(e.value().<Value>value(coCtx1, true));
        }
    }

    /** */
    protected void insertOrUpdate(IgniteEx ignite, int i) {
        insertOrUpdate(ignite, i, i);
    }

    /** */
    protected void insertOrUpdate(IgniteEx ignite, int i, int val) {
        ignite.cache(DEFAULT_CACHE_NAME).put(i, val);
        IgniteCache<Object, Object> cache = ignite.cache(CACHE_0);
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_1);

        if (mode == CacheAtomicityMode.TRANSACTIONAL) {
            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(i, USER_FACTORY.apply(val));

                tx.commit();
            }

            try (Transaction tx = ignite.transactions().txStart()) {
                cache1.put(new Key(i), new Value(String.valueOf(val)));

                tx.commit();
            }
        }
        else {
            cache.put(i, USER_FACTORY.apply(val));

            cache1.put(new Key(i), new Value(String.valueOf(val)));
        }
    }

    /** */
    protected void remove(IgniteEx ignite, int i) {
        ignite.cache(DEFAULT_CACHE_NAME).remove(i);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_0);
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_1);

        IntConsumer moreRemovals = j -> {
            cache.remove(j);
            cache.remove(j);
            cache1.remove(new Key(j));
            cache1.remove(new Key(j));
        };

        if (mode == CacheAtomicityMode.TRANSACTIONAL) {
            try (Transaction tx = ignite.transactions().txStart()) {
                moreRemovals.accept(i);

                tx.commit();
            }
        }
        else
            moreRemovals.accept(i);
    }

    /** */
    void createDump(IgniteEx ign) {
        createDump(ign, DMP_NAME);
    }

    /** */
    public static Dump dump(IgniteEx ign, String name) throws IgniteCheckedException {
        return new DumpImpl(
            ign.context(),
            new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), ign.configuration().getSnapshotPath(), false), name)
        );
    }

    /** */
    public static void checkDumpWithCommand(IgniteEx ign, String name) throws Exception {
        CacheGroupContext gctx = ign.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        for (GridCacheContext<?, ?> cctx : gctx.caches())
            assertNull(cctx.dumpListener());

        gctx = ign.context().cache().cacheGroup(CU.cacheId(GRP));

        for (GridCacheContext<?, ?> cctx : gctx.caches())
            assertNull(cctx.dumpListener());

        String msg = invokeCheckCommand(ign, name);
        String expMsg = "The check procedure has finished, no conflicts have been found.\n\n";

        if (!Objects.equals(msg, expMsg)) {
            Dump dump = new DumpImpl(
                ign.context(),
                new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), ign.configuration().getSnapshotPath(), false), name)
            );

            CacheObjectContext coCtx = ign.context().cache().context().cacheObjectContext(CU.cacheId(DEFAULT_CACHE_NAME));
            CacheObjectContext coCtx0 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_0));
            CacheObjectContext coCtx1 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_1));

            Map<String, List<Set<Object>>> keys = new HashMap<>();

            for (String nodeDir : dump.nodesDirectories()) {
                for (int part : dump.partitions(nodeDir, CU.cacheId(DEFAULT_CACHE_NAME))) {
                    try (DumpedPartitionIterator iter = dump.iterator(nodeDir, CU.cacheId(DEFAULT_CACHE_NAME), part)) {

                        Set<Object> partKeys = new HashSet<>();

                        while (iter.hasNext())
                            partKeys.add(iter.next().key().value(coCtx, false));

                        keys.computeIfAbsent(DEFAULT_CACHE_NAME + "-" + part, key -> new ArrayList<>())
                            .add(partKeys);
                    }
                }

                for (int part : dump.partitions(nodeDir, CU.cacheId(GRP))) {
                    try (DumpedPartitionIterator iter = dump.iterator(nodeDir, CU.cacheId(GRP), part)) {
                        Set<Object> partKeys = new HashSet<>();

                        while (iter.hasNext()) {
                            DumpEntry e = iter.next();
                            partKeys.add(e.key().value(e.cacheId() == CU.cacheId(CACHE_0) ? coCtx0 : coCtx1, false));
                        }

                        keys.computeIfAbsent(GRP + "-" + part, key -> new ArrayList<>())
                            .add(partKeys);
                    }
                }
            }

            for (Map.Entry<String, List<Set<Object>>> partCopies : keys.entrySet()) {
                String part = partCopies.getKey();

                assertEquals(2, partCopies.getValue().size());

                Set<Object> a = partCopies.getValue().get(0);
                Set<Object> b = partCopies.getValue().get(1);

                if (!Objects.equals(a, b)) {
                    log.error(part + " DIFF (b - a):");
                    for (Object b0 : b) {
                        if (!a.contains(b0))
                            log.error(b0.toString());
                    }

                    log.error(part + " DIFF (a - b):");
                    for (Object a0 : a) {
                        if (!b.contains(a0))
                            log.error(a0.toString());
                    }
                }
            }
        }

        assertEquals("The check procedure has finished, no conflicts have been found.\n\n", invokeCheckCommand(ign, name));
    }

    /** */
    public static String invokeCheckCommand(IgniteEx ign, String name) {
        Object[] args = {name};

        String[] signature = new String[args.length];

        Arrays.fill(signature, String.class.getName());

        try {
            return (String)mngmntBean(ign, "Dump", "Check").invoke(INVOKE, args, signature);
        }
        catch (MBeanException | ReflectionException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    void createDump(IgniteEx ign, String name) {
        Object[] args = {name};

        String[] signature = new String[args.length];

        Arrays.fill(signature, String.class.getName());

        try {
            String res = (String)mngmntBean(ign, "Dump", "Create").invoke(INVOKE, args, signature);

            assertEquals("Dump \"" + name + "\" was created.\n", res);
        }
        catch (MBeanException | ReflectionException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    static DynamicMBean mngmntBean(IgniteEx ign, String... path) {
        DynamicMBean mbean = getMxBean(
            ign.context().igniteInstanceName(),
            "management",
            Arrays.asList(path).subList(0, path.length - 1),
            path[path.length - 1],
            DynamicMBean.class
        );

        assertNotNull(mbean);

        return mbean;
    }
}
