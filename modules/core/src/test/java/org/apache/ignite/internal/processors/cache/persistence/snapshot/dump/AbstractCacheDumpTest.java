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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_THREAD_CNT;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_TIMEOUT;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_RUNNING_DIR_KEY;
import static org.apache.ignite.internal.util.IgniteUtils.toLong;
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
    public static final IntFunction<User> USER_FACTORY = i ->
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
    @Parameterized.Parameter(4)
    public boolean useDataStreamer;

    /** */
    @Parameterized.Parameter(5)
    public boolean onlyPrimary;

    /** */
    @Parameterized.Parameter(6)
    public boolean encrypted;

    /** */
    @Parameterized.Parameters(name = "nodes={0},backups={1},persistence={2},mode={3},useDataStreamer={4},onlyPrimary={5},encrypted={6}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (int nodes : new int[]{/*1, */3})
            for (int backups : new int[]{0, 1})
                for (boolean persistence : new boolean[]{true, false})
                    for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
                        for (boolean useDataStreamer : new boolean[]{true, false}) {
                            if (nodes == 1 && backups != 0)
                                continue;

                            if (backups > 0) {
                                params.add(new Object[]{nodes, backups, persistence, mode, useDataStreamer, false, false});
                                params.add(new Object[]{nodes, backups, persistence, mode, useDataStreamer, true, false});
                            }
                            else
                                params.add(new Object[]{nodes, backups, persistence, mode, useDataStreamer, false, false});
                        }
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
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setSnapshotThreadPoolSize(snpPoolSz)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistence)
                    .setStoragePath(nodes > 1 ? "default_storage" : null))
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setName("custom_storage")
                    .setStoragePath("custom_storage")))
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName(DEFAULT_CACHE_NAME)
                    .setBackups(backups)
                    .setDataRegionName(backups > 0 ? "custom_storage" : null)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20)),
                new CacheConfiguration<>()
                    .setGroupName(GRP)
                    .setName(CACHE_0)
                    .setBackups(backups)
                    .setDataRegionName(backups > 0 ? "custom_storage" : null)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20)),
                new CacheConfiguration<>()
                    .setGroupName(GRP)
                    .setName(CACHE_1)
                    .setBackups(backups)
                    .setDataRegionName(backups > 0 ? "custom_storage" : null)
                    .setAtomicityMode(mode)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(20))
            );

        if (encrypted)
            cfg.setEncryptionSpi(encryptionSpi());

        return cfg;
    }

    /** */
    protected IgniteEx startGridAndFillCaches() throws Exception {
        IgniteEx ign = (IgniteEx)startGridsMultiThreaded(nodes);

        cli = startClientGrid(nodes);

        ign.cluster().state(ClusterState.ACTIVE);

        putData(cli.cache(DEFAULT_CACHE_NAME), cli.cache(CACHE_0), cli.cache(CACHE_1));

        return ign;
    }

    /** */
    protected T2<CountDownLatch, IgniteInternalFuture<?>> runDumpAsyncAndStopBeforeStart(
        IgniteEx srv
    ) throws IgniteInterruptedCheckedException {
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

        IgniteInternalFuture<Object> dumpFut = runAsync(() -> createDump(srv));

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
        if (useDataStreamer) {
            try (
                IgniteDataStreamer<Integer, Integer> _cache = cli.dataStreamer(cache.getName());
                IgniteDataStreamer<Integer, User> _grpCache0 = cli.dataStreamer(grpCache0.getName());
                IgniteDataStreamer<Key, Value> _grpCache1 = cli.dataStreamer(grpCache1.getName())
            ) {
                IntStream.range(0, KEYS_CNT).forEach(i -> {
                    _cache.addData(i, i);
                    _grpCache0.addData(i, USER_FACTORY.apply(i));
                    _grpCache1.addData(new Key(i), new Value(String.valueOf(i)));
                });
            }
        }
        else {
            IntStream.range(0, KEYS_CNT).forEach(i -> {
                cache.put(i, i);
                grpCache0.put(i, USER_FACTORY.apply(i));
                grpCache1.put(new Key(i), new Value(String.valueOf(i)));
            });
        }
    }

    /** */
    protected void checkDump(IgniteEx ign) throws Exception {
        checkDump(ign, DMP_NAME, false);
    }

    /** */
    void checkDump(IgniteEx ign, String name, boolean expectedComprParts) throws Exception {
        checkDump(ign,
            name,
            null,
            new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME, CACHE_0, CACHE_1)),
            KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
            2 * (KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups)),
            KEYS_CNT,
            false,
            expectedComprParts
        );
    }

    /** */
    void checkDump(
        IgniteEx ign,
        String name,
        String[] cacheGrpNames,
        Set<String> expectedFoundCaches,
        int expectedDfltDumpSz,
        int expectedGrpDumpSz,
        int expectedCnt,
        boolean skipCopies,
        boolean expectedComprParts
    ) throws Exception {
        checkDumpWithCommand(ign, name, backups);

        if (persistence && !ign.context().clientNode())
            assertNull(ign.context().cache().context().database().metaStorage().read(SNP_RUNNING_DIR_KEY));

        Dump dump = dump(ign, name);

        List<SnapshotMetadata> metadata = dump.metadata();

        assertNotNull(metadata);
        assertEquals(nodes, metadata.size());

        for (SnapshotMetadata meta : metadata) {
            assertEquals(name, meta.snapshotName());
            assertTrue(meta.dump());
            assertFalse(meta.cacheGroupIds().contains(CU.cacheId(UTILITY_CACHE_NAME)));
            assertEquals(expectedComprParts, meta.compressPartitions());
        }

        List<SnapshotFileTree> sfts = dump.fileTrees();

        assertEquals(nodes, sfts.size());

        TestDumpConsumer cnsmr = dumpConsumer(expectedFoundCaches, expectedDfltDumpSz, expectedGrpDumpSz, expectedCnt);

        SnapshotFileTree sft = snapshotFileTree(ign, name);

        new DumpReader(
            new DumpReaderConfiguration(
                sft.name(),
                sft.path(),
                ign.configuration(),
                cnsmr,
                DFLT_THREAD_CNT, DFLT_TIMEOUT,
                true,
                false,
                false,
                cacheGrpNames,
                skipCopies,
                null
            ),
            log
        ).run();

        cnsmr.check();
    }

    /** */
    protected TestDumpConsumer dumpConsumer(
        Set<String> expectedFoundCaches,
        int expectedDfltDumpSz,
        int expectedGrpDumpSz,
        int expectedCnt
    ) {
        return new TestDumpConsumerImpl(expectedFoundCaches, expectedDfltDumpSz, expectedGrpDumpSz, expectedCnt);
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
    protected void createDump(IgniteEx ign) {
        createDump(ign, DMP_NAME, null);
    }

    /** */
    public static Dump dump(IgniteEx ign, String name) throws IgniteCheckedException {
        List<SnapshotFileTree> sfts = G.allGrids().stream()
            .filter(n -> !n.configuration().isClientMode())
            .map(n -> snapshotFileTree(((IgniteEx)n), DMP_NAME))
            .collect(Collectors.toList());

        List<SnapshotMetadata> metadata = DumpReader.metadata(F.first(sfts).root());

        return new Dump(ign.context(), sfts, metadata, true, false, null, log);
    }

    /** */
    public static void checkDumpWithCommand(IgniteEx ign, String name, int backups) throws Exception {
        CacheGroupContext gctx = ign.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        for (GridCacheContext<?, ?> cctx : gctx.caches())
            assertNull(cctx.dumpListener());

        gctx = ign.context().cache().cacheGroup(CU.cacheId(GRP));

        for (GridCacheContext<?, ?> cctx : gctx.caches())
            assertNull(cctx.dumpListener());

        assertEquals("The check procedure has finished, no conflicts have been found.\n\n", invokeCheckCommand(ign, name));
    }

    /** */
    public static String invokeCheckCommand(IgniteEx ign, String name) throws IgniteCheckedException {
        return invokeCheckCommand(ign, name, null);
    }

    /** */
    public static String invokeCheckCommand(IgniteEx ign, String name, String snpPath) throws IgniteCheckedException {
        StringBuffer buf = new StringBuffer();

        ign.context().cache().context().snapshotMgr().checkSnapshot(name, snpPath).get(60_000)
            .print(line -> buf.append(line).append(System.lineSeparator()));

        return buf.toString();
    }

    /** */
    void createDump(IgniteEx ign, String name, @Nullable Collection<String> cacheGrpNames) {
        createDump(ign, name, cacheGrpNames, false);
    }

    /** */
    void createDump(IgniteEx ign, String name, @Nullable Collection<String> cacheGrpNames, boolean comprParts) {
        ign.context().cache().context().snapshotMgr()
            .createSnapshot(name, null, cacheGrpNames, false, onlyPrimary, true, comprParts, encrypted).get();
    }

    /** */
    public static KeystoreEncryptionSpi encryptionSpi() {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        return encSpi;
    }

    /** */
    public class TestDumpConsumerImpl extends TestDumpConsumer {
        /** */
        private final Set<String> expectedFoundCaches;

        /** */
        private final int expectedDfltDumpSz;

        /** */
        private final int expectedGrpDumpSz;

        /** */
        private final int expectedCnt;

        /** */
        final Set<Integer> keys = new HashSet<>();

        /** */
        final Set<Long> grpParts = new HashSet<>();

        /** */
        int dfltDumpSz;

        /** */
        int grpDumpSz;

        /** */
        protected TestDumpConsumerImpl(Set<String> expectedFoundCaches, int expectedDfltDumpSz, int expectedGrpDumpSz, int expectedCnt) {
            this.expectedFoundCaches = expectedFoundCaches;
            this.expectedDfltDumpSz = expectedDfltDumpSz;
            this.expectedGrpDumpSz = expectedGrpDumpSz;
            this.expectedCnt = expectedCnt;
        }

        /** {@inheritDoc} */
        @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
            super.onCacheConfigs(caches);

            Set<String> cachesFound = new HashSet<>();

            caches.forEachRemaining(data -> {
                String cacheName = data.config().getName();

                assertTrue(cachesFound.add(cacheName));

                assertEquals(cacheName, data.configuration().getName());

                assertFalse(data.sql());

                assertTrue(data.queryEntities().isEmpty());

                if (cacheName.startsWith("cache-"))
                    assertEquals(GRP, data.configuration().getGroupName());
                else if (!cacheName.equals(DEFAULT_CACHE_NAME))
                    throw new IgniteException("Unknown cache");
            });

            assertEquals(expectedFoundCaches, cachesFound);
        }

        /** {@inheritDoc} */
        @Override public void onPartition(int grp, int part, Iterator<DumpEntry> iter) {
            if (onlyPrimary)
                assertTrue(grpParts.add(toLong(grp, part)));

            if (grp == CU.cacheId(DEFAULT_CACHE_NAME)) {
                while (iter.hasNext()) {
                    DumpEntry e = iter.next();

                    checkDefaultCacheEntry(e);

                    keys.add((Integer)e.key());

                    dfltDumpSz++;
                }
            }
            else {
                while (iter.hasNext()) {
                    DumpEntry e = iter.next();

                    assertNotNull(e);
                    assertNotNull(e.version());
                    assertNull(e.version().otherClusterVersion());

                    if (e.cacheId() == CU.cacheId(CACHE_0))
                        assertEquals(USER_FACTORY.apply((Integer)e.key()), e.value());
                    else
                        assertEquals(((Key)e.key()).getId() + "", ((Value)e.value()).getVal());

                    grpDumpSz++;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void check() {
            super.check();

            assertEquals(expectedDfltDumpSz, dfltDumpSz);
            assertEquals(expectedGrpDumpSz, grpDumpSz);

            IntStream.range(0, expectedCnt).forEach(key -> assertTrue(keys.contains(key)));
        }

        /** */
        protected void checkDefaultCacheEntry(DumpEntry e) {
            assertNotNull(e);

            Integer key = (Integer)e.key();

            assertEquals(key, e.value());
            assertNotNull(e.version());
            assertNull(e.version().otherClusterVersion());
        }
    }

    /** */
    public abstract static class TestDumpConsumer implements DumpConsumer {
        /** */
        private boolean started;

        /** */
        private boolean stopped;

        /** */
        private boolean typesCb;

        /** */
        private boolean mappingCb;

        /** */
        private boolean cacheCfgCb;

        /** {@inheritDoc} */
        @Override public void start() {
            assertFalse(started);
            assertFalse(mappingCb);
            assertFalse(typesCb);
            assertFalse(cacheCfgCb);
            assertFalse(stopped);

            started = true;
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            assertTrue(started);
            assertFalse(mappingCb);
            assertFalse(typesCb);
            assertFalse(cacheCfgCb);
            assertFalse(stopped);

            mappingCb = true;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            assertTrue(started);
            assertTrue(mappingCb);
            assertFalse(typesCb);
            assertFalse(cacheCfgCb);
            assertFalse(stopped);

            typesCb = true;
        }

        /** {@inheritDoc} */
        @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
            assertTrue(started);
            assertTrue(mappingCb);
            assertTrue(typesCb);
            assertFalse(cacheCfgCb);
            assertFalse(stopped);

            cacheCfgCb = true;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            assertTrue(started);
            assertTrue(typesCb);
            assertTrue(mappingCb);
            assertTrue(cacheCfgCb);
            assertFalse(stopped);

            stopped = true;
        }

        /** */
        public void check() {
            assertTrue(started);
            assertTrue(typesCb);
            assertTrue(mappingCb);
            assertTrue(cacheCfgCb);
            assertTrue(stopped);
        }
    }
}
