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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Test;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_RUNNING_DIR_KEY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** */
public class IgniteCacheDumpSelfTest extends AbstractCacheDumpTest {
    /** */
    public static final String EXISTS_ERR_MSG = "Create snapshot request has been rejected. " +
        "Snapshot with given name already exists on local node";

    /** */
    public static final long TTL = 5 * 1000;

    /** */
    public static final ExpiryPolicy EXPIRY_POLICY = new ExpiryPolicy() {
        @Override public Duration getExpiryForCreation() {
            return new Duration(MILLISECONDS, TTL);
        }

        @Override public Duration getExpiryForAccess() {
            return null;
        }

        @Override public Duration getExpiryForUpdate() {
            return null;
        }
    };

    /** */
    private Boolean explicitTtl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        explicitTtl = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (explicitTtl == TRUE) {
            for (CacheConfiguration<?, ?> ccfg : cfg.getCacheConfiguration())
                ccfg.setExpiryPolicyFactory(() -> EXPIRY_POLICY);
        }

        return cfg;
    }

    /** */
    @Test
    public void testDumpWithNodeFilterCache() throws Exception {
        assumeTrue(nodes > 1);

        CacheConfiguration<?, ?> ccfg0 = cacheConfiguration(getConfiguration(getTestIgniteInstanceName()), DEFAULT_CACHE_NAME)
            .setNodeFilter(new AttributeNodeFilter(DEFAULT_CACHE_NAME, null));

        CacheConfiguration<?, ?> ccfg1 = cacheConfiguration(getConfiguration(getTestIgniteInstanceName()), CACHE_0)
            .setNodeFilter(ccfg0.getNodeFilter());

        for (int i = 0; i <= nodes; ++i) {
            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(i));

            if (i == 0)
                cfg.setUserAttributes(F.asMap(DEFAULT_CACHE_NAME, ""));

            cfg.setCacheConfiguration(null);

            cfg.setClientMode(i == nodes);

            IgniteEx ig = startGrid(cfg);

            cli = i == nodes ? ig : null;
        }

        cli.cluster().state(ClusterState.ACTIVE);

        cli.createCache(ccfg0);
        cli.createCache(ccfg1);

        try (IgniteDataStreamer<Integer, Integer> ds0 = cli.dataStreamer(DEFAULT_CACHE_NAME);
             IgniteDataStreamer<Integer, User> ds1 = cli.dataStreamer(CACHE_0)) {
            IgniteCache<Integer, Integer> cache0 = cli.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, User> cache1 = cli.cache(CACHE_0);

            for (int i = 0; i < KEYS_CNT; ++i) {
                if (useDataStreamer) {
                    ds0.addData(i, i);
                    ds1.addData(i, USER_FACTORY.apply(i));
                }
                else {
                    cache0.put(i, i);
                    cache1.put(i, USER_FACTORY.apply(i));
                }
            }
        }

        createDump(cli, DMP_NAME, null);

        checkDump(cli,
            DMP_NAME,
            new String[] {DEFAULT_CACHE_NAME, GRP},
            Stream.of(DEFAULT_CACHE_NAME, CACHE_0).collect(Collectors.toSet()),
            KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
            KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
            0,
            false,
            false
        );
    }

    /** */
    @Test
    public void testCacheDump() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign);

            checkDump(ign);

            assertThrows(null, () -> createDump(ign), IgniteException.class, EXISTS_ERR_MSG);

            createDump(ign, DMP_NAME + 2, null);

            checkDump(ign, DMP_NAME + 2, false);

            if (persistence) {
                assertThrows(null, () -> ign.snapshot().createSnapshot(DMP_NAME).get(), IgniteException.class, EXISTS_ERR_MSG);

                ign.snapshot().createSnapshot(DMP_NAME + 3).get();
            }
            else {
                assertThrows(
                    null,
                    () -> ign.snapshot().createSnapshot(DMP_NAME + 3).get(),
                    IgniteException.class,
                    "Create snapshot request has been rejected. Snapshots on an in-memory clusters are not allowed."
                );
            }
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testZippedCacheDump() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign, DMP_NAME, null, true);

            checkDump(ign, DMP_NAME, true);

            createDump(cli, DMP_NAME + 2, null, true);

            checkDump(cli, DMP_NAME + 2, true);
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testCacheDumpWithReadGroupFilter() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign);

            checkDump(
                ign,
                DMP_NAME,
                new String[]{GRP},
                new HashSet<>(Arrays.asList(CACHE_0, CACHE_1)),
                0,
                2 * (KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups)),
                0,
                false,
                false
            );

            checkDump(
                ign,
                DMP_NAME,
                new String[]{DEFAULT_CACHE_NAME},
                new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME)),
                KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
                0,
                KEYS_CNT,
                false,
                false
            );

            checkDump(
                ign,
                DMP_NAME,
                new String[]{DEFAULT_CACHE_NAME, GRP},
                new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME, CACHE_0, CACHE_1)),
                KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
                2 * (KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups)),
                KEYS_CNT,
                false,
                false
            );
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testSkipCopies() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign);

            checkDump(
                ign,
                DMP_NAME,
                null,
                new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME, CACHE_0, CACHE_1)),
                KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups),
                2 * (KEYS_CNT + (onlyPrimary ? 0 : KEYS_CNT * backups)),
                KEYS_CNT,
                false,
                false
            );

            checkDump(
                ign,
                DMP_NAME,
                null,
                new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME, CACHE_0, CACHE_1)),
                KEYS_CNT,
                2 * KEYS_CNT,
                KEYS_CNT,
                true,
                false
            );
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testCacheDumpWithGroupFilter() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            String name = DMP_NAME;

            {
                createDump(ign, name, Collections.singleton(DEFAULT_CACHE_NAME));

                checkDumpWithCommand(ign, name, backups);

                Dump dump = dump(ign, name);

                List<Integer> grps = F.first(dump.metadata()).cacheGroupIds();

                assertEquals(1, grps.size());
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), (int)grps.get(0));
            }

            name = DMP_NAME + "2";

            {
                createDump(cli, name, Collections.singleton(GRP));

                checkDumpWithCommand(ign, name, backups);

                Dump dump = dump(ign, name);

                List<Integer> grps = F.first(dump.metadata()).cacheGroupIds();

                assertEquals(1, grps.size());
                assertEquals(CU.cacheId(GRP), (int)grps.get(0));
            }

            name = DMP_NAME + "3";

            {
                createDump(cli, name, new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME, GRP)));

                checkDumpWithCommand(ign, name, backups);

                Dump dump = dump(ign, name);

                List<Integer> grps = F.first(dump.metadata()).cacheGroupIds();

                assertEquals(2, grps.size());
                assertTrue(grps.contains(CU.cacheId(DEFAULT_CACHE_NAME)));
                assertTrue(grps.contains(CU.cacheId(GRP)));
            }
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testConcurrentDumpCreationThrows() throws Exception {
        doTestConcurrentOperations(ignite -> {
            assertThrows(
                null,
                () -> createDump(ignite, "other_dump", null),
                IgniteException.class,
                "Create snapshot request has been rejected. The previous snapshot operation was not completed."
            );

            if (persistence) {
                assertThrows(
                    null,
                    () -> ignite.snapshot().createSnapshot("other_snapshot").get(getTestTimeout()),
                    IgniteException.class,
                    "Create snapshot request has been rejected. The previous snapshot operation was not completed."
                );
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentInserts() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = KEYS_CNT * 10; i < KEYS_CNT * 10 + 3; i++) {
                assertFalse(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertFalse(ignite.cache(CACHE_0).containsKey(i));
                assertFalse(ignite.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(ignite, i);
            }

            for (int i = KEYS_CNT * 10 + 3; i < KEYS_CNT * 10 + 6; i++) {
                assertFalse(cli.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertFalse(cli.cache(CACHE_0).containsKey(i));
                assertFalse(cli.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(cli, i);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentUpdates() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertTrue(ignite.cache(CACHE_0).containsKey(i));
                assertTrue(ignite.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(ignite, i);
            }

            for (int i = 3; i < 6; i++) {
                assertTrue(cli.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertTrue(cli.cache(CACHE_0).containsKey(i));
                assertTrue(cli.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(cli, i);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentEntryProcessor() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = KEYS_CNT * 10; i < KEYS_CNT * 10 + 3; i++) {
                assertFalse(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));

                ignite.<Integer, Integer>cache(DEFAULT_CACHE_NAME).invoke(i, new CacheEntryProcessor<Integer, Integer, Void>() {
                    @Override public Void process(MutableEntry<Integer, Integer> entry, Object... arguments) {
                        entry.setValue(entry.getKey());
                        return null;
                    }
                });
            }

            for (int i = 0; i < 3; i++) {
                assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));

                ignite.<Integer, Integer>cache(DEFAULT_CACHE_NAME).invoke(i, new CacheEntryProcessor<Integer, Integer, Void>() {
                    @Override public Void process(MutableEntry<Integer, Integer> entry, Object... arguments) {
                        entry.setValue(entry.getKey() + 1);
                        return null;
                    }
                });
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentRemovals() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));

                remove(ignite, i);
            }

            for (int i = 3; i < 6; i++) {
                assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));

                remove(cli, i);
            }
        });
    }

    /** */
    @Test
    public void testDumpWithExplicitExpireTime() throws Exception {
        explicitTtl = true;

        doTestDumpWithExpiry();
    }

    /** */
    @Test
    public void testDumpWithImplicitExpireTime() throws Exception {
        assumeFalse(useDataStreamer);

        explicitTtl = false;

        doTestDumpWithExpiry();
    }

    /** */
    @Test
    public void testDumpCancelOnFileCreateError() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        for (Ignite node : G.allGrids()) {
            if (node.configuration().isClientMode() == TRUE)
                continue;

            ((IgniteEx)node).context().cache().context().snapshotMgr().ioFactory(new DumpFailingFactory((IgniteEx)node, false));
        }

        assertThrows(null, () -> ign.snapshot().createDump(DMP_NAME, null).get(), IgniteException.class, "Test error");

        checkDumpCleared(ign);

        ign.cache(DEFAULT_CACHE_NAME).put(KEYS_CNT, KEYS_CNT);

        IntStream.range(0, KEYS_CNT).forEach(i -> {
            assertEquals(i, ign.cache(DEFAULT_CACHE_NAME).get(i));
            assertEquals(USER_FACTORY.apply(i), ign.cache(CACHE_0).get(i));
            assertEquals(new Value(String.valueOf(i)), ign.cache(CACHE_1).get(new Key(i)));
        });
    }

    /** */
    @Test
    public void testDumpCancelOnIteratorWriteError() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        DumpFailingFactory ioFactory = new DumpFailingFactory(ign, true);

        ign.context().cache().context().snapshotMgr().ioFactory(ioFactory);

        assertThrows(null, () -> ign.snapshot().createDump(DMP_NAME, null).get(), IgniteException.class, "Test write error");

        assertTrue(ioFactory.errorAfter.get() <= 0);

        checkDumpCleared(ign);

        ign.cache(DEFAULT_CACHE_NAME).put(KEYS_CNT, KEYS_CNT);
    }

    /** */
    @Test
    public void testDumpCancelOnListenerWriteError() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        IgniteCache<Object, Object> cache = ign.cache(DEFAULT_CACHE_NAME);

        int keyToFail = findKeys(ign.localNode(), cache, 1, KEYS_CNT, 0).get(0);

        byte[] valToFail = new byte[100];

        AtomicBoolean keyToFailFound = new AtomicBoolean();

        IntStream.range(0, 100).forEach(i -> valToFail[i] = (byte)i);

        cache.put(keyToFail, valToFail);

        FileIOFactory delegate = ign.context().cache().context().snapshotMgr().ioFactory();

        ign.context().cache().context().snapshotMgr().ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                if (file.getName().endsWith(DUMP_FILE_EXT)) {
                    return new FileIODecorator(delegate.create(file, modes)) {
                        /** {@inheritDoc} */
                        @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                            if (findValToFail(srcBuf)) {
                                keyToFailFound.set(true);

                                throw new IOException("Val to fail found");
                            }

                            return super.writeFully(srcBuf, position);
                        }

                        private boolean findValToFail(ByteBuffer srcBuf) {
                            if (srcBuf.limit() >= valToFail.length) {
                                int valIdx = 0;

                                for (int i = 0; i < srcBuf.limit(); i++) {
                                    if (srcBuf.get(i) == valToFail[valIdx]) {
                                        valIdx++;

                                        if (valIdx == valToFail.length)
                                            return true;
                                    }
                                    else
                                        valIdx = 0;
                                }
                            }

                            return false;
                        }
                    };
                }

                return delegate.create(file, modes);
            }
        });

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart(ign);

        cache.put(keyToFail, "test string");

        latchAndFut.get1().countDown();

        assertThrows(null, () -> latchAndFut.get2().get(10 * 1000), IgniteCheckedException.class, "Val to fail found");

        assertTrue(keyToFailFound.get());

        checkDumpCleared(ign);

        cache.put(keyToFail, valToFail);
    }

    /** */
    private void checkDumpCleared(IgniteEx ign) throws IgniteCheckedException {
        if (persistence)
            assertNull(ign.context().cache().context().database().metaStorage().read(SNP_RUNNING_DIR_KEY));

        assertFalse(new File(sharedFileTree(ign.configuration()).snapshotsRoot(), DMP_NAME).exists());
    }

    /** */
    private void doTestDumpWithExpiry() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart(ign);

        Thread.sleep(TTL);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < KEYS_CNT; i++) {
                if (ign.cache(DEFAULT_CACHE_NAME).containsKey(i))
                    return false;

                if (ign.cache(CACHE_0).containsKey(i))
                    return false;

                if (ign.cache(CACHE_1).containsKey(new Key(i)))
                    return false;
            }

            return true;
        }, 2 * TTL));

        latchAndFut.get1().countDown();

        latchAndFut.get2().get();

        checkDump(ign);
    }

    /** */
    private void doTestConcurrentOperations(Consumer<IgniteEx> op) throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart(ign);

        // This operations will be catched by change listeners. Old value must be stored in dump.
        op.accept(ign);

        latchAndFut.get1().countDown();

        latchAndFut.get2().get(10 * 1000);

        checkDump(ign);
    }

    /** {@inheritDoc} */
    @Override protected void putData(
        IgniteCache<Object, Object> cache,
        IgniteCache<Object, Object> grpCache0,
        IgniteCache<Object, Object> grpCache1
    ) {
        if (explicitTtl == FALSE) {
            super.putData(
                cache.withExpiryPolicy(EXPIRY_POLICY),
                grpCache0.withExpiryPolicy(EXPIRY_POLICY),
                grpCache1.withExpiryPolicy(EXPIRY_POLICY)
            );
        }
        else
            super.putData(cache, grpCache0, grpCache1);
    }

    /** {@inheritDoc} */
    @Override protected TestDumpConsumer dumpConsumer(
        Set<String> expectedFoundCaches,
        int expectedDfltDumpSz,
        int expectedGrpDumpSz,
        int expectedCnt
    ) {
        return new TestDumpConsumerImpl(expectedFoundCaches, expectedDfltDumpSz, expectedGrpDumpSz, expectedCnt) {
            /** {@inheritDoc} */
            @Override protected void checkDefaultCacheEntry(DumpEntry e) {
                super.checkDefaultCacheEntry(e);

                if (explicitTtl != null) {
                    assertTrue("Expire time must be set", e.expireTime() != 0);
                    assertTrue("Expire time must be in past", System.currentTimeMillis() >= e.expireTime());
                    assertTrue("Expire time must be set during test run", System.currentTimeMillis() - getTestTimeout() < e.expireTime());
                }
            }
        };
    }

    /** */
    public static class DumpFailingFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegate;

        /** */
        private final AtomicInteger errorAfter;

        /** */
        private final boolean failOnWrite;

        /** */
        public DumpFailingFactory(IgniteEx ign, boolean failOnWrite) {
            this.delegate = ign.context().cache().context().snapshotMgr().ioFactory();
            this.failOnWrite = failOnWrite;
            this.errorAfter = new AtomicInteger(1);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (failOnWrite) {
                return new FileIODecorator(delegate.create(file, modes)) {
                    /** {@inheritDoc} */
                    @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                        if (errorAfter.decrementAndGet() > 0)
                            return super.writeFully(srcBuf, position);

                        throw new IOException("Test write error");
                    }
                };
            }
            else if (file.getName().endsWith(DUMP_FILE_EXT)) {
                throw new IOException("Test error");
            }

            return delegate.create(file, modes);
        }
    }
}
