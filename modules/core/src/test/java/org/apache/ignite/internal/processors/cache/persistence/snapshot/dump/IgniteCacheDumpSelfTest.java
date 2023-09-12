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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_RUNNING_DIR_KEY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

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
    public void testCacheDump() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign);

            checkDump(ign);

            assertThrows(null, () -> createDump(ign), IgniteException.class, EXISTS_ERR_MSG);

            createDump(ign, DMP_NAME + 2);

            checkDump(ign, DMP_NAME + 2);

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
    public void testWithConcurrentInserts() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = KEYS_CNT; i < KEYS_CNT + 3; i++) {
                assertFalse(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertFalse(ignite.cache(CACHE_0).containsKey(i));
                assertFalse(ignite.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(ignite, i);
            }

            for (int i = KEYS_CNT + 3; i < KEYS_CNT + 6; i++) {
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
        explicitTtl = false;

        doTestDumpWithExpiry();
    }

    /** */
    @Test
    public void testDumpCancelOnFileCreateError() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        ign.context().cache().context().snapshotMgr().ioFactory(new DumpFailingFactory(ign, false));

        assertThrows(null, () -> ign.snapshot().createDump(DMP_NAME).get(), IgniteException.class, "Test error");

        checkDumpCleared(ign);

        ign.cache(DEFAULT_CACHE_NAME).put(KEYS_CNT, KEYS_CNT);
    }

    /** */
    @Test
    public void testDumpCancelOnIteratorWriteError() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        DumpFailingFactory ioFactory = new DumpFailingFactory(ign, true);

        ign.context().cache().context().snapshotMgr().ioFactory(ioFactory);

        assertThrows(null, () -> ign.snapshot().createDump(DMP_NAME).get(), IgniteException.class, "Test write error");

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
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (findValToFail(srcBuf)) {
                                keyToFailFound.set(true);

                                throw new IOException("Val to fail found");
                            }

                            return super.writeFully(srcBuf);
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

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart();

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

        assertFalse(
            new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), ign.configuration().getSnapshotPath(), false), DMP_NAME).exists()
        );
    }

    /** */
    private void doTestDumpWithExpiry() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart();

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

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart();

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
    @Override protected void checkDefaultCacheEntry(DumpEntry e, CacheObjectContext coCtx) {
        super.checkDefaultCacheEntry(e, coCtx);

        if (explicitTtl != null) {
            assertTrue("Expire time must be set", e.expireTime() != 0);
            assertTrue("Expire time must be in past", System.currentTimeMillis() >= e.expireTime());
            assertTrue("Expire time must be set during test run", System.currentTimeMillis() - getTestTimeout() < e.expireTime());
        }
    }

    /** */
    public class DumpFailingFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegate;

        /** */
        private final AtomicInteger errorAfter = new AtomicInteger(KEYS_CNT / 2);

        /** */
        private final boolean failOnWrite;

        /** */
        public DumpFailingFactory(IgniteEx ign, boolean failOnWrite) {
            this.delegate = ign.context().cache().context().snapshotMgr().ioFactory();
            this.failOnWrite = failOnWrite;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (failOnWrite) {
                return new FileIODecorator(delegate.create(file, modes)) {
                    /** {@inheritDoc} */
                    @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                        if (errorAfter.decrementAndGet() > 0)
                            return super.writeFully(srcBuf);

                        throw new IOException("Test write error");
                    }
                };
            }
            else if (file.getName().endsWith(DUMP_FILE_EXT))
                throw new IOException("Test error");

            return delegate.create(file, modes);
        }
    }
}
