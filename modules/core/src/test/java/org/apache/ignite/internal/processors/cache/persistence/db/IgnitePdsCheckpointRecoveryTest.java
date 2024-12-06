/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CP_RECOVERY_DATA_COMRESSION;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointRecoveryFileStorage.FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;

/**
 * Class containing tests for applying checkpoint recovery data.
 */
@RunWith(Parameterized.class)
public class IgnitePdsCheckpointRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_CNT = 10_000;

    /** */
    private static final int PARTS = 10;

    /** */
    private final AtomicBoolean fail = new AtomicBoolean();

    /** */
    private final AtomicInteger spoiledPageLimit = new AtomicInteger();

    /** */
    @Parameterized.Parameter(0)
    public boolean encrypt;

    /** */
    @Parameterized.Parameters(name = "encrypt={0}")
    public static Collection<Object[]> parameters() {
        return F.asList(new Object[] {false}, new Object[] {true});
    }

    /** */
    protected DiskPageCompression getCompression() {
        return DFLT_CP_RECOVERY_DATA_COMRESSION;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setEncryptionSpi(encSpi)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setFileIOFactory(new PageStoreSpoilingFileIOFactory(fail, spoiledPageLimit))
                .setWriteRecoveryDataOnCheckpoint(true)
                .setCheckpointRecoveryDataCompression(getCompression())
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRecoverFromCheckpointRecoveryFiles() throws Exception {
        IgniteEx ignite = startGrid(0);
        ignite.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Integer, Integer> cacheCfg = GridAbstractTest.<Integer, Integer>defaultCacheConfiguration()
            .setAffinity(new RendezvousAffinityFunction(false, PARTS))
            .setEncryptionEnabled(encrypt);

        if (encrypt)
            cacheCfg.setDiskPageCompression(DiskPageCompression.DISABLED);

        IgniteCache<Integer, Integer> cache = ignite.createCache(cacheCfg);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        AtomicInteger val = new AtomicInteger(KEYS_CNT);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (true)
                cache.put(ThreadLocalRandom.current().nextInt(KEYS_CNT), val.incrementAndGet());
        });

        File cpDir = dbMgr(ignite).checkpointDirectory();

        spoiledPageLimit.set(10);
        fail.set(true);

        try {
            forceCheckpoint();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        try {
            fut.get(10_000);
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertTrue(GridTestUtils.waitForCondition(
            () -> Ignition.state(getTestIgniteInstanceName(0)) == IgniteState.STOPPED_ON_FAILURE,
            10_000
        ));

        fail.set(false);

        assertTrue(cpDir.listFiles(((dir, name) -> FILE_NAME_PATTERN.matcher(name).matches())).length > 0);

        ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache0 = ignite.cache(DEFAULT_CACHE_NAME);

        int max = 0;
        for (int i = 0; i < KEYS_CNT; i++)
            max = Math.max(max, cache0.get(i));

        // There are two cases possible:
        // 1. Failure during put before writting cache entry ta WAL, in this case, after restore we will get last value
        //    in cache: val.get() - 1
        // 2. Failure during put after writting cache entry ta WAL, in this case, after restore we will get last value
        //    in cache: val.get()
        assertTrue("Expected value between " + (val.get() - 1) + " and " + val.get() + ", actual value: " + max,
            max >= val.get() - 1 && max <= val.get());
    }

    /** */
    private static final class PageStoreSpoilingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private final AtomicBoolean failFlag;

        /** */
        private final AtomicInteger spoiledPageLimit;

        /** */
        PageStoreSpoilingFileIOFactory(AtomicBoolean failFlag, AtomicInteger spoiledPageLimit) {
            delegateFactory = new RandomAccessFileIOFactory();

            this.failFlag = failFlag;
            this.spoiledPageLimit = spoiledPageLimit;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return file.getName().startsWith(PART_FILE_PREFIX) ? new PageStoreSpoiling(delegate, spoiledPageLimit) : delegate;
        }

        /** */
        final class PageStoreSpoiling extends FileIODecorator {
            /** */
            private final AtomicInteger spoiledPages = new AtomicInteger();

            /** */
            private final AtomicInteger spoiledPagesLimit;

            /**
             * @param delegate File I/O delegate
             */
            public PageStoreSpoiling(FileIO delegate, AtomicInteger spoiledPagesLimit) {
                super(delegate);
                this.spoiledPagesLimit = spoiledPagesLimit;
            }

            /** {@inheritDoc} */
            @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                if (failFlag.get()) {
                    // Spoil specified pages amount and after that throw an exception.
                    if (spoiledPages.getAndIncrement() > spoiledPagesLimit.get())
                        throw new IOException("Test exception.");
                    else {
                        srcBuf = ByteBuffer.allocate(srcBuf.remaining()).order(ByteOrder.nativeOrder());
                        ThreadLocalRandom.current().nextBytes(srcBuf.array());
                    }
                }

                return delegate.writeFully(srcBuf, position);
            }
        }
    }
}
