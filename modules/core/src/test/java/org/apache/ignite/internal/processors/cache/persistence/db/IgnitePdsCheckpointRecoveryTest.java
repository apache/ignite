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
import java.util.regex.Pattern;
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
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointRecoveryFileStorage;
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
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;

/**
 * Class containing tests for applying checkpoint recovery data.
 */
@RunWith(Parameterized.class)
public class IgnitePdsCheckpointRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_CNT = 20_000;

    /** */
    private static final int PARTS = 10;

    /** */
    private final AtomicBoolean fail = new AtomicBoolean();

    /** */
    private final AtomicInteger spoiledPageLimit = new AtomicInteger();

    /** */
    private Pattern spoilFilePattern;

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
                .setFileIOFactory(new PageStoreSpoilingFileIOFactory(fail, spoiledPageLimit, spoilFilePattern))
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
        spoilFilePattern = Pattern.compile('^' + Pattern.quote(PART_FILE_PREFIX) + ".*");

        IgniteEx ignite = initIgnite();
        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        AtomicInteger val = new AtomicInteger(KEYS_CNT);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (true)
                cache.put(ThreadLocalRandom.current().nextInt(KEYS_CNT), val.incrementAndGet());
        });

        File cpDir = ignite.context().pdsFolderResolver().fileTree().checkpoint();

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
        // 1. Failure during put before writting cache entry to WAL, in this case, after restore we will get last value
        //    in cache: val.get() - 1
        // 2. Failure during put after writting cache entry to WAL, in this case, after restore we will get last value
        //    in cache: val.get()
        assertTrue("Expected value between " + (val.get() - 1) + " and " + val.get() + ", actual value: " + max,
            max >= val.get() - 1 && max <= val.get());
    }

    /** */
    @Test
    public void testFailToRecoverFromSpoiledCheckpointRecoveryFiles() throws Exception {
        spoilFilePattern = Pattern.compile('^' + Pattern.quote(PART_FILE_PREFIX) + ".*");

        IgniteEx ignite = initIgnite();

        File cpDir = ignite.context().pdsFolderResolver().fileTree().checkpoint();

        spoiledPageLimit.set(10);
        fail.set(true);

        try {
            forceCheckpoint();
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

        // Spoil recovery files.
        for (File file : cpDir.listFiles(((dir, name) -> FILE_NAME_PATTERN.matcher(name).matches()))) {
            if (file.length() == 0)
                continue;

            FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

            FileIO fileIO = fileIoFactory.create(file);

            for (int i = 0; i < 100; i++) {
                // Spoil random bytes.
                fileIO.position(ThreadLocalRandom.current().nextLong(file.length() - 1));
                fileIO.write(new byte[] {(byte)ThreadLocalRandom.current().nextInt(256)}, 0, 1);
            }
        }

        try {
            startGrid(0);

            fail();
        }
        catch (Exception ignore) {
            // Recovery files inconsistency should be detected by CRC or fields check (depending on bytes spoiled).
        }
    }

    /** */
    @Test
    public void testFailureOnCheckpointRecoveryFilesWrite() throws Exception {
        spoilFilePattern = CheckpointRecoveryFileStorage.FILE_NAME_PATTERN;

        IgniteEx ignite = initIgnite();

        spoiledPageLimit.set(10);
        fail.set(true);

        try {
            forceCheckpoint();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertTrue(GridTestUtils.waitForCondition(
            () -> Ignition.state(getTestIgniteInstanceName(0)) == IgniteState.STOPPED_ON_FAILURE,
            10_000
        ));

        fail.set(false);

        File cpDir = ignite.context().pdsFolderResolver().fileTree().checkpoint();

        assertTrue(cpDir.listFiles(((dir, name) -> FILE_NAME_PATTERN.matcher(name).matches())).length > 0);

        ignite = startGrid(0);

        IgniteCache<Integer, Integer> cache0 = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS_CNT; i++)
            assertEquals((Integer)i, cache0.get(i));
    }

    /** */
    private IgniteEx initIgnite() throws Exception {
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

        return ignite;
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
        private final Pattern filePattern;

        /** */
        PageStoreSpoilingFileIOFactory(AtomicBoolean failFlag, AtomicInteger spoiledPageLimit, Pattern filePattern) {
            delegateFactory = new RandomAccessFileIOFactory();

            this.failFlag = failFlag;
            this.spoiledPageLimit = spoiledPageLimit;
            this.filePattern = filePattern;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return filePattern.matcher(file.getName()).matches()
                ? new PageStoreSpoiling(delegate)
                : delegate;
        }

        /** */
        final class PageStoreSpoiling extends FileIODecorator {
            /** */
            private final AtomicInteger spoiledPages = new AtomicInteger();

            /**
             * @param delegate File I/O delegate
             */
            public PageStoreSpoiling(FileIO delegate) {
                super(delegate);
            }

            /** {@inheritDoc} */
            @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                spoilBufferIfNeeded(srcBuf);

                return delegate.writeFully(srcBuf);
            }

            /** {@inheritDoc} */
            @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                spoilBufferIfNeeded(srcBuf);

                return delegate.writeFully(srcBuf, position);
            }

            /** */
            private void spoilBufferIfNeeded(ByteBuffer srcBuf) throws IOException {
                if (failFlag.get()) {
                    // Spoil specified pages amount and after that throw an exception.
                    if (spoiledPages.getAndIncrement() > spoiledPageLimit.get())
                        throw new IOException("Test exception.");
                    else {
                        srcBuf = ByteBuffer.allocate(srcBuf.remaining()).order(ByteOrder.nativeOrder());
                        ThreadLocalRandom.current().nextBytes(srcBuf.array());
                    }
                }
            }
        }
    }
}
