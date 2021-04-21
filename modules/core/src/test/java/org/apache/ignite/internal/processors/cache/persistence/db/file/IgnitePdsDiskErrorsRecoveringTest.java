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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;

/**
 * Tests node recovering after disk errors during interaction with persistent storage.
 */
@WithSystemProperty(key = "IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC", value = "false")
public class IgnitePdsDiskErrorsRecoveringTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = DataStorageConfiguration.DFLT_PAGE_SIZE;

    /** */
    private static final int WAL_SEGMENT_SIZE = 1024 * PAGE_SIZE;

    /** */
    private static final long STOP_TIMEOUT_MS = 30 * 1000;

    /** */
    private static final String CACHE_NAME = "cache";

    /** Specified i/o factory for particular test. */
    private FileIOFactory ioFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ioFactory = null;
        System.clearProperty(IGNITE_WAL_MMAP);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY)
                .setWalCompactionEnabled(false)
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setCheckpointFrequency(240 * 60 * 1000)
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        if (ioFactory != null)
            dsCfg.setFileIOFactory(ioFactory);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME)
            .setRebalanceMode(CacheRebalanceMode.NONE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Test node stopping & recovering on cache initialization fail.
     */
    @Test
    public void testRecoveringOnCacheInitFail() throws Exception {
        // Fail to initialize page store. 2 extra pages is needed for MetaStorage.
        ioFactory = new FilteringFileIOFactory(".bin", new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), 2 * PAGE_SIZE));

        boolean failed = false;

        IgniteInternalFuture startGridFut = GridTestUtils.runAsync(() -> {
            try {
                IgniteEx grid = startGrid(0);

                grid.cluster().active(true);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to start node.", e);
            }
        });

        try {
            startGridFut.get();
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Failed to start node."));

            failed = true;
        }

        Assert.assertTrue("Cache initialization must failed", failed);

        // Grid should be successfully recovered after stopping.
        ioFactory = null;

        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);
    }

    /**
     * Test node stopping & recovering on checkpoint begin fail.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testRecoveringOnCheckpointBeginFail() throws Exception {
        // Fail to write checkpoint start marker tmp file at the second checkpoint. Pass only initial checkpoint.
        ioFactory = new FilteringFileIOFactory("START.bin" + FilePageStoreManager.TMP_SUFFIX, new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), 20));

        final IgniteEx grid = startGrid(0);
        grid.cluster().active(true);

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            grid.cache(CACHE_NAME).put(i, data);
        }

        String errMsg = "Failed to write checkpoint entry";

        boolean checkpointFailed = false;
        try {
            forceCheckpoint();
        }
        catch (IgniteCheckedException e) {
            if (e.getMessage().contains(errMsg))
                checkpointFailed = true;
        }

        Assert.assertTrue("Checkpoint must be failed by IgniteCheckedException: " + errMsg, checkpointFailed);

        // Grid should be automatically stopped after checkpoint fail.
        awaitStop(grid);

        // Grid should be successfully recovered after stopping.
        ioFactory = null;

        IgniteEx recoveredGrid = startGrid(0);
        recoveredGrid.cluster().active(true);

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            byte[] actualData = (byte[]) recoveredGrid.cache(CACHE_NAME).get(i);
            Assert.assertArrayEquals(data, actualData);
        }
    }

    /**
     * Test node stopping & recovering on checkpoint pages write fail.
     */
    @Test
    public void testRecoveringOnCheckpointWriteFail() throws Exception {
        // Fail write partition and index files at the second checkpoint. Pass only initial checkpoint.
        ioFactory = new FilteringFileIOFactory(".bin", new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), 128 * PAGE_SIZE));

        final IgniteEx grid = startGrid(0);
        grid.cluster().active(true);

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            grid.cache(CACHE_NAME).put(i, data);
        }

        boolean checkpointFailed = false;
        try {
            forceCheckpoint();
        }
        catch (IgniteCheckedException e) {
            for (Throwable t : e.getSuppressed())
                if (t.getCause() != null && t.getCause().getMessage().equals("Not enough space!"))
                    checkpointFailed = true;
        }

        Assert.assertTrue("Checkpoint must be failed by IOException (Not enough space!)", checkpointFailed);

        // Grid should be automatically stopped after checkpoint fail.
        awaitStop(grid);

        // Grid should be successfully recovered after stopping.
        ioFactory = null;

        IgniteEx recoveredGrid = startGrid(0);
        recoveredGrid.cluster().active(true);

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            byte[] actualData = (byte[]) recoveredGrid.cache(CACHE_NAME).get(i);
            Assert.assertArrayEquals(data, actualData);
        }
    }

    /**
     * Test node stopping & recovering on WAL writing fail with enabled MMAP (Batch allocation for WAL segments).
     */
    @Test
    public void testRecoveringOnWALWritingFail1() throws Exception {
        // Allow to allocate only 1 wal segment, fail on write to second.
        ioFactory = new FilteringFileIOFactory(".wal", new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), WAL_SEGMENT_SIZE));

        System.setProperty(IGNITE_WAL_MMAP, "true");

        doTestRecoveringOnWALWritingFail();
    }

    /**
     * Test node stopping & recovering on WAL writing fail with disabled MMAP.
     */
    @Test
    public void testRecoveringOnWALWritingFail2() throws Exception {
        // Fail somewhere on the second wal segment.
        ioFactory = new FilteringFileIOFactory(".wal", new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), (long) (1.5 * WAL_SEGMENT_SIZE)));

        System.setProperty(IGNITE_WAL_MMAP, "false");

        doTestRecoveringOnWALWritingFail();
    }

    /**
     * Test node stopping & recovery on WAL writing fail.
     */
    private void doTestRecoveringOnWALWritingFail() throws Exception {
        IgniteEx grid = startGrid(0);

        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)grid.context().cache().context().wal();

        wal.setFileIOFactory(ioFactory);

        grid.cluster().active(true);

        int failedPosition = -1;

        final int keysCount = 2000;

        final int dataSize = 2048;

        for (int i = 0; i < keysCount; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[dataSize];
            Arrays.fill(data, payload);

            try {
                grid.cache(CACHE_NAME).put(i, data);
            }
            catch (Exception e) {
                failedPosition = i;

                break;
            }
        }

        // We must be able to put something into cache before fail.
        Assert.assertTrue("One of the cache puts must be failed", failedPosition > 0);

        // Grid should be automatically stopped after WAL fail.
        awaitStop(grid);

        ioFactory = null;

        // Grid should be successfully recovered after stopping.
        grid = startGrid(0);

        grid.cluster().active(true);

        for (int i = 0; i < failedPosition; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[dataSize];
            Arrays.fill(data, payload);

            byte[] actualData = (byte[]) grid.cache(CACHE_NAME).get(i);
            Assert.assertArrayEquals(data, actualData);
        }
    }

    /**
     * Use stop node on critical error handler for this test class.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Failure handler instance.
     */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     *
     */
    private void awaitStop(final IgniteEx grid) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(() -> grid.context().gateway().getState() == GridKernalState.STOPPED, STOP_TIMEOUT_MS);
    }

    /**
     *
     */
    private static class LimitedSizeFileIO extends FileIODecorator {
        /** */
        private final AtomicLong availableSpaceBytes;

        /**
         * @param delegate File I/O delegate.
         * @param availableSpaceBytes Shared counter which indicates the number of available bytes in a FS.
         */
        public LimitedSizeFileIO(FileIO delegate, AtomicLong availableSpaceBytes) {
            super(delegate);
            this.availableSpaceBytes = availableSpaceBytes;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            int written = super.write(srcBuf);
            availableSpaceBytes.addAndGet(-written);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
            return written;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            int written = super.write(srcBuf, position);
            availableSpaceBytes.addAndGet(-written);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
            return written;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            final int num = super.write(buf, off, len);
            availableSpaceBytes.addAndGet(-len);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
            return num;
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
            availableSpaceBytes.addAndGet(-sizeBytes);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
            return super.map(sizeBytes);
        }
    }

    /**
     * Factory to provide custom File I/O interfaces only for files with specified suffix.
     * For other files {@link RandomAccessFileIO} will be used.
     */
    private static class FilteringFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate. */
        private final FileIOFactory delegate;

        /** File suffix pattern. */
        private final String pattern;

        /**
         * Constructor.
         *
         * @param pattern File suffix pattern.
         * @param delegate I/O Factory delegate.
         */
        FilteringFileIOFactory(String pattern, FileIOFactory delegate) {
            this.delegate = delegate;
            this.pattern = pattern;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (file.getName().endsWith(pattern))
                return delegate.create(file, modes);
            return new RandomAccessFileIO(file, modes);
        }
    }

    /**
     *
     */
    private static class LimitedSizeFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private final FileIOFactory delegate;

        /** */
        private final AtomicLong availableSpaceBytes;

        /**
         * @param delegate File I/O factory delegate.
         * @param fsSpaceBytes Number of available bytes in FS.
         */
        private LimitedSizeFileIOFactory(FileIOFactory delegate, long fsSpaceBytes) {
            this.delegate = delegate;
            this.availableSpaceBytes = new AtomicLong(fsSpaceBytes);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new LimitedSizeFileIO(delegate.create(file, modes), availableSpaceBytes);
        }
    }
}
