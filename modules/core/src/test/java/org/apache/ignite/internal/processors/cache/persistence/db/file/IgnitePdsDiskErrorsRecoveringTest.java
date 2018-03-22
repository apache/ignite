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
import org.apache.ignite.Ignite;
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
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;

/**
 * Tests node recovering after disk errors during interaction with persistent storage.
 */
public class IgnitePdsDiskErrorsRecoveringTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = DataStorageConfiguration.DFLT_PAGE_SIZE;

    /** */
    private static final int WAL_SEGMENT_SIZE = 1024 * PAGE_SIZE;

    /** */
    private static final long DFLT_DISK_SPACE_BYTES = Long.MAX_VALUE;

    /** */
    private static final long STOP_TIMEOUT_MS = 30 * 1000;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private boolean failPageStoreDiskOperations = false;

    /** */
    private long diskSpaceBytes = DFLT_DISK_SPACE_BYTES;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        failPageStoreDiskOperations = false;
        diskSpaceBytes = DFLT_DISK_SPACE_BYTES;
        System.clearProperty(IGNITE_WAL_MMAP);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY)
                .setWalCompactionEnabled(false)
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        if (failPageStoreDiskOperations)
            dsCfg.setFileIOFactory(new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), diskSpaceBytes));

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
     *
     */
    public void testRecoveringOnCacheInitError() throws Exception {
        failPageStoreDiskOperations = true;

        // Two pages is enough to initialize MetaStorage.
        diskSpaceBytes = 2 * PAGE_SIZE;

        final IgniteEx grid = startGrid(0);

        boolean failed = false;
        try {
            grid.active(true);
        } catch (Exception expected) {
            log.warning("Expected cache error", expected);

            failed = true;
        }

        Assert.assertTrue("Cache initialization must failed", failed);

        // Grid should be automatically stopped after checkpoint fail.
        awaitStop(grid);

        // Grid should be successfully recovered after stopping.
        failPageStoreDiskOperations = false;

        IgniteEx recoveredGrid = startGrid(0);
        recoveredGrid.active(true);
    }

    /**
     *
     */
    public void testRecoveringOnCheckpointWritingError() throws Exception {
        failPageStoreDiskOperations = true;
        diskSpaceBytes = 1024 * PAGE_SIZE;

        final IgniteEx grid = startGrid(0);
        grid.active(true);

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
        failPageStoreDiskOperations = false;

        IgniteEx recoveredGrid = startGrid(0);
        recoveredGrid.active(true);

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            byte[] actualData = (byte[]) recoveredGrid.cache(CACHE_NAME).get(i);
            Assert.assertArrayEquals(data, actualData);
        }
    }

    /**
     *
     */
    public void testRecoveringOnWALErrorWithMmap() throws Exception {
        diskSpaceBytes = WAL_SEGMENT_SIZE;
        System.setProperty(IGNITE_WAL_MMAP, "true");
        emulateRecoveringOnWALWritingError();
    }

    /**
     *
     */
    public void testRecoveringOnWALErrorWithoutMmap() throws Exception {
        diskSpaceBytes = 2 * WAL_SEGMENT_SIZE;
        System.setProperty(IGNITE_WAL_MMAP, "false");
        emulateRecoveringOnWALWritingError();
    }

    /**
     *
     */
    private void emulateRecoveringOnWALWritingError() throws Exception {
        final IgniteEx grid = startGrid(0);

        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)grid.context().cache().context().wal();
        wal.setFileIOFactory(new LimitedSizeFileIOFactory(new RandomAccessFileIOFactory(), diskSpaceBytes));

        grid.active(true);

        int failedPosition = -1;

        for (int i = 0; i < 1000; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
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
        Assert.assertTrue(failedPosition > 0);

        // Grid should be automatically stopped after WAL fail.
        awaitStop(grid);

        // Grid should be successfully recovered after stopping.
        IgniteEx recoveredGrid = startGrid(0);
        recoveredGrid.active(true);

        for (int i = 0; i < failedPosition; i++) {
            byte payload = (byte) i;
            byte[] data = new byte[2048];
            Arrays.fill(data, payload);

            byte[] actualData = (byte[]) recoveredGrid.cache(CACHE_NAME).get(i);
            Assert.assertArrayEquals(data, actualData);
        }
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
        @Override public void write(byte[] buf, int off, int len) throws IOException {
            super.write(buf, off, len);
            availableSpaceBytes.addAndGet(-len);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int maxWalSegmentSize) throws IOException {
            availableSpaceBytes.addAndGet(-maxWalSegmentSize);
            if (availableSpaceBytes.get() < 0)
                throw new IOException("Not enough space!");
            return super.map(maxWalSegmentSize);
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
        @Override public FileIO create(File file) throws IOException {
            return new LimitedSizeFileIO(delegate.create(file), availableSpaceBytes);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new LimitedSizeFileIO(delegate.create(file, modes), availableSpaceBytes);
        }
    }
}
