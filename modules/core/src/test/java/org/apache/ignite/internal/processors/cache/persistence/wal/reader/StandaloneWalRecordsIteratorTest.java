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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;

/**
 * The test check, that StandaloneWalRecordsIterator correctly close file descriptors associated with WAL files.
 */
public class StandaloneWalRecordsIteratorTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    private String createWalFiles() throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        String archiveWalDir = getArchiveWalDirPath(ig);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // Generate WAL segments for filling WAL archive folder.
        for (int i = 0; i < 2 * ig.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            try {
                walMgr.log(new SnapshotRecord(i, false), RolloverType.NEXT_SEGMENT);
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        stopGrid();

        return archiveWalDir;
    }

    /**
     * Check correct closing file descriptors.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testCorrectClosingFileDescriptors() throws Exception {

        // Iterate by all archived WAL segments.
        createWalIterator(createWalFiles()).forEach(x -> {
        });

        assertTrue("At least one WAL file must be opened!", CountedFileIO.getCountOpenedWalFiles() > 0);

        assertTrue("All WAL files must be closed at least ones!", CountedFileIO.getCountOpenedWalFiles() <= CountedFileIO.getCountClosedWalFiles());
    }

    /**
     * Check correct check bounds.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testStrictBounds() throws Exception {
        String dir = createWalFiles();

        FileWALPointer lowBound = null, highBound = null;

        for (IgniteBiTuple<WALPointer, WALRecord> p : createWalIterator(dir, null, null, false)) {
            if (lowBound == null)
                lowBound = (FileWALPointer) p.get1();

            highBound = (FileWALPointer) p.get1();
        }

        assertNotNull(lowBound);

        assertNotNull(highBound);

        createWalIterator(dir, lowBound, highBound, true);

        final FileWALPointer lBound = lowBound;
        final FileWALPointer hBound = highBound;

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, new FileWALPointer(lBound.index() - 1, 0, 0), hBound, true);

            return 0;
        }, IgniteCheckedException.class, null);

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, lBound, new FileWALPointer(hBound.index() + 1, 0, 0), true);

            return 0;
        }, IgniteCheckedException.class, null);

        List<FileDescriptor> walFiles = listWalFiles(dir);

        assertNotNull(walFiles);

        assertTrue(!walFiles.isEmpty());

        assertTrue(walFiles.get(new Random().nextInt(walFiles.size())).file().delete());

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, lBound, hBound, true);

            return 0;
        }, IgniteCheckedException.class, null);
    }

    /**
     * Creates WALIterator associated with files inside walDir.
     *
     * @param walDir - path to WAL directory.
     * @return WALIterator associated with files inside walDir.
     * @throws IgniteCheckedException if error occur.
     */
    private WALIterator createWalIterator(String walDir) throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new CountedFileIOFactory());

        return new IgniteWalIteratorFactory(log).iterator(params.filesOrDirs(walDir));
    }

    /**
     * @param walDir Wal directory.
     */
    private List<FileDescriptor> listWalFiles(String walDir) throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new RandomAccessFileIOFactory());

        return new IgniteWalIteratorFactory(log).resolveWalFiles(params.filesOrDirs(walDir));
    }

    /**
     * @param walDir Wal directory.
     * @param lowBound Low bound.
     * @param highBound High bound.
     * @param strictCheck Strict check.
     */
    private WALIterator createWalIterator(String walDir, FileWALPointer lowBound, FileWALPointer highBound, boolean strictCheck)
                    throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new RandomAccessFileIOFactory()).
            filesOrDirs(walDir).
            strictBoundsCheck(strictCheck);

        if (lowBound != null)
            params.from(lowBound);

        if (lowBound != null)
            params.to(highBound);

        return new IgniteWalIteratorFactory(log).iterator(params);
    }

    /**
     * Evaluate path to directory with WAL archive.
     *
     * @param ignite instance of Ignite.
     * @return path to directory with WAL archive.
     * @throws IgniteCheckedException if error occur.
     */
    private String getArchiveWalDirPath(Ignite ignite) throws IgniteCheckedException {
        return U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            ignite.configuration().getDataStorageConfiguration().getWalArchivePath(),
            false
        ).getAbsolutePath();
    }

    /**
     *
     */
    private static class CountedFileIOFactory extends RandomAccessFileIOFactory {
        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new CountedFileIO(file, modes);
        }
    }

    /**
     *
     */
    private static class CountedFileIO extends RandomAccessFileIO {
        /** Wal open counter. */
        private static final AtomicInteger WAL_OPEN_COUNTER = new AtomicInteger();

        /** Wal close counter. */
        private static final AtomicInteger WAL_CLOSE_COUNTER = new AtomicInteger();

        /** File name. */
        private final String fileName;

        /** */
        public CountedFileIO(File file, OpenOption... modes) throws IOException {
            super(file, modes);

            fileName = file.getName();

            if (FileWriteAheadLogManager.WAL_NAME_PATTERN.matcher(fileName).matches())
                WAL_OPEN_COUNTER.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            super.close();

            if (FileWriteAheadLogManager.WAL_NAME_PATTERN.matcher(fileName).matches())
                WAL_CLOSE_COUNTER.incrementAndGet();
        }

        /**
         *
         * @return number of opened files.
         */
        public static int getCountOpenedWalFiles() { return WAL_OPEN_COUNTER.get(); }

        /**
         *
         * @return number of closed files.
         */
        public static int getCountClosedWalFiles() { return WAL_CLOSE_COUNTER.get(); }
    }
}
