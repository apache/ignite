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
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class StandaloneWalRecordsIteratorTest extends GridCommonAbstractTest {
    /** Index. */
    protected static final int IDX = 0;

    /** Cache name. */
    protected static final String CACHE_NAME = "cache1";

    /** Keys count. */
    protected static final int KEYS_CNT = 50;

    /**
     * @throws Exception If failed.
     */
    public void testCorrectClosingFileDescriptors() throws Exception {
        startGrid(getConfiguration());

        Ignite ignite = ignite(IDX);

        final String archiveWalDir = getArchiveWalDirPath(ignite);

        ignite.cluster().active(true);

        IgniteCacheDatabaseSharedManager sharedMgr = ((IgniteEx)ignite).context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ((IgniteEx)ignite).context().cache().context().wal();

        for (int i = 0; i < 2 * ignite.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            walMgr.log(new SnapshotRecord(i, false));

            sharedMgr.checkpointReadUnlock();
        }

        stopGrid(IDX);

        createWalIterator(archiveWalDir).forEach(x -> {});

        assertTrue("At least one WAL file must be opened!", CountedFileIO.getCountOpenedWalFiles() > 0);
        assertEquals("All WAL files must be closed!", CountedFileIO.getCountOpenedWalFiles(), CountedFileIO.getCountClosedWalFiles());
    }

    /**
     * @param walDir
     * @return
     * @throws IgniteCheckedException
     */
    private WALIterator createWalIterator(String walDir) throws IgniteCheckedException {
        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder();

        params.ioFactory(new CountedFileIOFactory());

        return new IgniteWalIteratorFactory(log).iterator(params.copy().filesOrDirs(walDir));
    }

    /** */
    private String getArchiveWalDirPath(Ignite ignite) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), ignite.configuration().getDataStorageConfiguration().getWalArchivePath(), false).getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(IDX));
        cfg.setDataStorageConfiguration(new DataStorageConfiguration());
        cfg.getDataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true));
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
    private static class CountedFileIOFactory extends RandomAccessFileIOFactory {
        @Override public FileIO create(File file) throws IOException {
            return this.create(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new CountedFileIO(file, modes);
        }
    }

    /**
     *
     */
    private static class CountedFileIO extends RandomAccessFileIO {

        /** Wal file extension. */
        private static final String WAL_FILE_EXTENSION = ".wal";
        /** Wal open counter. */
        private static final AtomicInteger WAL_OPEN_COUNTER = new AtomicInteger();
        /** Wal close counter. */
        private static final AtomicInteger WAL_CLOSE_COUNTER = new AtomicInteger();

        /** File name. */
        private final String fileName;

        /**
         * @param file File.
         * @param modes Modes.
         */
        public CountedFileIO(File file, OpenOption... modes) throws IOException {
            super(file, modes);
            fileName = file.getAbsolutePath();
            if (fileName.endsWith(WAL_FILE_EXTENSION))
                WAL_OPEN_COUNTER.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            super.close();
            if (fileName.endsWith(WAL_FILE_EXTENSION))
                WAL_CLOSE_COUNTER.incrementAndGet();
        }

        /**
         * @return
         */
        public static int getCountOpenedWalFiles() {
            return WAL_OPEN_COUNTER.get();
        }

        /**
         * @return
         */
        public static int getCountClosedWalFiles() {
            return WAL_CLOSE_COUNTER.get();
        }
    }
}