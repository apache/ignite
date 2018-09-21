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
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;

/**
 * The test check, that StandaloneWalRecordsIterator correctly close file descriptors associated with WAL files.
 */
public class StandaloneWalRecordsIteratorTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        ).setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(IP_FINDER)
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
     * Check correct closing file descriptors.
     *
     * @throws Exception if test failed.
     */
    public void testCorrectClosingFileDescriptors() throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        String archiveWalDir = getArchiveWalDirPath(ig);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // Generate WAL segments for filling WAL archive folder.
        for (int i = 0; i < 2 * ig.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            try {
                walMgr.log(new SnapshotRecord(i, false));
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        stopGrid();

        // Iterate by all archived WAL segments.
        createWalIterator(archiveWalDir).forEach(x -> {
        });

        assertTrue("At least one WAL file must be opened!", CountedFileIO.getCountOpenedWalFiles() > 0);

        assertEquals("All WAL files must be closed!", CountedFileIO.getCountOpenedWalFiles(), CountedFileIO.getCountClosedWalFiles());
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
        @Override public FileIO create(File file) throws IOException {
            return create(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

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