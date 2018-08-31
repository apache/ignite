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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
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

    protected static final int IDX = 0;
    /** Cache name. */
    protected static final String CACHE_NAME = "cache1";
    /** Keys count. */
    protected static final int KEYS_CNT = 50;

    /** Account value origin. */
    protected static final long ACCOUNT_VAL_ORIGIN = 100;

    /** Account value bound. */
    protected static final long ACCOUNT_VAL_BOUND = 1000;

    public void testCorrectClosingFileDescriptors() throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        startGrid(cfg);
        Ignite ignite = ignite(IDX);
        final String walDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), ignite.configuration().getDataStorageConfiguration().getWalArchivePath(), false).getAbsolutePath();
        ignite.cluster().active(true);
        IgniteCacheDatabaseSharedManager sharedManager = ((IgniteEx)ignite).context().cache().context().database();
        IgniteWriteAheadLogManager walManager = ((IgniteEx)ignite).context().cache().context().wal();
        for (int i = 0; i < 2 * ignite.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedManager.checkpointReadLock();
            walManager.log(new SnapshotRecord(i, false));
            sharedManager.checkpointReadUnlock();
        }
        stopGrid(IDX);
        log.info("WAL open:  " + CountedFileIO.getCountOpenedWalFiles() + " close: " + CountedFileIO.getCountClosedWalFiles());
        WALIterator iterator = new IgniteWalIteratorFactory(log).iterator(new IgniteWalIteratorFactory.IteratorParametersBuilder().ioFactory(new CountedFileIOFactory()).copy().filesOrDirs(walDir));
        iterator.forEach(x -> {
        });
        assertTrue("At least one WAL must be opened!", CountedFileIO.getCountOpenedWalFiles() > 0);
        assertEquals("All WAL files must be closed!", CountedFileIO.getCountOpenedWalFiles(), CountedFileIO.getCountClosedWalFiles());
    }

    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(IDX));
        cfg.setDataStorageConfiguration(new DataStorageConfiguration());
        cfg.getDataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true));
        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        // cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @return Sum of all cache values.
     */
    protected long populateData(Ignite ignite, String cacheName) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long total = 0;

        try (IgniteDataStreamer<Integer, Long> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(false);

            for (int i = 0; i < KEYS_CNT; i++) {
                long val = rnd.nextLong(ACCOUNT_VAL_ORIGIN, ACCOUNT_VAL_BOUND + 1);

                dataStreamer.addData(i, val);

                total += val;
            }

            dataStreamer.flush();
        }

        log.info("Total sum for cache '" + cacheName + "': " + total);

        return total;
    }

    private static class CountedFileIOFactory extends RandomAccessFileIOFactory {
        @Override public FileIO create(File file) throws IOException {
            return this.create(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new CountedFileIO(file, modes);
        }
    }

    private static class CountedFileIO extends RandomAccessFileIO {

        private static final AtomicInteger WAL_OPEN_COUNTER = new AtomicInteger();
        private static final AtomicInteger WAL_CLOSE_COUNTER = new AtomicInteger();

        private final String fileName;

        public CountedFileIO(File file, OpenOption... modes) throws IOException {
            super(file, modes);
            fileName = file.getAbsolutePath();
            if (fileName.endsWith(".wal"))
                WAL_OPEN_COUNTER.incrementAndGet();
        }

        @Override public void close() throws IOException {
            super.close();
            if (fileName.endsWith(".wal"))
                WAL_CLOSE_COUNTER.incrementAndGet();
        }

        public static int getCountOpenedWalFiles() {
            return WAL_OPEN_COUNTER.get();
        }

        public static int getCountClosedWalFiles() {
            return WAL_CLOSE_COUNTER.get();
        }
    }
}