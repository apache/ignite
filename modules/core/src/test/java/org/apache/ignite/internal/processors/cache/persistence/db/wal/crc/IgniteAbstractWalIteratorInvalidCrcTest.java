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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteAbstractWalIteratorInvalidCrcTest extends GridCommonAbstractTest {
    /** Size of inserting dummy value. */
    private static final int VALUE_SIZE = 4 * 1024;

    /** Size of WAL segment file. */
    private static final int WAL_SEGMENT_SIZE = 1024 * 1024;

    /** Count of WAL segment files in working directory. */
    private static final int WAL_SEGMENTS = DataStorageConfiguration.DFLT_WAL_SEGMENTS;

    /** Ignite instance. */
    protected IgniteEx ignite;

    /** Random instance for utility purposes. */
    protected Random random = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setWalMode(getWalMode())
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        ignite = (IgniteEx)startGrid();

        ignite.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        byte[] val = new byte[VALUE_SIZE];

        // Fill value with random data.
        random.nextBytes(val);

        // Amount of values that's enough to fill working dir at least twice.
        int insertingCnt = 2 * WAL_SEGMENT_SIZE * WAL_SEGMENTS / VALUE_SIZE;
        for (int i = 0; i < insertingCnt; i++)
            cache.put(i, val);

        ignite.cluster().active(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();

        cleanPersistenceDir();
    }

    /**
     * @return WAL mode that will be used in {@link IgniteConfiguration}.
     */
    @NotNull protected abstract WALMode getWalMode();

    /**
     * Instantiate WAL iterator according to the iterator type of specific implementation.
     * @param walMgr WAL manager instance.
     * @param ignoreArchiveDir Do not include archive segments in resulting iterator if this flag is true.
     * @return WAL iterator instance.
     * @throws IgniteCheckedException If iterator creation failed for some reason.
     */
    @NotNull protected abstract WALIterator getWalIterator(
        IgniteWriteAheadLogManager walMgr,
        boolean ignoreArchiveDir
    ) throws IgniteCheckedException;

    /**
     * Test that iteration fails if one of archive segments contains record with invalid CRC.
     * @throws Exception If failed.
     */
    @Test
    public void testArchiveCorruptedPtr() throws Exception {
        doTest((archiveDescs, descs) -> archiveDescs.get(random.nextInt(archiveDescs.size())), false, true);
    }

    /**
     * Test that iteration fails if one of segments in working directory contains record with invalid CRC
     * and it is not the tail segment.
     * @throws Exception If failed.
     */
    @Test
    public void testNotTailCorruptedPtr() throws Exception {
        doTest((archiveDescs, descs) -> descs.get(random.nextInt(descs.size() - 1)), true, true);
    }


    /**
     * Test that iteration does not fail if tail segment in working directory contains record with invalid CRC.
     * @throws Exception If failed.
     */
    @Test
    public void testTailCorruptedPtr() throws Exception {
        doTest((archiveDescs, descs) -> descs.get(descs.size() - 1), false, false);
    }

    /**
     * @param descPicker Function that picks WAL segment to corrupt from archive segments list
     *      and working directory segments list.
     * @param ignoreArchiveDir Do not iterate over archive segments if this flag is true.
     * @param shouldFail Whether iteration is axpected to fail or not.
     * @throws IOException If IO exception.
     * @throws IgniteCheckedException If iterator failed.
     */
    protected void doTest(
        BiFunction<List<FileDescriptor>, List<FileDescriptor>, FileDescriptor> descPicker,
        boolean ignoreArchiveDir,
        boolean shouldFail
    ) throws IOException, IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory();

        File walArchiveDir = U.field(walMgr, "walArchiveDir");
        List<FileDescriptor> archiveDescs = iterFactory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(walArchiveDir)
        );

        File walDir = U.field(walMgr, "walWorkDir");
        List<FileDescriptor> descs = iterFactory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(walDir)
        );

        FileDescriptor corruptedDesc = descPicker.apply(archiveDescs, descs);

        WALPointer beforeCorruptedPtr = WalTestUtils.corruptWalSegmentFile(
            corruptedDesc,
            iterFactory,
            random
        );

        if (shouldFail) {
            WALPointer[] lastReadPtrRef = new WALPointer[1];

            IgniteException igniteException = (IgniteException) GridTestUtils.assertThrows(log, () -> {
                try (WALIterator iter = getWalIterator(walMgr, ignoreArchiveDir)) {
                    for (IgniteBiTuple<WALPointer, WALRecord> tuple : iter) {
                        WALPointer ptr = tuple.get1();
                        lastReadPtrRef[0] = ptr;
                    }
                }

                return null;
            }, IgniteException.class, "Failed to read WAL record");

            assertTrue(igniteException.hasCause(IgniteDataIntegrityViolationException.class));

            WALPointer lastReadPtr = lastReadPtrRef[0];
            assertNotNull(lastReadPtr);

            // WAL iterator advances to the next record and only then returns current one,
            // so next record has to be valid as well.
            assertEquals(lastReadPtr, beforeCorruptedPtr);
        }
        else
            try (WALIterator iter = getWalIterator(walMgr, ignoreArchiveDir)) {
                while (iter.hasNext())
                    iter.next();
            }
    }
}
