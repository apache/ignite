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
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static java.nio.ByteBuffer.allocate;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;

/**
 *
 */
public abstract class IgniteAbstractWalIteratorInvalidCrcTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

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
    public void testArchiveCorruptedPtr() throws Exception {
        doTest((archiveDescs, descs) -> archiveDescs.get(random.nextInt(archiveDescs.size())), false, true);
    }

    /**
     * Test that iteration fails if one of segments in working directory contains record with invalid CRC
     * and it is not the tail segment.
     * @throws Exception If failed.
     */
    public void testNotTailCorruptedPtr() throws Exception {
        doTest((archiveDescs, descs) -> descs.get(random.nextInt(descs.size() - 1)), true, true);
    }


    /**
     * Test that iteration does not fail if tail segment in working directory contains record with invalid CRC.
     * @throws Exception If failed.
     */
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

        FileWALPointer beforeCorruptedPtr = corruptWalSegmentFile(
            corruptedDesc,
            iterFactory
        );

        if (shouldFail) {
            FileWALPointer[] lastReadPtrRef = new FileWALPointer[1];

            IgniteException igniteException = (IgniteException) GridTestUtils.assertThrows(log, () -> {
                try (WALIterator iter = getWalIterator(walMgr, ignoreArchiveDir)) {
                    for (IgniteBiTuple<WALPointer, WALRecord> tuple : iter) {
                        FileWALPointer ptr = (FileWALPointer)tuple.get1();
                        lastReadPtrRef[0] = ptr;
                    }
                }

                return null;
            }, IgniteException.class, "Failed to read WAL record");

            assertTrue(igniteException.hasCause(IgniteDataIntegrityViolationException.class));

            FileWALPointer lastReadPtr = lastReadPtrRef[0];
            assertNotNull(lastReadPtr);

            // WAL iterator advances to the next record and only then returns current one,
            // so next record has to be valid as well.
            assertEquals(lastReadPtr.next(), beforeCorruptedPtr);
        }
        else
            try (WALIterator iter = getWalIterator(walMgr, ignoreArchiveDir)) {
                while (iter.hasNext())
                    iter.next();
            }
    }

    /**
     * Put zero CRC in one of records for the specified segment.
     * @param desc WAL segment descriptor.
     * @param iterFactory Iterator factory for segment iterating.
     * @return Descriptor that is located strictly before the corrupted one.
     * @throws IOException If IO exception.
     * @throws IgniteCheckedException If iterator failed.
     */
    protected FileWALPointer corruptWalSegmentFile(
        FileDescriptor desc,
        IgniteWalIteratorFactory iterFactory
    ) throws IOException, IgniteCheckedException {
        List<FileWALPointer> pointers = new ArrayList<>();

        try (WALIterator it = iterFactory.iterator(desc.file())) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                pointers.add((FileWALPointer) tuple.get1());
            }
        }

        // Should have a previous record to return and another value before that to ensure that "lastReadPtr"
        // in "doTest" will always exist.
        int idxCorrupted = 2 + random.nextInt(pointers.size() - 2);

        FileWALPointer pointer = pointers.get(idxCorrupted);
        int crc32Off = pointer.fileOffset() + pointer.length() - CRC_SIZE;

        ByteBuffer zeroCrc32 = allocate(CRC_SIZE); // Has 0 value by default.

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();
        try (FileIO io = ioFactory.create(desc.file(), WRITE)) {
            io.write(zeroCrc32, crc32Off);

            io.force(true);
        }

        return pointers.get(idxCorrupted - 1);
    }
}
