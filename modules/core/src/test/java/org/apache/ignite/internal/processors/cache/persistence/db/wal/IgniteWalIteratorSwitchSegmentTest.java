/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.nio.file.Paths;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheIoManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FsyncModeFileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessorImpl;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;

/***
 * Test check correct switch segment if in the tail of segment have garbage.
 */
public class IgniteWalIteratorSwitchSegmentTest extends GridCommonAbstractTest {
    /** Segment file size. */
    private static final int SEGMENT_SIZE = 1024 * 1024;

    /** WAL segment file sub directory. */
    private static final String WORK_SUB_DIR = "/NODE/wal";

    /** WAL archive segment file sub directory. */
    private static final String ARCHIVE_SUB_DIR = "/NODE/walArchive";

    /** Serializer versions for check. */
    private int[] checkSerializerVers = new int[] {
        1,
        2
    };

    /** FileWriteAheadLogManagers for check. */
    private Class[] checkWalManagers = new Class[] {
        FileWriteAheadLogManager.class,
        FsyncModeFileWriteAheadLogManager.class
    };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.delete(Paths.get(U.defaultWorkDirectory()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(Paths.get(U.defaultWorkDirectory()));
    }

    /**
     * Test for check invariant, size of SWITCH_SEGMENT_RECORD should be 1 byte.
     *
     * @throws Exception If some thing failed.
     */
    public void testCheckSerializer() throws Exception {
        for (int serVer : checkSerializerVers) {
            checkInvariantSwitchSegmentSize(serVer);
        }
    }

    /**
     * @param serVer WAL serializer version.
     * @throws Exception If some thing failed.
     */
    private void checkInvariantSwitchSegmentSize(int serVer) throws Exception {
        GridKernalContext kctx = new StandaloneGridKernalContext(
            log, null, null) {
            @Override public IgniteCacheObjectProcessor cacheObjects() {
                return new IgniteCacheObjectProcessorImpl(this);
            }
        };

        RecordSerializer serializer = new RecordSerializerFactoryImpl(
            new GridCacheSharedContext<>(
                kctx,
                null,
                null,
                null,
                null,
                null,
                null,
                new IgniteCacheDatabaseSharedManager() {
                    @Override public int pageSize() {
                        return DataStorageConfiguration.DFLT_PAGE_SIZE;
                    }
                },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,

                null)
        ).createSerializer(serVer);

        SwitchSegmentRecord switchSegmentRecord = new SwitchSegmentRecord();

        int recordSize = serializer.size(switchSegmentRecord);

        Assert.assertEquals(1, recordSize);
    }

    /**
     * Test for check invariant, size of SWITCH_SEGMENT_RECORD should be 1 byte.
     *
     * @throws Exception If some thing failed.
     */
    public void test() throws Exception {
        for (int serVer : checkSerializerVers) {
            for (Class walMgrClass : checkWalManagers) {
                try {
                    log.info("checking wal manager " + walMgrClass + " with serializer version " + serVer);

                    checkInvariantSwitchSegment(walMgrClass, serVer);
                }
                finally {
                    U.delete(Paths.get(U.defaultWorkDirectory()));
                }
            }
        }
    }

    /**
     * @param walMgrClass WAL manager class.
     * @param serVer WAL serializer version.
     * @throws Exception If some thing failed.
     */
    private void checkInvariantSwitchSegment(Class walMgrClass, int serVer) throws Exception {
        String workDir = U.defaultWorkDirectory();

        T2<IgniteWriteAheadLogManager, RecordSerializer> initTup = initiate(walMgrClass, serVer, workDir);

        IgniteWriteAheadLogManager walMgr = initTup.get1();

        RecordSerializer recordSerializer = initTup.get2();

        int switchSegmentRecordSize = recordSerializer.size(new SwitchSegmentRecord());

        log.info("switchSegmentRecordSize:" + switchSegmentRecordSize);

        int tailSize = 0;

        /* Initial record payload size. */
        int payloadSize = 1024;

        int recSize = 0;

        MetastoreDataRecord rec = null;

        /* Record size. */
        int recordTypeSize = 1;

        /* Record pointer. */
        int recordPointerSize = 8 + 4 + 4;

        int lowBound = recordTypeSize + recordPointerSize;
        int highBound = lowBound + /*CRC*/4;

        int attempt = 1000;

        // Try find how many record need for specific tail size.
        while (true) {
            if (attempt < 0)
                throw new IgniteCheckedException("Can not find any payload size for test, " +
                    "lowBound=" + lowBound + ", highBound=" + highBound);

            if (tailSize >= lowBound && tailSize < highBound)
                break;

            payloadSize++;

            byte[] payload = new byte[payloadSize];

            // Fake record for payload.
            rec = new MetastoreDataRecord("0", payload);

            recSize = recordSerializer.size(rec);

            tailSize = (SEGMENT_SIZE - HEADER_RECORD_SIZE) % recSize;

            attempt--;
        }

        Assert.assertNotNull(rec);

        int recordsToWrite = SEGMENT_SIZE / recSize;

        log.info("records to write " + recordsToWrite + " tail size " +
            (SEGMENT_SIZE - HEADER_RECORD_SIZE) % recSize);

        // Add more record for rollover to the next segment.
        recordsToWrite += 100;

        for (int i = 0; i < recordsToWrite; i++) {
            walMgr.log(new MetastoreDataRecord(rec.key(), rec.value()));
        }

        walMgr.flush(null, true);

        // Await archiver move segment to WAL archive.
        Thread.sleep(5000);

        // If switchSegmentRecordSize more that 1, it mean that invariant is broke.
        // Filling tail some garbage. Simulate tail garbage on rotate segment in WAL work directory.
        if (switchSegmentRecordSize > 1) {
            File seg = new File(workDir + ARCHIVE_SUB_DIR + "/0000000000000000.wal");

            FileIOFactory ioFactory = new RandomAccessFileIOFactory();

            FileIO seg0 = ioFactory.create(seg);

            byte[] bytes = new byte[tailSize];

            Random rnd = new Random();

            rnd.nextBytes(bytes);

            // Some record type.
            bytes[0] = (byte)(METASTORE_DATA_RECORD.ordinal() + 1);

            seg0.position((int)(seg0.size() - tailSize));

            seg0.write(bytes, 0, tailSize);

            seg0.force(true);

            seg0.close();
        }

        int expectedRecords = recordsToWrite;
        int actualRecords = 0;

        // Check that switch segment works as expected and all record is reachable.
        try (WALIterator it = walMgr.replay(null)) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                WALRecord rec0 = tup.get2();

                if (rec0.type() == METASTORE_DATA_RECORD)
                    actualRecords++;
            }
        }

        Assert.assertEquals("Not all records read during iteration.", expectedRecords, actualRecords);
    }

    /***
     * Initiate WAL manager.
     *
     * @param walMgrClass WAL manager class.
     * @param serVer WAL serializer version.
     * @param workDir Work directory path.
     * @return Tuple of WAL manager and WAL record serializer.
     * @throws IgniteCheckedException If some think failed.
     */
    private T2<IgniteWriteAheadLogManager, RecordSerializer> initiate(
        Class walMgrClass,
        int serVer,
        String workDir
    ) throws IgniteCheckedException {

        GridKernalContext kctx = new StandaloneGridKernalContext(
            log, null, null
        ) {
            @Override protected IgniteConfiguration prepareIgniteConfiguration() {
                IgniteConfiguration cfg = super.prepareIgniteConfiguration();

                cfg.setDataStorageConfiguration(
                    new DataStorageConfiguration()
                        .setWalSegmentSize(SEGMENT_SIZE)
                        .setWalMode(WALMode.FSYNC)
                        .setWalPath(workDir + WORK_SUB_DIR)
                        .setWalArchivePath(workDir + ARCHIVE_SUB_DIR)
                );

                cfg.setEventStorageSpi(new NoopEventStorageSpi());

                return cfg;
            }

            @Override public GridInternalSubscriptionProcessor internalSubscriptionProcessor() {
                return new GridInternalSubscriptionProcessor(this);
            }

            @Override public GridEventStorageManager event() {
                return new GridEventStorageManager(this);
            }
        };

        IgniteWriteAheadLogManager walMgr = null;

        if (walMgrClass.equals(FileWriteAheadLogManager.class)) {
            walMgr = new FileWriteAheadLogManager(kctx);

            GridTestUtils.setFieldValue(walMgr, "serializerVer", serVer);
        }
        else if (walMgrClass.equals(FsyncModeFileWriteAheadLogManager.class)) {
            walMgr = new FsyncModeFileWriteAheadLogManager(kctx);

            GridTestUtils.setFieldValue(walMgr, "serializerVersion", serVer);
        }

        GridCacheSharedContext<?, ?> ctx = new GridCacheSharedContext<>(
            kctx,
            null,
            null,
            null,
            null,
            walMgr,
            new WalStateManager(kctx),
            new GridCacheDatabaseSharedManager(kctx),
            null,
            null,
            null,
            null,
            new GridCacheIoManager(),
            null,
            null,
            null,
            null
        );

        walMgr.start(ctx);

        walMgr.onActivate(kctx);

        walMgr.resumeLogging(null);

        RecordSerializer recordSerializer = new RecordSerializerFactoryImpl(ctx)
            .createSerializer(walMgr.serializerVersion());

        return new T2<>(walMgr, recordSerializer);
    }
}
