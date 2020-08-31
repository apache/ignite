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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV2Serializer;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridFilteredClosableIterator;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/**
 *
 */
public class IgniteWalSerializerVersionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(100L * 1024 * 1024)));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckDifferentSerializerVersions() throws Exception {
        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "1");

        IgniteEx ig0 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal0 = ig0.context().cache().context().wal();

        RecordSerializer ser0 = U.field(wal0, "serializer");

        assertTrue(ser0 instanceof RecordV1Serializer);

        stopGrid();

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "2");

        IgniteEx ig1 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal1 = ig1.context().cache().context().wal();

        RecordSerializer ser1 = U.field(wal1, "serializer");

        assertTrue(ser1 instanceof RecordV2Serializer);

        stopGrid();

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "3");

        GridTestUtils.assertThrowsAnyCause(log, new GPC<Void>() {
            @Override public Void call() throws Exception {
                startGrid();

                return null;
            }
        }, IgniteCheckedException.class, "Failed to create a serializer with the given version");

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "1");

        IgniteEx ig2 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal2 = ig2.context().cache().context().wal();

        RecordSerializer ser2 = U.field(wal2, "serializer");

        assertTrue(ser2 instanceof RecordV1Serializer);

        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckDifferentSerializerVersionsAndLogTimestamp() throws Exception {
        IgniteCallable<List<WALRecord>> recordsFactory = new IgniteCallable<List<WALRecord>>() {
            @Override public List<WALRecord> call() throws Exception {
                WALRecord rec0 = new DataRecord(Collections.<DataEntry>emptyList());

                WALRecord rec1 = new TxRecord(PREPARED, null, null, null);

                return Arrays.asList(rec0, rec1);
            }
        };

        long time0 = U.currentTimeMillis();

        check(new Checker(
            1,
            RecordV1Serializer.class,
            recordsFactory,
            Arrays.asList(0L, time0)
        ));

        long time1 = U.currentTimeMillis();

        check(new Checker(
            2,
            RecordV2Serializer.class,
            recordsFactory,
            Arrays.asList(time1, time1)
        ));
    }

    /**
     *
     */
    public static class Checker {
        /** */
        private final int serializerVer;

        /** */
        private final Class serializer;

        /** */
        private final List<Long> timeStamps;

        /** */
        private final IgniteCallable<List<WALRecord>> recordsToWrite;

        /**
         *
         */
        public Checker(
            int serializerVer,
            Class serializer,
            IgniteCallable<List<WALRecord>> recordsToWrite,
            List<Long> timeStamps) {
            this.serializerVer = serializerVer;
            this.serializer = serializer;
            this.timeStamps = timeStamps;
            this.recordsToWrite = recordsToWrite;
        }

        /**
         *
         */
        public int serializerVersion() {
            return serializerVer;
        }

        /**
         *
         */
        public Class serializer() {
            return serializer;
        }

        /**
         *
         */
        public List<Long> getTimeStamps() {
            return timeStamps;
        }

        /**
         *
         */
        public List<WALRecord> recordsToWrite() throws Exception {
            return recordsToWrite.call();
        }

        /**
         *
         */
        public void assertRecords(long exp, WALRecord act) {
            if (act instanceof TimeStampRecord) {
                TimeStampRecord act0 = (TimeStampRecord)act;

                if (exp == 0L)
                    assertTrue(act0.timestamp() == 0L);
                else {
                    long diff = Math.abs(exp - act0.timestamp());

                    assertTrue(String.valueOf(diff), diff < 10_000);
                }
            }
            else
                fail(String.valueOf(act));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void check(Checker checker) throws Exception {
        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, Integer.toString(checker.serializerVersion()));

        IgniteEx ig0 = (IgniteEx)startGrid();

        ig0.cluster().active(true);

        IgniteWriteAheadLogManager wal = ig0.context().cache().context().wal();

        RecordSerializer ser0 = U.field(wal, "serializer");

        assertTrue(ser0.getClass().getName().equals(checker.serializer().getName()));

        List<WALRecord> recs = checker.recordsToWrite();

        assertTrue(!recs.isEmpty());

        WALPointer p = null;

        for (WALRecord rec : recs) {
            WALPointer p0 = wal.log(rec);

            if (p == null)
                p = p0;
        }

        wal.flush(null, false);

        Iterator<Long> itToCheck = checker.getTimeStamps().iterator();

        try (TimestampRecordIterator it = new TimestampRecordIterator(wal.replay(p))) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup0 = it.next();

                checker.assertRecords(itToCheck.next(), tup0.get2());
            }
        }

        stopGrid();

        System.clearProperty(IGNITE_WAL_SERIALIZER_VERSION);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_WAL_SERIALIZER_VERSION);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_WAL_SERIALIZER_VERSION);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IGNITE_WAL_SERIALIZER_VERSION);
    }

    /**
     *
     */
    private static class TimestampRecordIterator extends GridFilteredClosableIterator<IgniteBiTuple<WALPointer, WALRecord>> {
        /**
         * @param it Iterator.
         */
        private TimestampRecordIterator(GridCloseableIterator<? extends IgniteBiTuple<WALPointer, WALRecord>> it) {
            super(it);
        }

        /** {@inheritDoc} */
        @Override protected boolean accept(IgniteBiTuple<WALPointer, WALRecord> tup) {
            return tup.get2() instanceof TimeStampRecord;
        }
    }
}
