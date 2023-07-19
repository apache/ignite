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

package org.apache.ignite.internal.processors.cache.persistence.cdc;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
public class RealtimeCdcBufferTest extends GridCommonAbstractTest {
    /** */
    private static final String CONSISTENT_ID = "ID";

    /** */
    private static ListeningTestLogger lsnrLog;

    /** */
    private static CountDownLatch stopLatch;

    /** */
    private ByteBufferCdcConsumer consumer;

    /** */
    private boolean cdcEnabled;

    /** */
    private int maxCdcBufSize;

    /** */
    private AtomicInteger commitCnt;

    /** */
    @Parameterized.Parameter()
    public WALMode walMode;

    /** */
    @Parameterized.Parameters(name = "walMode={0}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] { WALMode.LOG_ONLY },
            new Object[] { WALMode.BACKGROUND },
            new Object[] { WALMode.FSYNC }
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        consumer = new ByteBufferCdcConsumer(commitCnt);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMaxCdcBufferSize(maxCdcBufSize)
            .setCdcConsumer(consumer)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(cdcEnabled)
                .setPersistenceEnabled(true)  // TODO: test for in-memory mode.
            )
            .setWalMode(walMode)
        );

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setGridLogger(lsnrLog);

        cfg.setConsistentId(CONSISTENT_ID);

        return cfg;
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        lsnrLog = new ListeningTestLogger(log);

        cleanPersistenceDir();

        cdcEnabled = true;

        stopLatch = null;
        commitCnt = null;
        consumer = null;
    }

    /** */
    @Test
    public void testCdcBufferOverflow() throws Exception {
        // TODO: Looks like there is a bug in the FSYNC case: WAL misses some records (HEADER_RECORD).
        assumeFalse(walMode == WALMode.FSYNC);

        maxCdcBufSize = 100 * (int)U.KB;

        checkCdcBufferOverflow(buildEntries(100, 10 * (int)U.KB), true);
    }

    /** */
    @Test
    public void testCdcDisabled() throws Exception {
        cdcEnabled = false;

        checkCdcBufferOverflow(buildEntries(100, 10 * (int)U.KB), false);
    }

    /** */
    @Test
    public void testCdcRecords() throws Exception {
        // TODO: Looks like there is a bug in the FSYNC case: WAL misses some records (HEADER_RECORD).
        assumeFalse(walMode == WALMode.FSYNC);

        maxCdcBufSize = 100 * (int)U.MB;
        commitCnt = new AtomicInteger();

        IgniteEx crd = startGrid(0);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = crd.cache(DEFAULT_CACHE_NAME);

        // Await while cluster is fully activated.
        cache.put(0, 0);

        int expCommitCnt = 5;

        commitCnt.set(expCommitCnt);

        while (commitCnt.get() >= 0)
            cache.put(0, 0);

        forceCheckpoint(crd);

        stopGrid(0);

        try (WALIterator walIt = walIter(null)) {
            int cdcRecCnt = 0;

            while (walIt.hasNext()) {
                if (walIt.next().getValue().type() == WALRecord.RecordType.REALTIME_CDC_RECORD)
                    cdcRecCnt++;
            }

            assertEquals(expCommitCnt, cdcRecCnt);
        }
    }

    /** */
    @Test
    public void testCdcBufferContent() throws Exception {
        // TODO: Looks like there is a bug in the FSYNC case: WAL misses some records (HEADER_RECORD).
        assumeFalse(walMode == WALMode.FSYNC);

        maxCdcBufSize = 10 * (int)U.MB;

        int entriesCnt = 1;

        List<T2<Integer, byte[]>> expEntries = buildEntries((int)U.KB, entriesCnt);

        checkCdcBufferOverflow(expEntries, false);

        assertEquals(entriesCnt, expEntries.size());

        List<T2<Integer, byte[]>> cdcEntries = consumer.buf.stream()
            .map(e -> new T2<>((int)e.key(), (byte[])e.value()))
            .collect(Collectors.toList());

        assertEquals(expEntries.size(), cdcEntries.size());

        for (int i = 0; i < expEntries.size(); i++) {
            assertEquals(expEntries.get(i).getKey(), cdcEntries.get(i).getKey());
            assertTrue(Arrays.equals(expEntries.get(i).getValue(), cdcEntries.get(i).getValue()));
        }
    }

    /** */
    private List<T2<Integer, byte[]>> buildEntries(int size, int cnt) {
        List<T2<Integer, byte[]>> entries = new ArrayList<>();

        for (int i = 0; i < cnt; i++) {
            byte[] data = new byte[size];

            ThreadLocalRandom.current().nextBytes(data);

            entries.add(new T2<>(i, data));
        }

        return entries;
    }

    /** */
    private void checkCdcBufferOverflow(List<T2<Integer, byte[]>> entries, boolean shouldOverflow) throws Exception {
        stopLatch = cdcEnabled ? new CountDownLatch(entries.size()) : null;

        LogListener lsnr = LogListener.matches("CDC buffer has overflowed. Stop realtime mode of CDC.")
            .times(shouldOverflow ? 1 : 0)
            .build();

        lsnrLog.registerListener(lsnr);

        IgniteEx crd = startGrid(0);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = crd.cache(DEFAULT_CACHE_NAME);

        for (T2<Integer, byte[]> e: entries)
            cache.put(e.getKey(), e.getValue());

        forceCheckpoint(crd);

        if (stopLatch != null && !shouldOverflow)
            U.awaitQuiet(stopLatch);

        stopGrid(0);

        assertTrue(lsnr.check());

        try (WALIterator walIt = walIter((recType, recPtr) -> recType == WALRecord.RecordType.REALTIME_STOP_CDC_RECORD)) {
            AtomicInteger stopRecCnt = new AtomicInteger();

            walIt.forEach((rec) -> stopRecCnt.incrementAndGet());

            assertEquals(shouldOverflow ? 1 : 0, stopRecCnt.get());
        }
    }

    /** */
    private File walSegments() throws IgniteCheckedException {
        return U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/" + CONSISTENT_ID, false);
    }

    /** Get iterator over WAL. */
    private WALIterator walIter(@Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> filter) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walSegments())
            .filter(filter);

        return factory.iterator(params);
    }

    /** */
    private static class ByteBufferCdcConsumer implements CdcBufferConsumer {
        /** */
        private final List<CdcEvent> buf = new ArrayList<>();

        /** */
        private final AtomicInteger commitCnt;

        /** */
        ByteBufferCdcConsumer(@Nullable AtomicInteger commitCnt) {
            this.commitCnt = commitCnt;
        }

        /** {@inheritDoc} */
        @Override public boolean consume(Collection<CdcEvent> data) {
            buf.addAll(data);

            if (stopLatch != null) {
                for (int i = 0; i < data.size(); i++)
                    stopLatch.countDown();
            }

            return commitCnt != null && commitCnt.decrementAndGet() >= 0;
        }

        /** */
        @Override public void close() {
            for (int i = 0; i < stopLatch.getCount(); i++)
                stopLatch.countDown();
        }
    }
}
