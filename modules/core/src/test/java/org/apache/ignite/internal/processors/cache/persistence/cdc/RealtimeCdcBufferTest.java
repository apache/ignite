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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.util.typedef.internal.U;
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

        consumer = new ByteBufferCdcConsumer(maxCdcBufSize, commitCnt);

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
        maxCdcBufSize = 100 * (int)U.KB;

        checkCdcBufferOverflow(10 * (int)U.KB, 100, true);
    }

    /** */
    @Test
    public void testCdcDisabled() throws Exception {
        cdcEnabled = false;

        checkCdcBufferOverflow(10 * (int)U.KB, 100, false);
    }

    /** */
    @Test
    public void testCdcRecords() throws Exception {
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

        try (WALIterator walIt = walIter(walSegments())) {
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
        // TODO: Looks like there is a bug in the FSYNC case: WAL misses some records.
        assumeFalse(walMode == WALMode.FSYNC);

        maxCdcBufSize = 10 * (int)U.MB;

        stopLatch = new CountDownLatch(1);

        checkCdcBufferOverflow((int)U.KB, 100, false);

        U.awaitQuiet(stopLatch);

        File walSegments = walSegments();

        WALIterator it = walIter(walSegments);

        while (it.hasNext())
            it.next();

        WALPointer ptr = it.lastRead().get();
        int length = ptr.fileOffset() + ptr.length();

        File seg = Arrays.stream(walSegments.listFiles()).sorted().findFirst().get();

        byte[] walSegData = Files.readAllBytes(seg.toPath());

        int step = 100;

        for (int off = 0; off < length; off += step) {
            int l = off + step < length ? step : length - off;

            byte[] testWalData = new byte[l];
            byte[] testCdcData = new byte[l];

            ByteBuffer buf = ByteBuffer.wrap(walSegData);
            buf.position(off);
            buf.get(testWalData, 0, l);

            buf = ByteBuffer.wrap(consumer.buf.array());
            buf.position(off);
            buf.get(testCdcData, 0, l);

            assertTrue(
                "Offset " + off + "/" + length + "\n" +
                "EXPECT " + Arrays.toString(testWalData) + "\n" +
                "ACTUAL " + Arrays.toString(testCdcData),
                Arrays.equals(testWalData, testCdcData));
        }
    }

    /** */
    private void checkCdcBufferOverflow(int entrySize, int entryCnt, boolean shouldOverflow) throws Exception {
        LogListener lsnr = LogListener.matches("CDC buffer has overflowed. Stop realtime mode of CDC.")
            .times(shouldOverflow ? 1 : 0)
            .build();

        lsnrLog.registerListener(lsnr);

        IgniteEx crd = startGrid(0);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = crd.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < entryCnt; i++) {
            byte[] data = new byte[entrySize];

            Arrays.fill(data, (byte)1);

            cache.put(i, data);
        }

        forceCheckpoint(crd);

        stopGrid(0);

        assertTrue(lsnr.check());

        try (WALIterator walIt = walIter(walSegments())) {
            int stopRecCnt = 0;

            while (walIt.hasNext()) {
                if (walIt.next().getValue().type() == WALRecord.RecordType.REALTIME_STOP_CDC_RECORD)
                    stopRecCnt++;
            }

            assertEquals(shouldOverflow ? 1 : 0, stopRecCnt);
        }
    }

    /** */
    private File walSegments() throws IgniteCheckedException {
        return U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/" + CONSISTENT_ID, false);
    }

    /** Get iterator over WAL. */
    private WALIterator walIter(File walSegments) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walSegments);

        return factory.iterator(params);
    }

    /** */
    private static class ByteBufferCdcConsumer implements CdcBufferConsumer {
        /** */
        private final ByteBuffer buf;

        /** */
        private final AtomicInteger commitCnt;

        /** */
        ByteBufferCdcConsumer(int maxCdcBufSize, @Nullable AtomicInteger commitCnt) {
            this.commitCnt = commitCnt;

            buf = ByteBuffer.allocate(maxCdcBufSize);

            Arrays.fill(buf.array(), (byte)0);

            buf.position(0);
        }

        /** {@inheritDoc} */
        @Override public boolean consume(ByteBuffer data) {
            buf.put(data);

            return commitCnt != null && commitCnt.decrementAndGet() >= 0;
        }

        /** */
        @Override public void close() {
            if (stopLatch != null)
                stopLatch.countDown();
        }
    }
}
