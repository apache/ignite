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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.cache.WalStateManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Class for testing WAL manager.
 */
public class WriteAheadLogManagerSelfTest extends GridCommonAbstractTest {
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrids(int cnt) throws Exception {
        IgniteEx n = super.startGrids(cnt);

        n.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        return n;
    }

    /**
     * Checking the correctness of WAL segment reservation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReservation() throws Exception {
        IgniteEx n = startGrids(1);

        for (int i = 0; walMgr(n).lastArchivedSegment() < 2; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(10 * U.KB)]);

        forceCheckpoint();

        assertTrue(walMgr(n).lastArchivedSegment() >= 2);
        assertTrue(walMgr(n).lastTruncatedSegment() == -1);

        WALPointer segment0WalPtr = new WALPointer(0, 0, 0);
        assertTrue(walMgr(n).reserve(segment0WalPtr));
        assertTrue(walMgr(n).reserved(segment0WalPtr));

        WALPointer segment1WalPtr = new WALPointer(1, 0, 0);

        // Delete segment manually.
        FileDescriptor segment1 = Arrays.stream(walMgr(n).walArchiveFiles())
            .filter(fd -> fd.idx() == segment1WalPtr.index()).findAny().orElseThrow(AssertionError::new);

        assertTrue(segment1.file().delete());

        assertFalse(walMgr(n).reserve(segment1WalPtr));
        assertTrue(walMgr(n).reserved(segment1WalPtr));

        walMgr(n).release(segment0WalPtr);
        assertFalse(walMgr(n).reserved(segment0WalPtr));
        assertFalse(walMgr(n).reserved(segment1WalPtr));

        assertEquals(1, walMgr(n).truncate(segment1WalPtr));
        assertFalse(walMgr(n).reserve(segment0WalPtr));
        assertFalse(walMgr(n).reserve(segment1WalPtr));
        assertFalse(walMgr(n).reserved(segment0WalPtr));
        assertFalse(walMgr(n).reserved(segment1WalPtr));

        WALPointer segmentMaxWalPtr = new WALPointer(Long.MAX_VALUE, 0, 0);
        assertFalse(walMgr(n).reserve(segmentMaxWalPtr));
        assertFalse(walMgr(n).reserved(segmentMaxWalPtr));
    }

    /**
     * Checking the correctness of the method {@link FileWriteAheadLogManager#getWalFilesFromArchive}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetWalFilesFromArchive() throws Exception {
        IgniteEx n = startGrids(1);

        WALPointer segment0WalPtr = new WALPointer(0, 0, 0);
        WALPointer segment1WalPtr = new WALPointer(1, 0, 0);
        WALPointer segment2WalPtr = new WALPointer(2, 0, 0);

        CountDownLatch startLatch = new CountDownLatch(1);
        IgniteInternalFuture<Collection<File>> fut = runAsync(() -> {
            startLatch.countDown();

            return walMgr(n).getWalFilesFromArchive(segment0WalPtr, segment2WalPtr);
        });

        startLatch.await();

        // Check that the expected archiving segment 1.
        assertThrows(log, () -> fut.get(1_000), IgniteCheckedException.class, null);

        for (int i = 0; walMgr(n).lastArchivedSegment() < 2; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(10 * U.KB)]);

        assertEquals(2, fut.get(getTestTimeout()).size());

        forceCheckpoint();

        assertEquals(1, walMgr(n).truncate(segment1WalPtr));
        assertEquals(0, walMgr(n).getWalFilesFromArchive(segment0WalPtr, segment2WalPtr).size());
        assertEquals(1, walMgr(n).getWalFilesFromArchive(segment1WalPtr, segment2WalPtr).size());
    }

    /**
     * Check that auto archive will execute without {@link NullPointerException}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAutoArchiveWithoutNullPointerException() throws Exception {
        setLoggerDebugLevel();

        LogListener logLsnr0 = LogListener.matches("Checking if WAL rollover required").build();

        LogListener logLsnr1 = LogListener.matches(
            Pattern.compile("Rollover segment \\[\\d+ to \\d+\\], recordType=null")).build();

        IgniteEx n = startGrid(0, cfg -> {
            cfg.setGridLogger(new ListeningTestLogger(log, logLsnr0, logLsnr1))
                .getDataStorageConfiguration().setWalAutoArchiveAfterInactivity(100_000);
        });

        n.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        GridTimeoutObject timeoutObj = timeoutRollover(n);
        assertNotNull(timeoutObj);

        n.cache(DEFAULT_CACHE_NAME).put(current().nextInt(), new byte[16]);

        disableWal(n);

        lastRecordLoggedMs(n).set(1);

        timeoutObj.onTimeout();

        assertTrue(logLsnr0.check());
        assertTrue(logLsnr1.check());
    }

    /**
     * Checking the absence of a race between the deactivation of the VAL and
     * automatic archiving, which may lead to a fail of the node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoRaceAutoArchiveAndDeactivation() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (log.isInfoEnabled())
                log.info(">>> Test iteration:" + i);

            IgniteEx n = startGrid(0, cfg -> {
                cfg.getDataStorageConfiguration().setWalAutoArchiveAfterInactivity(10);
            });

            n.cluster().state(ACTIVE);
            awaitPartitionMapExchange();

            GridTimeoutObject timeoutObj = timeoutRollover(n);
            assertNotNull(timeoutObj);

            n.cache(DEFAULT_CACHE_NAME).put(current().nextInt(), new byte[16]);

            CountDownLatch l = new CountDownLatch(1);
            AtomicBoolean stop = new AtomicBoolean();

            IgniteInternalFuture<Object> fut = runAsync(() -> {
                l.countDown();

                while (!stop.get())
                    lastRecordLoggedMs(n).set(1);
            });

            assertTrue(l.await(getTestTimeout(), MILLISECONDS));

            walMgr(n).onDeActivate(n.context());
            stop.set(true);

            fut.get(getTestTimeout());

            assertEquals(1, G.allGrids().size());

            stopAllGrids();
            cleanPersistenceDir();
        }
    }

    /**
     * Getting {@code FileWriteAheadLogManager#lastRecordLoggedMs}
     *
     * @param n Node.
     * @return Container with last WAL record logged timestamp.
     */
    private AtomicLong lastRecordLoggedMs(IgniteEx n) {
        return getFieldValue(walMgr(n), "lastRecordLoggedMs");
    }

    /**
     * Disable WAL.
     *
     * @param n Node.
     */
    private void disableWal(IgniteEx n) throws Exception {
        WALDisableContext walDisableCtx = n.context().cache().context().walState().walDisableContext();
        assertNotNull(walDisableCtx);

        setFieldValue(walDisableCtx, "disableWal", true);

        assertTrue(walDisableCtx.check());
        assertNull(walMgr(n).log(new DataRecord(emptyList(), false)));
    }

    /**
     * Getting {@code FileWriteAheadLogManager#timeoutRollover};
     *
     * @param n Node.
     * @return Timeout object.
     */
    @Nullable private GridTimeoutObject timeoutRollover(IgniteEx n) {
        synchronized (getFieldValue(walMgr(n), "timeoutRolloverMux")) {
            return getFieldValue(walMgr(n), "timeoutRollover");
        }
    }
}
