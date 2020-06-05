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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS;
import static org.apache.ignite.internal.util.IgniteUtils.LOCK_HOLD_MESSAGE;

/**
 * Tests critical failure handling on checkpoint read lock acquisition errors.
 */
public class CheckpointReadLockFailureTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger testLog;

    /** */
    private static final AbstractFailureHandler FAILURE_HND = new AbstractFailureHandler() {
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            if (failureCtx.type() != FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT)
                return true;

            if (hndLatch != null)
                hndLatch.countDown();

            return false;
        }
    };

    /** */
    private static volatile CountDownLatch hndLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setFailureHandler(FAILURE_HND)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(Integer.MAX_VALUE)
                .setCheckpointReadLockTimeout(1));

        if (testLog != null) {
            cfg.setGridLogger(testLog);

            testLog = null;
        }

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeClass() {
        Set<FailureType> ignoredFailureTypes = new HashSet<>(FAILURE_HND.getIgnoredFailureTypes());
        ignoredFailureTypes.remove(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT);

        FAILURE_HND.setIgnoredFailureTypes(ignoredFailureTypes);
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailureTypeOnTimeout() throws Exception {
        hndLatch = new CountDownLatch(1);

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        IgniteInternalFuture acquireWriteLock = GridTestUtils.runAsync(() -> {
            db.checkpointLock.writeLock().lock();

            try {
                doSleep(Long.MAX_VALUE);
            }
            finally {
                db.checkpointLock.writeLock().unlock();
            }
        });

        IgniteInternalFuture acquireReadLock = GridTestUtils.runAsync(() -> {
            db.checkpointReadLock();
            db.checkpointReadUnlock();
        });

        assertTrue(hndLatch.await(5, TimeUnit.SECONDS));

        acquireWriteLock.cancel();

        acquireReadLock.get(5, TimeUnit.SECONDS);

        GridTestUtils.waitForCondition(acquireWriteLock::isCancelled, 5000);

        stopGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS, value = "true")
    public void testPrintCpRLockHolder() throws Exception {
        CountDownLatch canRelease = new CountDownLatch(1);

        testLog = new ListeningTestLogger(false, log);

        LogListener lsnr = LogListener.matches(LOCK_HOLD_MESSAGE).build();

        testLog.registerListener(lsnr);

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        U.ReentrantReadWriteLockTracer tracker = (U.ReentrantReadWriteLockTracer)db.checkpointLock;

        GridTestUtils.runAsync(() -> {
            db.checkpointLock.readLock().lock();

            try {
                canRelease.await(tracker.lockWaitThreshold() + 500, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                db.checkpointLock.readLock().unlock();
            }
        }, "async-runnable-runner-1");

        assertTrue(GridTestUtils.waitForCondition(lsnr::check, tracker.lockWaitThreshold() + 1000));

        stopGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS, value = "true")
    public void testReentrance() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        ReentrantReadWriteLock rwLock = db.checkpointLock;

        CountDownLatch waitFirstRLock = new CountDownLatch(1);

        CountDownLatch waitSecondRLock = new CountDownLatch(1);

        long timeout = 500L;

        IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
            //noinspection LockAcquiredButNotSafelyReleased
            rwLock.readLock().lock();

            //noinspection LockAcquiredButNotSafelyReleased
            rwLock.readLock().lock();

            rwLock.readLock().unlock();

            waitFirstRLock.countDown();

            try {
                waitSecondRLock.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            rwLock.readLock().unlock();
        }, "async-runnable-runner-1");

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
            try {
                waitFirstRLock.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                rwLock.writeLock().tryLock();

                assertFalse(GridTestUtils.waitForCondition(rwLock.writeLock()::isHeldByCurrentThread, timeout));
            }
            catch (IgniteInterruptedCheckedException e) {
                e.printStackTrace();
            }

            waitSecondRLock.countDown();

            try {
                rwLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS);

                assertTrue(rwLock.writeLock().isHeldByCurrentThread());
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                rwLock.writeLock().unlock();
            }

        }, "async-runnable-runner-2");

        f1.get(4 * timeout);

        f0.get(4 * timeout);

        stopGrid(0);
    }
}
