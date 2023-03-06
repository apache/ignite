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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeFalse;

/**
 * Tests {@link SnapshotMXBean}.
 */
public class IgniteSnapshotMXBeanTest extends AbstractSnapshotSelfTest {
    /** Snapshot group name. */
    private static final String SNAPSHOT_GROUP = "Snapshot";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** @throws Exception If fails. */
    @Test
    public void testCreateSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        DynamicMBean snpMBean = metricRegistry(ignite.name(), null, SNAPSHOT_METRICS);

        assertEquals("Snapshot end time must be undefined on first snapshot operation starts.",
            0, (long)getMetric("LastSnapshotEndTime", snpMBean));

        SnapshotMXBean mxBean = getMxBean(ignite.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class, SnapshotMXBean.class);

        mxBean.createSnapshot(SNAPSHOT_NAME, "");

        assertTrue("Waiting for snapshot operation failed.",
            GridTestUtils.waitForCondition(() -> (long)getMetric("LastSnapshotEndTime", snpMBean) > 0, TIMEOUT));

        if (!encryption) {
            mxBean.createIncrementalSnapshot(SNAPSHOT_NAME, "");

            assertTrue(
                "Waiting for incremental snapshot failed",
                GridTestUtils.waitForCondition(() -> checkIncremental(ignite, SNAPSHOT_NAME, null, 1), TIMEOUT)
            );
        }

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testCancelSnapshot() throws Exception {
        IgniteEx srv = startGridsWithCache(1, dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx startCli = startClientGrid(1);
        IgniteEx killCli = startClientGrid(2);

        SnapshotMXBean mxBean = getMxBean(killCli.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class,
            SnapshotMXBean.class);

        doSnapshotCancellationTest(startCli,
            Collections.singletonList(srv),
            srv.cache(dfltCacheCfg.getName()),
            mxBean::cancelSnapshot);
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE, false);

        DynamicMBean mReg0 = metricRegistry(grid(0).name(), null, SNAPSHOT_RESTORE_METRICS);
        DynamicMBean mReg1 = metricRegistry(grid(1).name(), null, SNAPSHOT_RESTORE_METRICS);

        assertEquals(0, (long)getMetric("endTime", mReg0));
        assertEquals(0, (long)getMetric("endTime", mReg1));

        getMxBean(ignite.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class, SnapshotMXBean.class)
            .restoreSnapshot(SNAPSHOT_NAME, "", "");

        assertTrue(GridTestUtils.waitForCondition(() -> (long)getMetric("endTime", mReg0) > 0, TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(() -> (long)getMetric("endTime", mReg1) > 0, TIMEOUT));

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreIncrementalSnapshot() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        try (IgniteDataStreamer<Integer, Object> ds = ignite.dataStreamer(dfltCacheCfg.getName())) {
            for (int i = CACHE_KEYS_RANGE; i < 2 * CACHE_KEYS_RANGE; i++)
                ds.addData(i, valueBuilder().apply(i));
        }

        ignite.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        DynamicMBean mReg0 = metricRegistry(grid(0).name(), null, SNAPSHOT_RESTORE_METRICS);
        DynamicMBean mReg1 = metricRegistry(grid(1).name(), null, SNAPSHOT_RESTORE_METRICS);

        assertEquals(0, (long)getMetric("endTime", mReg0));
        assertEquals(0, (long)getMetric("endTime", mReg1));

        getMxBean(ignite.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class, SnapshotMXBean.class)
            .restoreSnapshot(SNAPSHOT_NAME, "", "", 1);

        assertTrue(GridTestUtils.waitForCondition(() -> (long)getMetric("endTime", mReg0) > 0, getTestTimeout()));
        assertTrue(GridTestUtils.waitForCondition(() -> (long)getMetric("endTime", mReg1) > 0, getTestTimeout()));

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), 2 * CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testCancelRestoreSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE, false);
        SnapshotMXBean mxBean = getMxBean(ignite.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class, SnapshotMXBean.class);
        DynamicMBean mReg0 = metricRegistry(grid(0).name(), null, SNAPSHOT_RESTORE_METRICS);
        DynamicMBean mReg1 = metricRegistry(grid(1).name(), null, SNAPSHOT_RESTORE_METRICS);

        assertEquals("", getMetric("error", mReg0));
        assertEquals("", getMetric("error", mReg1));
        assertEquals(0, (long)getMetric("endTime", mReg0));
        assertEquals(0, (long)getMetric("endTime", mReg1));

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage &&
            ((SingleNodeMessage<?>)msg).type() == RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE.ordinal());

        IgniteFuture<Void> fut = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        spi.waitForBlocked();

        IgniteInternalFuture<Boolean> interruptFut = GridTestUtils.runAsync(() -> {
            try {
                return GridTestUtils.waitForCondition(
                    () -> !"".equals(getMetric("error", mReg0)) && !"".equals(getMetric("error", mReg1)), TIMEOUT);
            }
            finally {
                spi.stopBlock();
            }
        });

        mxBean.cancelSnapshotRestore(SNAPSHOT_NAME);

        assertTrue(interruptFut.get());

        String expErrMsg = "Operation has been canceled by the user.";

        assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteCheckedException.class, expErrMsg);

        assertTrue((long)getMetric("endTime", mReg0) > 0);
        assertTrue(((String)getMetric("error", mReg0)).contains(expErrMsg));

        // We use {@code waitForCondition} here and below since {@code fut} completeness guarantees that snapshot
        // procedure is completed only on the initiator node. Remote nodes can not handle snapshot cancellation event yet.
        assertTrue(waitForCondition(() -> (long)getMetric("endTime", mReg1) > 0, getTestTimeout()));
        assertTrue(waitForCondition(() -> ((String)getMetric("error", mReg1)).contains(expErrMsg), getTestTimeout()));

        assertNull(ignite.cache(DEFAULT_CACHE_NAME));
    }

    /** @throws Exception If fails. */
    @Test
    public void testStatus() throws Exception {
        IgniteEx srv = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        checkSnapshotStatus(false, false, false, null);

        // Create full snapshot.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = srv.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitForBlocked();

        checkSnapshotStatus(true, false, false, SNAPSHOT_NAME);

        spi.stopBlock();

        fut.get(getTestTimeout());

        checkSnapshotStatus(false, false, false, null);

        // Create incremental snapshot.
        // TODO: remove condition after resolving IGNITE-17819.
        if (!encryption) {
            spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

            fut = srv.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME);

            spi.waitForBlocked();

            checkSnapshotStatus(true, false, true, SNAPSHOT_NAME);

            spi.stopBlock();

            fut.get(getTestTimeout());

            checkSnapshotStatus(false, false, false, null);
        }

        // Restore full snapshot.
        srv.destroyCache(DEFAULT_CACHE_NAME);

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        fut = srv.snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList(DEFAULT_CACHE_NAME));

        spi.waitForBlocked();

        checkSnapshotStatus(false, true, false, SNAPSHOT_NAME);

        spi.stopBlock();

        fut.get(getTestTimeout());

        checkSnapshotStatus(false, false, false, null);

        // Restore incremental snapshot.
        // TODO: remove condition after resolving IGNITE-17819.
        if (!encryption) {
            srv.destroyCache(DEFAULT_CACHE_NAME);

            spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

            fut = srv.snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList(DEFAULT_CACHE_NAME), 1);

            spi.waitForBlocked();

            checkSnapshotStatus(false, true, true, SNAPSHOT_NAME);

            spi.stopBlock();

            fut.get(getTestTimeout());

            checkSnapshotStatus(false, false, false, null);
        }
    }

    /**
     * @param isCreating {@code True} if create snapshot operation is in progress.
     * @param isRestoring {@code True} if restore snapshot operation is in progress.
     * @param isIncremental {@code True} if incremental snapshot operation.
     * @param expName Expected snapshot name.
     */
    private void checkSnapshotStatus(boolean isCreating, boolean isRestoring, boolean isIncremental, String expName) throws Exception {
        assertTrue(waitForCondition(() -> G.allGrids().stream().allMatch(
                ignite -> {
                    IgniteSnapshotManager mgr = ((IgniteEx)ignite).context().cache().context().snapshotMgr();

                    return isCreating == mgr.isSnapshotCreating() && isRestoring == mgr.isRestoring();
                }),
            getTestTimeout()));

        for (Ignite grid : G.allGrids()) {
            SnapshotMXBean bean = getMxBean(grid.name(), SNAPSHOT_GROUP, SnapshotMXBeanImpl.class, SnapshotMXBean.class);

            String status = bean.status();

            if (!isCreating && !isRestoring) {
                assertContains(log, status, "There is no create or restore snapshot operation in progress");

                continue;
            }

            if (isCreating)
                assertContains(log, status, "Create snapshot operation is in progress");
            else
                assertContains(log, status, "Restore snapshot operation is in progress");

            assertContains(log, status, "name=" + expName);
            assertContains(log, status, "incremental=" + isIncremental);

            if (isIncremental)
                assertContains(log, status, "incrementalIndex=1");
        }
    }

    /**
     * @param mBean Ignite snapshot restore MBean.
     * @param name Metric name.
     * @return Metric value.
     */
    private static <T> T getMetric(String name, DynamicMBean mBean) {
        try {
            return (T)mBean.getAttribute(name);
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
