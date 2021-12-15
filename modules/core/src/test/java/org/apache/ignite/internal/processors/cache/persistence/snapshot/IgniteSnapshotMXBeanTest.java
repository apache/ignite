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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests {@link SnapshotMXBean}.
 */
public class IgniteSnapshotMXBeanTest extends AbstractSnapshotSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new JmxMetricExporterSpi());
    }

    /** @throws Exception If fails. */
    @Test
    public void testCreateSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        DynamicMBean snpMBean = metricRegistry(ignite.name(), null, SNAPSHOT_METRICS);

        assertEquals("Snapshot end time must be undefined on first snapshot operation starts.",
            0, getLastSnapshotEndTime(snpMBean));

        SnapshotMXBean mxBean = getMxBean(ignite.name(), "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);

        mxBean.createSnapshot(SNAPSHOT_NAME);

        assertTrue("Waiting for snapshot operation failed.",
            GridTestUtils.waitForCondition(() -> getLastSnapshotEndTime(snpMBean) > 0, TIMEOUT));

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

        SnapshotMXBean mxBean = getMxBean(killCli.name(), "Snapshot", SnapshotMXBeanImpl.class,
            SnapshotMXBean.class);

        doSnapshotCancellationTest(startCli,
            Collections.singletonList(srv),
            srv.cache(dfltCacheCfg.getName()),
            mxBean::cancelSnapshot);
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshot() throws Exception {
        CacheConfiguration<Integer, Object> ccfg1 = new CacheConfiguration<>(dfltCacheCfg).setBackups(1).setName("cache1");
        CacheConfiguration<Integer, Object> ccfg2 = new CacheConfiguration<>(ccfg1).setName("cache2");

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, Integer::new, ccfg1, ccfg2);

        DynamicMBean snpMBean = metricRegistry(ignite.name(), null, SNAPSHOT_METRICS);

        assertEquals("Snapshot end time must be undefined on first snapshot operation starts.",
            0, getLastSnapshotEndTime(snpMBean));

        SnapshotMXBean mxBean = getMxBean(ignite.name(), "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);

        mxBean.createSnapshot(SNAPSHOT_NAME);

        assertTrue(GridTestUtils.waitForCondition(() -> getLastSnapshotEndTime(snpMBean) > 0, TIMEOUT));

        ignite.destroyCaches(Arrays.asList(ccfg1.getName(), ccfg2.getName()));

        awaitPartitionMapExchange();

        DynamicMBean restoreMBean = metricRegistry(ignite.name(), null, SNAPSHOT_RESTORE_METRICS);

        assertEquals(0, getLongMetric("endTime", restoreMBean));
        assertEquals(-1, getLongMetric("totalPartitions", restoreMBean));
        assertEquals(0, getLongMetric("processedPartitions", restoreMBean));
        assertTrue(String.valueOf(restoreMBean.getAttribute("snapshotName")).isEmpty());

        Set<String> grpNames = new HashSet<>(F.asList(ccfg1.getName(), ccfg2.getName()));

        mxBean.restoreSnapshot(SNAPSHOT_NAME, F.concat(grpNames, " ,"));

        assertTrue(GridTestUtils.waitForCondition(() -> getLongMetric("endTime", restoreMBean) > 0, TIMEOUT));

        int expPartCnt = grid(0).cachex(ccfg1.getName()).context().affinity().partitions() +
            grid(0).cachex(ccfg2.getName()).context().affinity().partitions();

        for (Ignite grid : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);

            assertEquals(expPartCnt, getLongMetric("totalPartitions", mReg));
            assertEquals(expPartCnt, getLongMetric("processedPartitions", mReg));

            assertEquals(SNAPSHOT_NAME, restoreMBean.getAttribute("snapshotName"));
        }

        assertSnapshotCacheKeys(ignite.cache(ccfg1.getName()));
        assertSnapshotCacheKeys(ignite.cache(ccfg2.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testCancelRestoreSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        DynamicMBean snpMBean = metricRegistry(ignite.name(), null, SNAPSHOT_METRICS);

        assertEquals("Snapshot end time must be undefined on first snapshot operation starts.",
            0, getLastSnapshotEndTime(snpMBean));

        SnapshotMXBean mxBean = getMxBean(ignite.name(), "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);

        mxBean.createSnapshot(SNAPSHOT_NAME);

        assertTrue(GridTestUtils.waitForCondition(() -> getLastSnapshotEndTime(snpMBean) > 0, TIMEOUT));

        ignite.destroyCache(dfltCacheCfg.getName());

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage &&
            ((SingleNodeMessage<?>)msg).type() == RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE.ordinal());

        IgniteFuture<Void> fut = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        spi.waitForBlocked();

        IgniteInternalFuture<Boolean> interruptFut = GridTestUtils.runAsync(() -> {
            // Wait for the process to be interrupted.
            AtomicReference<?> errRef = U.field((Object)U.field((Object)U.field(
                grid(0).context().cache().context().snapshotMgr(), "restoreCacheGrpProc"), "opCtx"), "err");

            boolean interrupted = waitForCondition(() -> errRef.get() != null, getTestTimeout());

            spi.stopBlock();

            return interrupted;
        });

        mxBean.cancelSnapshotRestore(SNAPSHOT_NAME);

        assertTrue(interruptFut.get());

        assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, "Operation has been canceled by the user");
    }

    /**
     * @param mBean Ignite snapshot MBean.
     * @return Value of snapshot end time.
     */
    private static long getLastSnapshotEndTime(DynamicMBean mBean) {
        try {
            return (long)mBean.getAttribute("LastSnapshotEndTime");
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param mBean Ignite snapshot restore MBean.
     * @param name Metric name.
     * @return Metric value.
     */
    private static long getLongMetric(String name, DynamicMBean mBean) {
        try {
            return (long)mBean.getAttribute(name);
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
