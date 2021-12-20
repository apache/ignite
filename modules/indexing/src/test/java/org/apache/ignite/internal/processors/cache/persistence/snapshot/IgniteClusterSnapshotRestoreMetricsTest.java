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

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import javax.management.DynamicMBean;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;

/**
 * Tests snapshot restore metrics.
 */
public class IgniteClusterSnapshotRestoreMetricsTest extends AbstractSnapshotSelfTest {
    /** TODO Remove this method after IGNITE-14999 to enable encryption. */
    @Parameterized.Parameters(name = "Encryption is disabled")
    public static Iterable<Boolean> disabledEncryption() {
        return Collections.singletonList(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new JmxMetricExporterSpi());
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Object> cacheConfig(String name) {
        return new CacheConfiguration<>(dfltCacheCfg)
            .setName(name)
            .setSqlSchema(name)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), Account.class.getName())
                .setFields(new LinkedHashMap<>(F.asMap(
                    "id", Integer.class.getName(),
                    "balance", Integer.class.getName()))
                )
                .setIndexes(Collections.singletonList(new QueryIndex("id"))))
            );
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshot() throws Exception {
        // Caches with differebt partition distribution.
        CacheConfiguration<Integer, Object> ccfg1 = cacheConfig("cache1").setBackups(0);
        CacheConfiguration<Integer, Object> ccfg2 = cacheConfig("cache2").setCacheMode(CacheMode.REPLICATED);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, key -> new Account(key, key), ccfg1, ccfg2);

        DynamicMBean snpMBean = metricRegistry(ignite.name(), null, SNAPSHOT_METRICS);

        assertEquals("Snapshot end time must be undefined on start.", 0, getLastSnapshotEndTime(snpMBean));

        SnapshotMXBean mxBean = getMxBean(ignite.name(), "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);

        mxBean.createSnapshot(SNAPSHOT_NAME);

        assertTrue(GridTestUtils.waitForCondition(() -> getLastSnapshotEndTime(snpMBean) > 0, TIMEOUT));

        ignite.destroyCaches(F.asList(ccfg1.getName(), ccfg2.getName()));
        awaitPartitionMapExchange();

        // Add new empty node.
        startGrid(3);
        resetBaselineTopology();

        DynamicMBean restoreMBean = metricRegistry(ignite.name(), null, SNAPSHOT_RESTORE_METRICS);

        assertEquals(0, getLongMetric("endTime", restoreMBean));
        assertEquals(-1, getLongMetric("totalPartitions", restoreMBean));
        assertEquals(0, getLongMetric("processedPartitions", restoreMBean));
        assertTrue(String.valueOf(restoreMBean.getAttribute("snapshotName")).isEmpty());

        Set<String> grpNames = new HashSet<>(F.asList(ccfg1.getName(), ccfg2.getName()));

        mxBean.restoreSnapshot(SNAPSHOT_NAME, F.concat(grpNames, " ,"));

        for (Ignite grid : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);

            assertTrue(GridTestUtils.waitForCondition(() -> getLongMetric("endTime", mReg) > 0, TIMEOUT));

            IgniteInternalCache<?, ?> cache1 = ((IgniteEx)grid).cachex(ccfg1.getName());
            IgniteInternalCache<?, ?> cache2 = ((IgniteEx)grid).cachex(ccfg2.getName());

            int cache1Parts = cache1.context().topology().localPartitions().size();

            // Cache2 is replicated - the index partition is being copied.
            int cache2Parts = cache2.context().topology().localPartitions().size() + 1;

            String nodeNameMsg = "node=" + grid.name();

            assertEquals(nodeNameMsg, cache1Parts + cache2Parts, getLongMetric("totalPartitions", mReg));
            assertEquals(nodeNameMsg, cache1Parts + cache2Parts, getLongMetric("processedPartitions", mReg));

            long startTime = getLongMetric("startTime", mReg);
            long endTime = getLongMetric("endTime", mReg);

            assertTrue(nodeNameMsg, startTime > 0);
            assertTrue(nodeNameMsg, endTime > 0);
            assertTrue(nodeNameMsg, endTime >= startTime);

            assertFalse(nodeNameMsg, ((String)mReg.getAttribute("requestId")).isEmpty());

            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));
            assertEquals(nodeNameMsg, "", mReg.getAttribute("error"));
        }

        assertSnapshotCacheKeys(ignite.cache(ccfg1.getName()));
        assertSnapshotCacheKeys(ignite.cache(ccfg2.getName()));

        // Clear everything and check the error metric.
        ignite.destroyCaches(F.asList(ccfg1.getName(), ccfg2.getName()));
        awaitPartitionMapExchange();

        String failingFilePath = Paths.get(CACHE_DIR_PREFIX + "cache2",
            PART_FILE_PREFIX + (dfltCacheCfg.getAffinity().partitions() / 2) + FILE_SUFFIX).toString();

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();
        String testErrMsg = "Test exception";

        grid(0).context().cache().context().snapshotMgr().ioFactory((file, modes) -> {
            FileIO delegate = ioFactory.create(file, modes);

            if (file.getPath().endsWith(failingFilePath))
                throw new RuntimeException(testErrMsg);

            return delegate;
        });

        long procStartTime = U.currentTimeMillis();

        mxBean.restoreSnapshot(SNAPSHOT_NAME, null);

        for (Ignite grid : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);

            assertTrue(GridTestUtils.waitForCondition(() -> getLongMetric("endTime", mReg) >= procStartTime, TIMEOUT));

            String nodeNameMsg = "node=" + grid.name();

            long startTime = getLongMetric("startTime", mReg);
            long endTime = getLongMetric("endTime", mReg);

            assertTrue(nodeNameMsg, startTime >= procStartTime);
            assertTrue(nodeNameMsg, endTime >= startTime);
            assertTrue(nodeNameMsg, ((String)mReg.getAttribute("error")).contains(testErrMsg));
            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));
        }
    }
}
