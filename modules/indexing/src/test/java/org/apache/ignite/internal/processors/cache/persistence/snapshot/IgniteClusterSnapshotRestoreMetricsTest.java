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

import java.io.File;
import java.io.FilenameFilter;
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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;

/**
 * Tests snapshot restore metrics.
 */
public class IgniteClusterSnapshotRestoreMetricsTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Separate working directory prefix. */
    private static final String DEDICATED_DIR_PREFIX = "dedicated-";

    /** Number of nodes using a separate working directory. */
    private static final int DEDICATED_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new JmxMetricExporterSpi());

        if (getTestIgniteInstanceIndex(igniteInstanceName) < DEDICATED_CNT) {
            cfg.setWorkDirectory(Paths.get(U.defaultWorkDirectory(),
                DEDICATED_DIR_PREFIX + U.maskForFileName(cfg.getIgniteInstanceName())).toString());
        }

        return cfg;
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

    /** {@inheritDoc} */
    @Before
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        cleanuo();
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTestSnapshot() throws Exception {
        super.afterTestSnapshot();

        cleanuo();
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshotProgress() throws Exception {
        // Caches with differebt partition distribution.
        CacheConfiguration<Integer, Object> ccfg1 = cacheConfig("cache1").setBackups(0);
        CacheConfiguration<Integer, Object> ccfg2 = cacheConfig("cache2").setCacheMode(CacheMode.REPLICATED);

        Ignite ignite = startGridsWithCache(DEDICATED_CNT, CACHE_KEYS_RANGE, key -> new Account(key, key), ccfg1, ccfg2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.destroyCaches(F.asList(ccfg1.getName(), ccfg2.getName()));
        awaitPartitionMapExchange();

        // Add new empty node.
        IgniteEx emptyNode = startGrid(DEDICATED_CNT);
        resetBaselineTopology();

        checkMetricsDefaults();

        Set<String> grpNames = new HashSet<>(F.asList(ccfg1.getName(), ccfg2.getName()));

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, grpNames);

        for (Ignite node : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(node.name(), null, SNAPSHOT_RESTORE_METRICS);
            String nodeNameMsg = "node=" + node.name();

            assertTrue(nodeNameMsg, GridTestUtils.waitForCondition(() -> getLongMetric("endTime", mReg) > 0, TIMEOUT));

            int expParts = ((IgniteEx)node).cachex(ccfg1.getName()).context().topology().localPartitions().size() +
                ((IgniteEx)node).cachex(ccfg2.getName()).context().topology().localPartitions().size();

            // Cache2 is replicated - the index partition is being copied (on snapshot data nodes).
            if (!emptyNode.name().equals(node.name()))
                expParts += 1;

            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));
            assertEquals(nodeNameMsg, "", mReg.getAttribute("error"));

            assertFalse(nodeNameMsg, ((String)mReg.getAttribute("requestId")).isEmpty());

            assertEquals(nodeNameMsg, expParts, getLongMetric("totalPartitions", mReg));
            assertEquals(nodeNameMsg, expParts, getLongMetric("processedPartitions", mReg));

            long startTime = getLongMetric("startTime", mReg);
            long endTime = getLongMetric("endTime", mReg);

            assertTrue(nodeNameMsg, startTime > 0);
            assertTrue(nodeNameMsg, endTime >= startTime);
        }

        assertSnapshotCacheKeys(ignite.cache(ccfg1.getName()));
        assertSnapshotCacheKeys(ignite.cache(ccfg2.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshotError() throws Exception {
        dfltCacheCfg.setCacheMode(CacheMode.REPLICATED);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        String failingFilePath = Paths.get(FilePageStoreManager.cacheDirName(dfltCacheCfg),
            PART_FILE_PREFIX + (dfltCacheCfg.getAffinity().partitions() / 2) + FILE_SUFFIX).toString();

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();
        String testErrMsg = "Test exception";

        ignite.context().cache().context().snapshotMgr().ioFactory((file, modes) -> {
            FileIO delegate = ioFactory.create(file, modes);

            if (file.getPath().endsWith(failingFilePath))
                throw new RuntimeException(testErrMsg);

            return delegate;
        });

        checkMetricsDefaults();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        for (Ignite node : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(node.name(), null, SNAPSHOT_RESTORE_METRICS);

            String nodeNameMsg = "node=" + node.name();

            assertTrue(nodeNameMsg, GridTestUtils.waitForCondition(() -> getLongMetric("endTime", mReg) > 0, TIMEOUT));

            long startTime = getLongMetric("startTime", mReg);
            long endTime = getLongMetric("endTime", mReg);

            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));

            assertFalse(nodeNameMsg, ((String)mReg.getAttribute("requestId")).isEmpty());

            assertTrue(nodeNameMsg, startTime > 0);
            assertTrue(nodeNameMsg, endTime >= startTime);
            assertTrue(nodeNameMsg, ((String)mReg.getAttribute("error")).contains(testErrMsg));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkMetricsDefaults() throws Exception {
        DynamicMBean mReg = metricRegistry(grid(0).name(), null, SNAPSHOT_RESTORE_METRICS);

        for (Ignite node : G.allGrids()) {
            String nodeNameMsg = "node=" + node.name();

            assertEquals(nodeNameMsg, 0, getLongMetric("endTime", mReg));
            assertEquals(nodeNameMsg, -1, getLongMetric("totalPartitions", mReg));
            assertEquals(nodeNameMsg, 0, getLongMetric("processedPartitions", mReg));
            assertTrue(nodeNameMsg, String.valueOf(mReg.getAttribute("snapshotName")).isEmpty());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void cleanuo() throws Exception {
        FilenameFilter filter = (file, name) -> file.isDirectory() && name.startsWith(DEDICATED_DIR_PREFIX);

        for (File file : new File(U.defaultWorkDirectory()).listFiles(filter))
            U.delete(file);
    }
}
