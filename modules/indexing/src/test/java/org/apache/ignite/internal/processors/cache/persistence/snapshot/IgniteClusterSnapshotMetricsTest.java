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
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_TRANSFER_RATE_DMS_KEY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;

/**
 * Tests snapshot create/restore metrics.
 */
public class IgniteClusterSnapshotMetricsTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Separate working directory prefix. */
    private static final String DEDICATED_DIR_PREFIX = "dedicated-";

    /** Number of nodes using a separate working directory. */
    private static final int DEDICATED_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
            .setEncryptionEnabled(encryption)
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

        cleanup();
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTestSnapshot() throws Exception {
        super.afterTestSnapshot();

        cleanup();
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreSnapshotProgress() throws Exception {
        // Caches with different partition distribution.
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

        for (Ignite grid : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);
            String nodeNameMsg = "node=" + grid.name();

            assertTrue(nodeNameMsg, GridTestUtils.waitForCondition(() -> getNumMetric("endTime", mReg) > 0, TIMEOUT));

            int expParts = ((IgniteEx)grid).cachex(ccfg1.getName()).context().topology().localPartitions().size() +
                ((IgniteEx)grid).cachex(ccfg2.getName()).context().topology().localPartitions().size();

            // Cache2 is replicated - the index partition is being copied (on snapshot data nodes).
            if (!emptyNode.name().equals(grid.name()))
                expParts += 1;

            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));
            assertEquals(nodeNameMsg, "", mReg.getAttribute("error"));

            assertFalse(nodeNameMsg, ((String)mReg.getAttribute("requestId")).isEmpty());

            assertEquals(nodeNameMsg, expParts, getNumMetric("totalPartitions", mReg));
            assertEquals(nodeNameMsg, expParts, getNumMetric("processedPartitions", mReg));

            long startTime = getNumMetric("startTime", mReg);
            long endTime = getNumMetric("endTime", mReg);

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

        String failingFilePath = Paths.get(ignite.context().pdsFolderResolver().fileTree().cacheDirName(dfltCacheCfg),
            PART_FILE_PREFIX + primaries[0] + FILE_SUFFIX).toString();

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();
        String testErrMsg = "Test exception";

        AtomicBoolean failFlag = new AtomicBoolean();

        ignite.context().cache().context().snapshotMgr().ioFactory((file, modes) -> {
            FileIO delegate = ioFactory.create(file, modes);

            if (file.getPath().endsWith(failingFilePath)) {
                failFlag.set(true);

                throw new RuntimeException(testErrMsg);
            }

            return delegate;
        });

        checkMetricsDefaults();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        for (Ignite grid : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);

            String nodeNameMsg = "node=" + grid.name();

            assertTrue(nodeNameMsg, GridTestUtils.waitForCondition(() -> getNumMetric("endTime", mReg) > 0, TIMEOUT));

            long startTime = getNumMetric("startTime", mReg);
            long endTime = getNumMetric("endTime", mReg);

            assertEquals(nodeNameMsg, SNAPSHOT_NAME, mReg.getAttribute("snapshotName"));

            assertFalse(nodeNameMsg, ((String)mReg.getAttribute("requestId")).isEmpty());

            assertTrue(nodeNameMsg, startTime > 0);
            assertTrue(nodeNameMsg, endTime >= startTime);
            assertTrue(nodeNameMsg, ((String)mReg.getAttribute("error")).contains(testErrMsg));
        }

        assertTrue(failFlag.get());
    }

    /** @throws Exception If fails. */
    @Test
    public void testUnableToRestoreSnapshotError() throws Exception {
        Ignite ignite = startGridsWithCache(DEDICATED_CNT, CACHE_KEYS_RANGE, key -> key, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        checkMetricsDefaults();

        try {
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get();
        }
        catch (Exception ignored) {
            // No-op.
        }

        String errMsg = "Unable to restore cache group - directory is not empty. Cache group should be destroyed " +
            "manually before perform restore operation";

        for (Ignite ig : G.allGrids()) {
            DynamicMBean mReg = metricRegistry(ig.name(), null, SNAPSHOT_RESTORE_METRICS);

            assertTrue("Wrong 'endTime' metric on " + ig.name(),
                GridTestUtils.waitForCondition(() -> getNumMetric("endTime", mReg) > 0, TIMEOUT));

            assertEquals("Wrong 'totalPartitions' metric on" + ig.name(), -1,
                getNumMetric("totalPartitions", mReg));

            assertEquals("Wrong 'processedPartitions' metric on " + ig.name(), 0,
                getNumMetric("processedPartitions", mReg));

            assertEquals("Wrong 'snapshotName' metric on " + ig.name(), SNAPSHOT_NAME,
                mReg.getAttribute("snapshotName"));

            assertTrue("Wrong 'error' metric on " + ig.name(),
                mReg.getAttribute("error").toString().contains(errMsg));
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testCreateSnapshotProgress() throws Exception {
        CacheConfiguration<Integer, Object> ccfg1 = cacheConfig("cache1");

        IgniteEx ignite = startGridsWithCache(DEDICATED_CNT, CACHE_KEYS_RANGE, key -> new Account(key, key), ccfg1);

        MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_METRICS);

        LongMetric totalSize = mreg.findMetric("CurrentSnapshotTotalSize");
        LongMetric processedSize = mreg.findMetric("CurrentSnapshotProcessedSize");

        assertEquals(-1, totalSize.value());
        assertEquals(-1, processedSize.value());

        // Calculate transfer rate limit.
        long rate = FileUtils.sizeOfDirectory(ignite.context().pdsFolderResolver().fileTree().nodeStorage()) / 5;

        // Limit snapshot transfer rate.
        DistributedChangeableProperty<Serializable> rateProp =
            ignite.context().distributedConfiguration().property(SNAPSHOT_TRANSFER_RATE_DMS_KEY);

        rateProp.propagate(rate);

        // Start cluster snapshot.
        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        // Run load.
        IgniteInternalFuture<?> loadFut = GridTestUtils.runAsync(() -> {
            IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(ccfg1);

            while (!fut.isDone()) {
                Integer key = ThreadLocalRandom.current().nextInt(CACHE_KEYS_RANGE);
                Account val = new Account(ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt());

                cache.put(key, val);
            }
        });

        List<Long> totalVals = new ArrayList<>();
        List<Long> processedVals = new ArrayList<>();

        // Store metrics values during cluster snapshot.
        while (!fut.isDone()) {
            long total = totalSize.value();
            long processed = processedSize.value();

            if (total != -1 && processed != -1) {
                totalVals.add(totalSize.value());
                processedVals.add(processedSize.value());
            }

            U.sleep(500);
        }

        fut.get(getTestTimeout());

        loadFut.get();

        assertTrue("Expected distinct values: " + totalVals,
            totalVals.stream().mapToLong(v -> v).distinct().count() > 1);
        assertTrue("Expected distinct values: " + processedVals,
            processedVals.stream().mapToLong(v -> v).distinct().count() > 1);

        assertTrue("Expected sorted values: " + totalVals,
            F.isSorted(totalVals.stream().mapToLong(v -> v).toArray()));
        assertTrue("Expected sorted values: " + processedVals,
            F.isSorted(processedVals.stream().mapToLong(v -> v).toArray()));

        for (int i = 0; i < totalVals.size(); i++) {
            assertTrue("Total size less than processed [total=" + totalVals + ", processed=" + processedVals + ']',
                processedVals.get(i) <= totalVals.get(i));
        }

        assertEquals(-1, totalSize.value());
        assertEquals(-1, processedSize.value());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkMetricsDefaults() throws Exception {
        for (Ignite grid : G.allGrids()) {
            String nodeNameMsg = "node=" + grid.name();

            DynamicMBean mReg = metricRegistry(grid.name(), null, SNAPSHOT_RESTORE_METRICS);

            assertEquals(nodeNameMsg, 0, getNumMetric("endTime", mReg));
            assertEquals(nodeNameMsg, -1, getNumMetric("totalPartitions", mReg));
            assertEquals(nodeNameMsg, 0, getNumMetric("processedPartitions", mReg));
            assertTrue(nodeNameMsg, String.valueOf(mReg.getAttribute("snapshotName")).isEmpty());
        }
    }

    /**
     * @param mBean Ignite snapshot restore MBean.
     * @param name Metric name.
     * @return Metric value.
     */
    private long getNumMetric(String name, DynamicMBean mBean) {
        try {
            return ((Number)mBean.getAttribute(name)).longValue();
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void cleanup() throws Exception {
        FilenameFilter filter = (file, name) -> file.isDirectory() && name.startsWith(DEDICATED_DIR_PREFIX);

        for (File file : new File(U.defaultWorkDirectory()).listFiles(filter))
            U.delete(file);
    }
}
