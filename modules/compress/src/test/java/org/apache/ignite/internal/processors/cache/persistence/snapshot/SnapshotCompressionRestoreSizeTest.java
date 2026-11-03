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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.SNAPSHOT_NAME;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** Checks the disk space of a compressed cache group restored from a snapshot. */
public class SnapshotCompressionRestoreSizeTest extends GridCommonAbstractTest {
    /**
     * Partition files must be well above 64 KiB: the end-of-file preallocation that can make the copy dense (XFS does it)
     * starts at that size.
     */
    private static final int KEYS = 4_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(new DataStorageConfiguration()
            .setPageSize(8 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRestoredPartitionsAreSparse() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = ignite.createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 4))
            .setDiskPageCompression(DiskPageCompression.SNAPPY));

        for (int i = 0; i < KEYS; i++)
            cache.put(i, new byte[1024]);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        cache.destroy();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get();

        MetricRegistry mreg = ignite.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX, DEFAULT_CACHE_NAME));

        long size = mreg.<LongMetric>findMetric("StorageSize").value();
        long sparseSize = mreg.<LongMetric>findMetric("SparseStorageSize").value();

        // Zero-filled values compress every 8 KiB page into one 4 KiB block, so a sparse group takes about half of its size.
        assertTrue("sparseSize=" + sparseSize + ", size=" + size, sparseSize < 0.75 * size);
    }
}
