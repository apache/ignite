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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;

/**
 * Test SKIP_GARBAGE compression mode for WAL page snapshot records without extra dependencies.
 */
public class WalPageRecordCompactionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteName) throws Exception {
        return super.getConfiguration(igniteName).setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setMetricsEnabled(true)
            .setWalPageCompression(DiskPageCompression.SKIP_GARBAGE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalPageCompaction() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        int pageSize = ignite.configuration().getDataStorageConfiguration().getPageSize();

        MetricRegistry registry = ignite.context().metric().registry(DATASTORAGE_METRIC_PREFIX);

        LongMetric loggingRate = registry.findMetric("WalLoggingRate");
        LongMetric walSize = registry.findMetric("WalWrittenBytes");

        ByteBuffer pageBuf = ByteBuffer.allocateDirect(pageSize);
        long pageAddr = GridUnsafe.bufferAddress(pageBuf);
        DataPageIO.VERSIONS.latest().initNewPage(pageAddr, -1, pageSize, null);
        PageSnapshot pageSnapshot = new PageSnapshot(new FullPageId(-1, -1), pageAddr, pageSize, pageSize);

        long prevCnt;
        long size;

        do {
            prevCnt = loggingRate.value();
            long prevSize = walSize.value();

            ignite.context().cache().context().wal().log(pageSnapshot);

            size = walSize.value() - prevSize;
        } while (loggingRate.value() - prevCnt > 1); // Ensure that no more than one record logged.

        // Check that record is compacted.
        assertTrue("Unexpected WAL record size: " + size, size > 0 && size < pageSize);
    }
}
