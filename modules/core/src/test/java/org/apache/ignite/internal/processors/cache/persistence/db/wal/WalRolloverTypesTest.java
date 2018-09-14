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

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.CURRENT_SEGMENT;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.NEXT_SEGMENT;

/**
 *
 */
public class WalRolloverTypesTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private WALMode walMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(20 * 1024 * 1024))
            .setWalMode(walMode)
            .setWalSegmentSize(4 * 1024 * 1024));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testCurrentSegmentTypeLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkCurrentSegmentType();
    }

    /** */
    public void testCurrentSegmentTypeLogFsyncMode() throws Exception {
        walMode = FSYNC;

        checkCurrentSegmentType();
    }

    /** */
    public void testNextSegmentTypeLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkNextSegmentType();
    }

    /** */
    public void testNextSegmentTypeLogFsyncMode() throws Exception {
        walMode = FSYNC;

        checkNextSegmentType();
    }

    /** */
    private void checkCurrentSegmentType() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = ig.context().cache().context().wal().log(new CheckpointRecord(null), CURRENT_SEGMENT);

            assertEquals(0, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    private void checkNextSegmentType() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = ig.context().cache().context().wal().log(new CheckpointRecord(null), NEXT_SEGMENT);

            assertEquals(1, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }
}
