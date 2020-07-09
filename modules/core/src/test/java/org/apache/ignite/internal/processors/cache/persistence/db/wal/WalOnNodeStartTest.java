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
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/** */
public class WalOnNodeStartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegments(3)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(50 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

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

    /** */
    @Test
    public void testNoNewMetaPagesSnapshotsOnNodeStart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        // Default cache with a lot of partitions required.
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1024; k++)
            cache.put(k, k);

        // Graceful caches shutdown with the final checkpoint.
        ignite.cluster().state(ClusterState.INACTIVE);

        WALPointer lastWalPtr = ignite.context().cache().context().database().lastCheckpointMarkWalPointer();

        stopGrid(0);

        ignite = startGrid(0);

        awaitPartitionMapExchange();

        ignite.cluster().state(ClusterState.INACTIVE);

        String walPath = ignite.configuration().getDataStorageConfiguration().getWalPath();
        String walArchivePath = ignite.configuration().getDataStorageConfiguration().getWalArchivePath();

        // Stop grid so there are no ongoing wal records (BLT update and something else maybe).
        stopGrid(0);

        WALIterator replayIter = new IgniteWalIteratorFactory(log).iterator(
            (FileWALPointer)lastWalPtr.next(),
            new File(walArchivePath),
            new File(walPath)
        );

        replayIter.forEach(walPtrAndRecordPair -> {
            WALRecord walRecord = walPtrAndRecordPair.getValue();

            if (walRecord.type() == WALRecord.RecordType.PAGE_RECORD) {
                PageSnapshot pageSnapshot = (PageSnapshot)walRecord;

                ByteBuffer data = pageSnapshot.pageDataBuffer();

                // No metapages should be present in WAL because they all were in correct states already.
                assertThat(PageIO.T_PART_META, not(equalTo(PageIO.getType(data))));
            }
        });
    }
}
