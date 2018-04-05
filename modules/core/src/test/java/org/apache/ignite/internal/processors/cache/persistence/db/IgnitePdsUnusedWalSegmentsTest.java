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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskArg;
import org.apache.ignite.internal.visor.misc.VisorWalTaskOperation;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Test correctness of WAL Visor Task and correctness of delete.
 */
public class IgnitePdsUnusedWalSegmentsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        System.setProperty(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, "2");

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setWalSegmentSize(1024 * 1024)
                .setWalHistorySize(Integer.MAX_VALUE)
                .setWalSegments(10)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setMaxSize(100 * 1024 * 1024)
                        .setPersistenceEnabled(true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * Tests correctness of {@link VisorWalTaskOperation}.
     *
     * @throws Exception if failed.
     */
    public void testCorrectnessOfDeletionTaskSegments() throws Exception {
        try {
            IgniteEx ig0 = (IgniteEx) startGrids(4);

            ig0.cluster().active(true);

            IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

            for (int k = 0; k < 10_000; k++)
                cache.put(k, new byte[1024]);

            forceCheckpoint();

            for (int k = 0; k < 1_000; k++)
                cache.put(k, new byte[1024]);

            forceCheckpoint();

            VisorWalTaskResult printRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that print task finished without exceptions", printRes.results().size(), 4);

            List<File> walArchives = new ArrayList<>();

            printRes.results().entrySet().forEach(e -> {
                log.warning("Node " + e.getKey());
                log.warning("Num of segments " + e.getValue().size());
            });

            for(int i = 0; i < 4; i++){
                IgniteEx ig = grid(i);
                log.warning("node:" + ig.cluster().node().consistentId().toString());
                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) ig.context().cache().context()
                        .database();
                log.warning("min reserved index: " + dbMgr.reservedWalSegmentIndex());

                File walArchiveDir = GridTestUtils.getFieldValue(ig.context().cache().context().wal(),"walArchiveDir");

                File[] wals = walArchiveDir.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);



                if (wals != null) {
                    for (File wal: wals)
                        log.warning(wal.getAbsolutePath());

                }

            }

            for (Collection<String> pathsPerNode : printRes.results().values()) {
                assertTrue("Check that all nodes has unused WAL segments", pathsPerNode.size() > 0);

                for (String path : pathsPerNode)
                    walArchives.add(Paths.get(path).toFile());
            }

            VisorWalTaskResult delRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.DELETE_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that delete task finished with no exceptions", delRes.results().size(), 4);

            List<File> walDeletedArchives = new ArrayList<>();

            for (Collection<String> pathsPerNode : delRes.results().values()) {
                assertTrue("Check that unused WAL segments deleted from all nodes",
                        pathsPerNode.size() > 0);

                for (String path : pathsPerNode)
                    walDeletedArchives.add(Paths.get(path).toFile());
            }

            for (File f : walDeletedArchives)
                assertTrue("Checking existing of deleted WAL archived segments: " + f.getAbsolutePath(), !f.exists());

            for (File f : walArchives)
                assertTrue("Checking existing of WAL archived segments from print task after delete: " + f.getAbsolutePath(),
                        !f.exists());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests that reserved WAL segment wasn't affected by {@link VisorWalTask}.
     *
     * @throws Exception if failed.
     */
    public void testReservedWalSegmentNotAffected() throws Exception {
        try {
            IgniteEx ig0 = (IgniteEx) startGrids(4);

            ig0.cluster().active(true);

            IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

            for (int k = 0; k < 10_000; k++)
                cache.put(k, new byte[1024]);

            forceCheckpoint();

            for (int k = 0; k < 1_000; k++)
                cache.put(k, new byte[1024]);

            forceCheckpoint();

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) ig0.context().cache().context()
                    .database();

            IgniteWriteAheadLogManager wal = ig0.context().cache().context().wal();

            long resIdx = getReservedWalSegmentIndex(dbMgr);

            // Reserve previous WAL segment.
            wal.reserve(new FileWALPointer(resIdx - 1, 0, 0));

            VisorWalTaskResult printRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS), false));

            int fileCnt2 = printRes.results().get(ig0.cluster().node().consistentId().toString()).size();

            // Check that manually reserved WAL segment wasn't printed.
            assertTrue(fileCnt2 == resIdx - 1 );

            VisorWalTaskResult delRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS), false));

            fileCnt2 = delRes.results().get(ig0.cluster().node().consistentId().toString()).size();

            // Check that manually reserved WAL segment wasn't deleted.
            assertTrue(fileCnt2 == resIdx - 1 );
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Get index of reserved WAL segment by checkpointer.
     *
     * @param dbMgr Database shared manager.
     * @throws Exception If failed.
     */
    private long getReservedWalSegmentIndex(GridCacheDatabaseSharedManager dbMgr) throws Exception{
        GridCacheDatabaseSharedManager.CheckpointHistory cpHist = dbMgr.checkpointHistory();

        Object histMap = GridTestUtils.getFieldValue(cpHist, "histMap");

        Object cpEntry = GridTestUtils.getFieldValue(GridTestUtils.invoke(histMap, "firstEntry"), "value");

        FileWALPointer walPtr = GridTestUtils.getFieldValue(cpEntry, "cpMark");

        return walPtr.index();
    }
}
