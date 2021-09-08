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

package org.apache.ignite.util;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskArg;
import org.apache.ignite.internal.visor.misc.VisorWalTaskOperation;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Test correctness of VisorWalTask.
 */
public class GridInternalTaskUnusedWalSegmentsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
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
    @Test
    @WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "2")
    public void testCorrectnessOfDeletionTaskSegments() throws Exception {
        try {
            IgniteEx ig0 = (IgniteEx)startGrids(4);

            ig0.cluster().active(true);

            try (IgniteDataStreamer streamer = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int k = 0; k < 10_000; k++)
                    streamer.addData(k, new byte[1024]);
            }

            forceCheckpoint();

            try (IgniteDataStreamer streamer = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int k = 0; k < 1_000; k++)
                    streamer.addData(k, new byte[1024]);
            }

            forceCheckpoint();

            VisorWalTaskResult printRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that print task finished without exceptions", printRes.results().size(), 4);

            List<File> walArchives = new ArrayList<>();

            for (Collection<String> pathsPerNode : printRes.results().values()) {
                for (String path : pathsPerNode)
                    walArchives.add(Paths.get(path).toFile());
            }

            VisorWalTaskResult delRes = ig0.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ig0.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.DELETE_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that delete task finished with no exceptions", delRes.results().size(), 4);

            List<File> walDeletedArchives = new ArrayList<>();

            for (Collection<String> pathsPerNode : delRes.results().values()) {
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
}
