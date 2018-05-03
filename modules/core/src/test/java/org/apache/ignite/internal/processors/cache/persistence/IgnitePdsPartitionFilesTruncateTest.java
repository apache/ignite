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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks that evicted partitions doesn't leave files in PDS.
 */
public class IgnitePdsPartitionFilesTruncateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                            .setPageSize(1024)
                            .setWalMode(WALMode.LOG_ONLY)
                            .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(true)))
                .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                        .setBackups(1)
                        .setAffinity(new RendezvousAffinityFunction(false, 32)));

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

    /**
     * @throws Exception If failed.
     */
    public void testTruncatingPartitionFilesOnEviction() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        try (IgniteDataStreamer<Integer,String> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, "Value " + i);
        }

        assertEquals(1, ignite0.cacheNames().size());

        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(0);
        checkPartFiles(1);
        checkPartFiles(2);

        stopGrid(2);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(0);
        checkPartFiles(1);

        startGrid(2);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(0);
        checkPartFiles(1);
        checkPartFiles(2);
    }

    /**
     * @param idx Node index.
     */
    private void checkPartFiles(int idx) throws Exception {
        Ignite ignite = grid(idx);

        int[] parts = ignite.affinity(DEFAULT_CACHE_NAME).allPartitions(ignite.cluster().localNode());

        Path dirPath = Paths.get(U.defaultWorkDirectory(), "db",
                U.maskForFileName(ignite.configuration().getIgniteInstanceName()), "cache-" + DEFAULT_CACHE_NAME);

        info("Path: " + dirPath.toString());

        assertTrue(Files.exists(dirPath));

        for (Path f : Files.newDirectoryStream(dirPath)) {
            if (f.getFileName().toString().startsWith("part-"))
                assertTrue("Node_" + idx +" should contains only partitions " + Arrays.toString(parts)
                        + ", but the file is redundant: " + f.getFileName(), anyMatch(parts, f));
        }
    }

    /** */
    private boolean anyMatch(int[] parts, Path f) {
        Pattern ptrn = Pattern.compile("part-(\\d+).bin");
        Matcher matcher = ptrn.matcher(f.getFileName().toString());

        if (!matcher.find())
            throw new IllegalArgumentException("File is not a partition:" + f.getFileName());

        int part = Integer.parseInt(matcher.group(1));

        for (int p: parts) {
            if (p == part)
                return true;
        }

        return false;
    }
}
