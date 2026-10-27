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

package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_MAP_SNAPSHOT_THRESHOLD;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_17_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.since;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage.EARLIEST_CP_SNAPSHOT_FILE;

/**
 * Checks that the earliest checkpoint map snapshot file ({@code cpMapSnapshot.bin}) written by a node of a previous
 * Ignite version is readable by the current version.
 */
@RunWith(Parameterized.class)
public class CheckpointMapSnapshotCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** Consistent id shared by the old and the current node, so they reuse the same PDS folder. */
    private static final String CONSISTENT_ID = "cp-map-snapshot-node";

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final int ENTRIES_CNT = 100;

    /** Message logged by {@code CheckpointMarkersStorage} when the snapshot file can't be deserialized. */
    private static final String UNMARSHAL_ERR_MSG = "Failed to unmarshal earliest checkpoint map snapshot";

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String oldVer;

    /** */
    @Parameterized.Parameters(name = "oldVer={0}")
    public static Collection<String> versions() {
        return since(VER_2_17_0);
    }

    /** */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        configure(cfg);

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> getJvmParams() {
        // Makes the old node write the snapshot file on every change of the earliest checkpoint map.
        return List.of("-D" + IGNITE_CHECKPOINT_MAP_SNAPSHOT_THRESHOLD + "=1");
    }

    /**
     * 1. Starts a node of the old version, waits until it writes {@code cpMapSnapshot.bin}
     * 2. Starts a node of the current version on the same PDS and checks that the snapshot file is read without errors.
     */
    @Test
    public void testSnapshotFileReadableByCurrentVersion() throws Exception {
        startGrid(1, oldVer, CheckpointMapSnapshotCompatibilityTest::configure, (ignite) -> {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

            for (int i = 0; i < ENTRIES_CNT; i++)
                cache.put(i, "val" + i);
        });

        File snpFile = new File(nodeFileTree(U.maskForFileName(CONSISTENT_ID)).checkpoint(), EARLIEST_CP_SNAPSHOT_FILE);

        assertTrue(
            "Old node must write " + snpFile.getAbsolutePath(),
            GridTestUtils.waitForCondition(snpFile::exists, 60_000)
        );

        stopAllGrids();

        byte[] oldBytes = Files.readAllBytes(snpFile.toPath());

        EarliestCheckpointMapSnapshot snp = Marshallers.jdk().unmarshal(oldBytes, null);

        assertNotNull(snp);

        listeningLog = new ListeningTestLogger(log);

        LogListener errLsnr = LogListener.matches(UNMARSHAL_ERR_MSG).build();

        listeningLog.registerListener(errLsnr);

        IgniteEx node = startGrid(0);

        node.cluster().state(ClusterState.ACTIVE);

        assertFalse(
            "Current version failed to read " + EARLIEST_CP_SNAPSHOT_FILE + " written by Ignite " + oldVer,
            errLsnr.check()
        );

        IgniteCache<Integer, String> cache = node.cache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_CNT; i++)
            assertEquals("val" + i, cache.get(i));
    }

    /** */
    private static void configure(IgniteConfiguration cfg) {
        cfg.setConsistentId(CONSISTENT_ID);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCheckpointFrequency(1_000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));
    }
}
