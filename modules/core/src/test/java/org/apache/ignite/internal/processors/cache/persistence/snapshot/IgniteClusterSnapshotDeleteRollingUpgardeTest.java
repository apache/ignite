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

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class IgniteClusterSnapshotDeleteRollingUpgardeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final int ALL_GRIDS = 4;

    /** */
    private static final int CLIENTS = 1;

    /** */
    private static final String SNP_NAME = "testSnapshot";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        var cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
        );

        return cfg;
    }

    /** */
    @Test
    public void testNodeNotSupportingSnapshotDeleteFeature() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.19.0", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        createCacheAndSnasphot(1);

        ensureSnapshotDeletionFailed();

        ru(grid(0)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.19.1");

            ensureSnapshotDeletionFailed();
        }

        ru(grid(1)).finalizeClusterVersion();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertFalse(ru(grid(i)).isVersionUpgradeEnabled());

            assertFalse(F.isEmpty(snp(i).deleteSnapshot(SNP_NAME, null).get().completedNodes));

            if (i < ALL_GRIDS - 1)
                createSnapshot(i);
        }
    }

    /** */
    private void createCacheAndSnasphot(int gridIdx) {
        int partsCnt = 32;
        int keysCnt = partsCnt * 10;

        grid(gridIdx).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        try (var ds = grid(gridIdx).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < keysCnt; i++)
                ds.addData(i, i);
        }

        createSnapshot(gridIdx);
    }

    /** */
    private void createSnapshot(int gridIdx) {
        snp(gridIdx).createSnapshot(SNP_NAME).get(getTestTimeout());
    }

    /** */
    private void ensureSnapshotDeletionFailed() {
        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertThrowsAnyCause(
                null,
                () -> snp(i0).deleteSnapshot(SNP_NAME, null).get(),
                IgniteIllegalStateException.class,
                "The snapshot deletion feature isn't activated yet"
            );
        }
    }

    /** */
    private IgniteSnapshotManager snp(int gridIdx) {
        return grid(gridIdx).context().cache().context().snapshotMgr();
    }
}
