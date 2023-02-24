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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class IncrementalSnapshotRebalanceTest extends AbstractIncrementalSnapshotTest {
    /** */
    private static final String CACHE2 = "CACHE2";

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE), cacheConfiguration(CACHE2));

        return cfg;
    }

    /** */
    @Test
    public void createIncSnpAfterClusterRestartIsAllowed() throws Exception {
        stopAllGrids();

        Ignite ign = startGrids(3);

        awaitPartitionMapExchange();

        ign.cluster().state(ClusterState.ACTIVE);

        snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());
    }

    /** */
    @Test
    public void skipNonBaselineNodesDoesNotForbidIncSnapshot() throws Exception {
        startGrid(nodes() + 2);

        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE).put(i, i);

        stopGrid(nodes() + 1);
        stopGrid(nodes() + 2);

        awaitPartitionMapExchange();

        snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());
    }

    /** */
    @Test
    public void createIncSnpAfterRebalanceIsForbidden() throws Exception {
        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE).put(i, i);

        stopGrid(1);

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "Create incremental snapshot request has been rejected. Node from full snapshot offline");

        for (int i = 1_000; i < 2_000; i++)
            grid(0).cache(CACHE).put(i, i);

        startGrid(1);

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "WAL was disabled");

        stopAllGrids();

        startGrids(nodes());

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "WAL was disabled");
    }

    /** */
    @Test
    public void forbidIncSnpCreationAfterRestore() throws Exception {
        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE).put(i, i);

        snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

        restartWithCleanPersistence(nodes(), F.asList(CACHE, CACHE2));

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "WAL was disabled");
    }

    /** */
    @Test
    public void forbidIncSnpCreationAllowedAfterFullSnapshot() throws Exception {
        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE).put(i, i);

        snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

        restartWithCleanPersistence(nodes(), F.asList(CACHE, CACHE2));

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "WAL was disabled");

        snp(grid(0)).createSnapshot(SNP + "0").get(getTestTimeout());
        snp(grid(0)).createIncrementalSnapshot(SNP + "0").get(getTestTimeout());
    }

    /** */
    @Test
    public void incSnpIsNotAllowedIfOnlyOneCacheWalIsDisabled() throws Exception {
        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE).put(i, i);

        snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

        stopGrid(1);

        for (int i = 0; i < 1_000; i++)
            grid(0).cache(CACHE2).put(i, i);

        startGrid(1);

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(log, () -> snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class, "WAL was disabled");
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }
}
