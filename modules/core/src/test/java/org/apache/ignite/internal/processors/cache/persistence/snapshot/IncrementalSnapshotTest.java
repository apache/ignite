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

import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Basic tests for incremental snapshots.
 */
public class IncrementalSnapshotTest extends AbstractSnapshotSelfTest {
    /** */
    public static final int GRID_CND = 3;

    /** */
    @Test
    public void testCreation() throws Exception {
        IgniteEx srv = startGridsWithCache(
            GRID_CND,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        IgniteEx cli = startClientGrid(
            GRID_CND,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        for (boolean client : new boolean[] {false, true}) {
            IgniteSnapshotManager snpCreate = snp(client ? cli : srv);

            String snpName = SNAPSHOT_NAME + "_" + client;

            snpCreate.createSnapshot(snpName).get(TIMEOUT);

            for (int incIdx = 1; incIdx < 3; incIdx++) {
                snpCreate.createIncrementalSnapshot(snpName).get(TIMEOUT);

                for (int gridIdx = 0; gridIdx < GRID_CND; gridIdx++)
                    assertTrue(checkIncremental(grid(gridIdx), snpName, incIdx));
            }
        }
    }

    /** */
    @Test
    public void testFailForUnknownBaseSnapshot() throws Exception {
        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(TIMEOUT),
            IgniteException.class
        );

        snp(ign).createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(TIMEOUT),
            IgniteException.class
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotNotEnoughSpace() throws Exception {
        // TODO: test that check exception that throw from smf file creation.
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnTopologyChange() throws Exception {
        IgniteEx srv = startGridsWithCache(
            GRID_CND,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        IgniteSnapshotManager snpCreate = snp(srv);

        snpCreate.createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        String consId = grid(1).context().discovery().localNode().consistentId().toString();

        // Stop some node.
        stopGrid(1);

        assertThrows(
            null,
            () -> snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Create incremental snapshot request has been rejected. " +
                "One of nodes from full snapshot offline [consistenId=" + consId + ']'
        );

        // Start another.
        startGrid(
            GRID_CND,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        assertThrows(
            null,
            () -> snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Create incremental snapshot request has been rejected. " +
                                    "One of nodes from full snapshot offline [consistenId=" + consId + ']'
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnCachesChange() throws Exception {
        // TODO: test that incremental snapshot fail if current cache state differs from base snapshot state.
    }

    /** */
    @Test
    public void testIncrementalSnapshotCleanedOnLeft() throws Exception {
        // TODO: test that incremental snapshot cleared if node left during creation.
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsIfRebalanceHappen() throws Exception {
        // TODO: test that incremental snapshot fail if rebalance happens between creation.
    }

    /** */
    @Test
    public void testIncrementalSnapshotWithExplicitPath() throws Exception {
        // TODO: test that incremental snapshot may be created with custom snapshot path.
    }

    /** */
    @Test
    public void testIncrementalSnapshotWithExplicitPathError() throws Exception {
        // TODO: test that incremental snapshot fails for invalid custom snapshot path.
    }

    /** */
    @Test
    public void testConcurrentIncrementalSnapshotFromClient() throws Exception {
        // See for cluster snapshot.
    }
}
