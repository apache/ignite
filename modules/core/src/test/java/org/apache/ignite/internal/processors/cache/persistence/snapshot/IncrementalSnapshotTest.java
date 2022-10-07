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

import java.io.File;
import java.util.Collections;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Basic tests for incremental snapshots.
 */
public class IncrementalSnapshotTest extends AbstractSnapshotSelfTest {
    /** */
    public static final int GRID_CNT = 3;

    /** */
    public static final String OTHER_CACHE = "other-cache";

    /** */
    public static final String GROUPED_CACHE = "my-grouped-cache2";

    /** */
    @Test
    public void testCreation() throws Exception {
        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        assertTrue("Target directory is not empty: " + snpDir, F.isEmpty(snpDir.list()));

        IgniteEx cli = startClientGrid(
            GRID_CNT,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        for (boolean client : new boolean[] {false, true}) {
            IgniteSnapshotManager snpCreate = snp(client ? cli : srv);

            for (File snpPath : new File[] {null, exSnpDir}) {
                if (client && snpPath != null) // Set snapshot path not supported for snapshot from client nodes.
                    continue;

                String snpName = SNAPSHOT_NAME + "_" + client + "_" + (snpPath == null ? "" : snpPath.getName());

                if (snpPath == null)
                    snpCreate.createSnapshot(snpName).get(TIMEOUT);
                else
                    snpCreate.createSnapshot(snpName, snpPath.getAbsolutePath(), false).get(TIMEOUT);

                for (int incIdx = 1; incIdx < 3; incIdx++) {
                    if (snpPath == null)
                        snpCreate.createIncrementalSnapshot(snpName).get(TIMEOUT);
                    else
                        snpCreate.createSnapshot(snpName, snpPath.getAbsolutePath(), true).get(TIMEOUT);

                    for (int gridIdx = 0; gridIdx < GRID_CNT; gridIdx++) {
                        assertTrue("Incremental snapshot must exists on node " + gridIdx, checkIncremental(
                            grid(gridIdx),
                            snpName,
                            snpPath == null ? null : snpPath.getAbsolutePath(),
                            incIdx
                        ));
                    }
                }
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
    public void testIncrementalSnapshotFailsOnTopologyChange() throws Exception {
        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
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
                "Node from full snapshot offline [consistentId=" + consId + ']'
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnCacheDestroy() throws Exception {
        checkFailWhenCacheDestroyed(OTHER_CACHE, "Create incremental snapshot request has been rejected. " +
            "Cache group destroyed [groupId=" + CU.cacheId(OTHER_CACHE) + ']');

        checkFailWhenCacheDestroyed(GROUPED_CACHE, "Create incremental snapshot request has been rejected. " +
            "Cache destroyed [cacheId=" + CU.cacheId(GROUPED_CACHE) + ", cacheName=" + GROUPED_CACHE + ']');
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnCacheChange() throws Exception {
        CacheConfiguration<Integer, Account> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        IgniteEx srv = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key), ccfg);

        snp(srv).createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        GridLocalConfigManager locCfgMgr = srv.context().cache().configManager();

        File ccfgFile = locCfgMgr.cacheConfigurationFile(ccfg);

        StoredCacheData cacheData = locCfgMgr.readCacheData(ccfgFile);

        assertNotNull(cacheData);

        cacheData.queryEntities(Collections.singletonList(new QueryEntity(String.class, Account.class)));

        locCfgMgr.writeCacheData(cacheData, ccfgFile);

        assertThrows(
            null,
            () -> snp(srv).createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Cache changed [cacheId=" + CU.cacheId(DEFAULT_CACHE_NAME) + ", cacheName=" + DEFAULT_CACHE_NAME + ']'
        );

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailOnDirtyDir() throws Exception {
        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        snp(srv).createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        assertTrue(snp(srv).incrementalSnapshotsLocalRootDir(SNAPSHOT_NAME, null).mkdirs());
        assertTrue(snp(srv).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, 1).createNewFile());

        assertThrows(
            null,
            () -> snp(srv).createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Can't create snapshot directory"
        );

        for (int i = 0; i < GRID_CNT; i++)
            assertFalse(snp(srv).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, 1).exists());
    }

    /** */
    private void checkFailWhenCacheDestroyed(String cache2rvm, String errMsg) throws Exception {
        IgniteEx srv = startGridsWithCache(
            1,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME),
            new CacheConfiguration<>(OTHER_CACHE),
            new CacheConfiguration<Integer, Object>("my-grouped-cache1").setGroupName("mygroup"),
            new CacheConfiguration<Integer, Object>(GROUPED_CACHE).setGroupName("mygroup")
        );

        IgniteSnapshotManager snpCreate = snp(srv);

        snpCreate.createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        srv.destroyCache(cache2rvm);

        assertThrows(
            null,
            () -> snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            errMsg
        );

        stopAllGrids();
        cleanPersistenceDir();
    }
}
