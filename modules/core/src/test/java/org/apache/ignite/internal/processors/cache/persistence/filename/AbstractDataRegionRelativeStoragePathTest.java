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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.util.List;
import java.util.function.ObjIntConsumer;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases when {@link DataRegionConfiguration#setStoragePath(String)} used to set custom data region storage path.
 */
@RunWith(Parameterized.class)
public abstract class AbstractDataRegionRelativeStoragePathTest extends GridCommonAbstractTest {
    /** Custom storage path for default data region. */
    static final String DEFAULT_DR_STORAGE_PATH = "dflt_dr";

    /** Custom storage path for custom data region. */
    static final String CUSTOM_STORAGE_PATH = "custom_dr";

    /** Data region name with custom storage. */
    static final String DR_WITH_STORAGE = "custom-storage";

    /** Data region with default storage. */
    static final String DR_WITH_DFLT_STORAGE = "default-storage";

    /** */
    static final String SNP_PATH = "ex_snapshots";

    /** */
    @Parameterized.Parameter()
    public boolean useAbsStoragePath;

    /** */
    @Parameterized.Parameters(name = "useAbsStoragePath={0}")
    public static Object[] params() {
        return new Object[]{true, false};
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        if (useAbsStoragePath)
            U.delete(new File(storagePath(DEFAULT_DR_STORAGE_PATH)).getParentFile());
        else {
            U.delete(new File(U.defaultWorkDirectory(), DEFAULT_DR_STORAGE_PATH));
            U.delete(new File(U.defaultWorkDirectory(), CUSTOM_STORAGE_PATH));
        }

        U.delete(new File(U.defaultWorkDirectory(), SNP_PATH));
    }

    /**
     * @param name Snapshot name
     * @param path Snapshot path.
     * @param fts Nodes file trees.
     */
    void restoreAndCheck(String name, String path, List<NodeFileTree> fts) throws Exception {
        stopAllGrids();

        checkFileTrees(fts);

        fts.forEach(ft -> {
            U.delete(ft.nodeStorage());
            ft.dataRegionStorages().values().forEach(U::delete);
        });

        U.delete(F.first(fts).db());

        IgniteEx srv = startAndActivate();

        checkDataNotExists();

        for (CacheConfiguration<?, ?> ccfg : ccfgs())
            grid(0).destroyCache(ccfg.getName());

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (NodeFileTree ft : fts) {
                for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                    if (!F.isEmpty(ft.cacheStorage(ccfg).listFiles()))
                        return false;
                }
            }

            return true;
        }, getTestTimeout()));

        srv.context().cache().context().snapshotMgr().restoreSnapshot(name, path, null).get();

        checkDataExists();
    }

    /** */
    void putData() {
        forAllEntries((c, j) -> c.put(j, j));
    }

    /** */
    void checkDataExists() {
        forAllEntries((c, j) -> assertEquals((Integer)j, c.get(j)));
    }

    /** */
    private void checkDataNotExists() {
        forAllEntries((c, j) -> assertNull(c.get(j)));
    }

    /** */
    void forAllEntries(ObjIntConsumer<IgniteCache<Integer, Integer>> cnsmr) {
        for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
            IgniteCache<Integer, Integer> c = grid(0).cache(ccfg.getName());

            IntStream.range(0, 100).forEach(j -> cnsmr.accept(c, j));
        }
    }

    /** */
    IgniteEx startAndActivate() throws Exception {
        IgniteEx srv = startGrids(3);

        srv.cluster().state(ClusterState.ACTIVE);

        return srv;
    }

    /** */
    File ensureExists(File file) {
        assertTrue(file.getAbsolutePath() + " must exists", file.exists());

        return file;
    }

    /** */
    CacheConfiguration<?, ?> ccfg(String name, String grp, String dr) {
        return new CacheConfiguration<>(name)
            .setGroupName(grp)
            .setDataRegionName(dr)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(15));
    }

    /** */
    String storagePath(String storagePath) throws IgniteCheckedException {
        return useAbsStoragePath ? new File(U.defaultWorkDirectory(), "abs/" + storagePath).getAbsolutePath() : storagePath;
    }

    /** @param fts Nodes file trees. */
    abstract void checkFileTrees(List<NodeFileTree> fts) throws IgniteCheckedException;

    /** Cache configs. */
    abstract CacheConfiguration[] ccfgs();
}
