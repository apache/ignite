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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree.DB_DIR;

/**
 * Test cases when {@link CacheConfiguration#setStoragePath(String)} used to set custom data region storage path.
 */
@RunWith(Parameterized.class)
public class DataRegionRelativeStoragePathTest extends GridCommonAbstractTest {
    /** Custom storage path for default data region. */
    private static final String DEFAULT_DR_STORAGE_PATH = "dflt_dr";

    /** Custom storage path for custom data region. */
    private static final String CUSTOM_STORAGE_PATH = "custom_dr";

    /** */
    private static final String SNP_PATH = "ex_snapshots";

    /** */
    public CacheConfiguration[] ccfgs;

    /** */
    @Parameterized.Parameter()
    public boolean useAbsStoragePath;

    /** */
    @Parameterized.Parameters(name = "useAbsStoragePath={0}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean useAbsStoragePath : new boolean[]{true, false})
            params.add(new Object[]{useAbsStoragePath});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setStoragePath(storagePath(DEFAULT_DR_STORAGE_PATH))
            .setExtraStoragePathes(storagePath(CUSTOM_STORAGE_PATH))
            .getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(U.maskForFileName(igniteInstanceName))
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfgs);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.delete(new File(U.defaultWorkDirectory()));

        ccfgs = new CacheConfiguration[]{
            ccfg("cache0", null, null),
            ccfg("cache1", "grp1", null),
            ccfg("cache2", "grp1", null),
            ccfg("cache3", null, storagePath(DEFAULT_DR_STORAGE_PATH)),
            ccfg("cache4", "grp2", storagePath(DEFAULT_DR_STORAGE_PATH)),
            ccfg("cache5", null, storagePath(CUSTOM_STORAGE_PATH)),
            ccfg("cache6", "grp3", storagePath(CUSTOM_STORAGE_PATH)),
            ccfg("cache7", "grp3", storagePath(CUSTOM_STORAGE_PATH))
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

/*
        cleanPersistenceDir();

        if (useAbsStoragePath)
            U.delete(new File(storagePath(DEFAULT_DR_STORAGE_PATH)).getParentFile());
        else {
            U.delete(new File(U.defaultWorkDirectory(), DEFAULT_DR_STORAGE_PATH));
            U.delete(new File(U.defaultWorkDirectory(), CUSTOM_STORAGE_PATH));
        }

        U.delete(new File(U.defaultWorkDirectory(), SNP_PATH));
*/
    }

    /** */
    @Test
    public void testCaches() throws Exception {
        startAndActivate();

        putData();

        checkDataExists();

        stopAllGrids();

        IgniteEx srv = startAndActivate();

        checkDataExists();

        List<NodeFileTree> fts = IntStream.range(0, 3)
            .mapToObj(this::grid)
            .map(ign -> ign.context().pdsFolderResolver().fileTree())
            .collect(Collectors.toList());

        srv.snapshot().createSnapshot("mysnp").get();

        File fullPathSnp = new File(U.defaultWorkDirectory(), SNP_PATH);

        srv.context().cache().context().snapshotMgr().createSnapshot("mysnp2", fullPathSnp.getAbsolutePath(), false, false).get();

        restoreAndCheck("mysnp", null, fts);

        restoreAndCheck("mysnp2", fullPathSnp.getAbsolutePath(), fts);
    }

    /**
     * @param name Snapshot name
     * @param path Snapshot path.
     * @param fts Nodes file trees.
     */
    private void restoreAndCheck(String name, String path, List<NodeFileTree> fts) throws Exception {
        stopAllGrids();

        checkFileTrees(fts);

        fts.forEach(ft -> ft.extraStorages().values().forEach(U::delete));

        U.delete(F.first(fts).db());

        IgniteEx srv = startAndActivate();

        checkDataNotExists();

        for (CacheConfiguration<?, ?> ccfg : ccfgs)
            grid(0).destroyCache(ccfg.getName());

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (NodeFileTree ft : fts) {
                for (CacheConfiguration<?, ?> ccfg : ccfgs) {
                    if (!F.isEmpty(ft.cacheStorage(ccfg).listFiles()))
                        return false;
                }
            }

            return true;
        }, getTestTimeout()));

        srv.context().cache().context().snapshotMgr().restoreSnapshot(name, path, null).get();

        checkDataExists();
    }

    /** @param fts Nodes file trees. */
    private void checkFileTrees(List<NodeFileTree> fts) throws IgniteCheckedException {
        for (NodeFileTree ft : fts) {
            boolean[] flags = new boolean[2];

            for (CacheConfiguration<?, ?> ccfg : ccfgs) {
                File db;

                if (ccfg.getStoragePath() == null || Objects.equals(ccfg.getStoragePath(), storagePath(DEFAULT_DR_STORAGE_PATH))) {
                    ensureExists(new File(ft.root(), DB_DIR));

                    db = ensureExists(Path.of(storagePath(DEFAULT_DR_STORAGE_PATH), ft.folderName()).toFile());

                    flags[0] = true;
                }
                else {
                    String storagePath = ccfg.getDataRegionName() == null ? DEFAULT_DR_STORAGE_PATH : CUSTOM_STORAGE_PATH;

                    File customRoot = ensureExists(useAbsStoragePath
                        ? new File(storagePath(storagePath))
                        : new File(ft.root(), storagePath)
                    );

                    db = ensureExists(new File(customRoot, DB_DIR));

                    flags[1] = true;
                }

                File nodeStorage = ensureExists(new File(db, ft.folderName()));

                ensureExists(new File(nodeStorage, ft.cacheStorage(ccfg).getName()));
            }

            for (boolean flag : flags)
                assertTrue(flag);
        }
    }

    /** */
    private void putData() {
        forAllEntries((c, j) -> c.put(j, j));
    }

    /** */
    private void checkDataExists() {
        forAllEntries((c, j) -> assertEquals((Integer)j, c.get(j)));
    }

    /** */
    private void checkDataNotExists() {
        forAllEntries((c, j) -> assertNull(c.get(j)));
    }

    /** */
    private void forAllEntries(ObjIntConsumer<IgniteCache<Integer, Integer>> cnsmr) {
        for (CacheConfiguration<?, ?> ccfg : ccfgs) {
            IgniteCache<Integer, Integer> c = grid(0).cache(ccfg.getName());

            IntStream.range(0, 100).forEach(j -> cnsmr.accept(c, j));
        }
    }

    /** */
    private IgniteEx startAndActivate() throws Exception {
        IgniteEx srv = startGrids(3);

        srv.cluster().state(ClusterState.ACTIVE);

        return srv;
    }

    /** */
    private File ensureExists(File file) {
        assertTrue(file.getAbsolutePath() + " must exists", file.exists());

        return file;
    }

    /** */
    private CacheConfiguration<?, ?> ccfg(String name, String grp, String storagePath) {
        return new CacheConfiguration<>(name)
            .setGroupName(grp)
            .setStoragePath(storagePath)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(15));
    }

    /** */
    private String storagePath(String storagePath) throws IgniteCheckedException {
        return useAbsStoragePath ? new File(U.defaultWorkDirectory(), "abs/" + storagePath).getAbsolutePath() : storagePath;
    }
}
