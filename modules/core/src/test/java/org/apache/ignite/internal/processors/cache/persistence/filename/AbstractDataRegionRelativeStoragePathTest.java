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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases when {@link CacheConfiguration#setStoragePaths(String...)} used to set custom data region storage path.
 */
@RunWith(Parameterized.class)
public abstract class AbstractDataRegionRelativeStoragePathTest extends GridCommonAbstractTest {
    /** Custom storage path . */
    static final String STORAGE_PATH = "storage";

    /** Second custom storage path. */
    static final String STORAGE_PATH_2 = "storage2";

    /** */
    static final String SNP_PATH = "ex_snapshots";

    /** */
    protected static final int PARTS_CNT = 15;

    /** */
    @Parameterized.Parameter()
    public boolean absPath;

    /** */
    @Parameterized.Parameter(1)
    public boolean severalCacheStorages;

    /** */
    @Parameterized.Parameters(name = "absPath={0},severalCacheStorages={1}")
    public static List<Object[]> params() {
        List<Object[]> res = new ArrayList<>();

        for (boolean absPath : new boolean[]{true, false}) {
            for (boolean severalCacheStorages : new boolean[]{true, false}) {
                res.add(new Object[] {absPath, severalCacheStorages});
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        if (absPath) {
            U.delete(new File(storagePath(STORAGE_PATH)).getParentFile());
            U.delete(new File(storagePath(STORAGE_PATH_2)).getParentFile());
        }
        else {
            U.delete(new File(U.defaultWorkDirectory(), STORAGE_PATH));
            U.delete(new File(U.defaultWorkDirectory(), STORAGE_PATH_2));
        }

        U.delete(new File(U.defaultWorkDirectory(), SNP_PATH));
    }

    /**
     * @param name Snapshot name
     * @param path Snapshot path.
     */
    void restoreAndCheck(String name, String path) throws Exception {
        List<NodeFileTree> fts = IgnitionEx.allGrids().stream()
            .map(ign -> ((IgniteEx)ign).context().pdsFolderResolver().fileTree())
            .collect(Collectors.toList());

        stopAllGrids();

        checkFileTrees(fts);

        fts.forEach(ft -> {
            U.delete(ft.nodeStorage());
            ft.extraStorages().values().forEach(U::delete);
        });

        U.delete(F.first(fts).db());

        IgniteEx srv = startAndActivate();

        checkDataNotExists();

        for (CacheConfiguration<?, ?> ccfg : ccfgs())
            grid(0).destroyCache(ccfg.getName());

        awaitPartitionMapExchange();

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (NodeFileTree ft : fts) {
                for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                    Stream<File> cacheFiles = Arrays.stream(ft.cacheStorages(ccfg)).flatMap(dir -> Arrays.stream(dir.listFiles()));

                    return cacheFiles.findAny().isEmpty();
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
    CacheConfiguration<?, ?> ccfg(String name, String grp, String... storagePath) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name)
            .setGroupName(grp)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(PARTS_CNT));

        if (!F.isEmpty(storagePath))
            ccfg.setStoragePaths(storagePath);

        return ccfg;
    }

    /** */
    String[] storagePaths(String... storagePath) {
        if (severalCacheStorages) {
            String[] res = new String[storagePath.length];

            for (int i = 0; i < storagePath.length; i++)
                res[i] = storagePath(storagePath[i]);

            return res;
        }

        return new String[] {storagePath(storagePath[0])};
    }

    /** */
    String storagePath(String storagePath) {
        try {
            return absPath ? new File(U.defaultWorkDirectory(), "abs/" + storagePath).getAbsolutePath() : storagePath;
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** @param fts Nodes file trees. */
    abstract void checkFileTrees(List<NodeFileTree> fts) throws IgniteCheckedException;

    /** Cache configs. */
    abstract CacheConfiguration[] ccfgs();
}
