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
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;

/**
 * Test cases when {@link DataRegionConfiguration#setStoragePath(String)} used to set custom data region storage path.
 */
public class DataRegionStoragePathTest extends GridCommonAbstractTest {
    /** Custom storage path for default data region. */
    private static final String DEFAULT_DR_STORAGE_PATH = "dflt_dr";

    /** Custom storage path for custom data region. */
    private static final String CUSTOM_STORAGE_PATH = "custom_dr";

    /** Data region name with custom storage. */
    private static final String DR_WITH_STORAGE = "custom-storage";

    /** Data region with default storage. */
    private static final String DR_WITH_DFLT_STORAGE = "default-storage";

    /** */
    public final CacheConfiguration[] ccfgs = new CacheConfiguration[] {
        ccfg("cache0", null, null),
        ccfg("cache1", "grp1", null),
        ccfg("cache2", "grp1", null),
        ccfg("cache3", null, DR_WITH_DFLT_STORAGE),
        ccfg("cache4", "grp2", DR_WITH_DFLT_STORAGE),
        ccfg("cache5", null, DR_WITH_STORAGE),
        ccfg("cache6", "grp3", DR_WITH_STORAGE),
        ccfg("cache7", "grp3", DR_WITH_STORAGE)
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration().setStoragePath(DEFAULT_DR_STORAGE_PATH).setPersistenceEnabled(true);

        dsCfg.setDataRegionConfigurations(
            new DataRegionConfiguration().setName(DR_WITH_STORAGE).setStoragePath(CUSTOM_STORAGE_PATH).setPersistenceEnabled(true),
            new DataRegionConfiguration().setName(DR_WITH_DFLT_STORAGE).setPersistenceEnabled(true)
        );

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfgs);
    }

    /** */
    @Test
    public void testCaches() throws Exception {
        int nodeCnt = 3;

        IgniteEx srv = startGrids(nodeCnt);

        srv.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < 8; i++) {
            IgniteCache<Integer, Integer> c = srv.cache("cache" + i);

            for (int j=0; j<100; j++)
                c.put(j, i);
        }

        Consumer<IgniteEx> check = srv0 -> {
            for (int i = 0; i < 8; i++) {
                IgniteCache<Integer, Integer> c = srv0.cache("cache" + i);

                for (int j=0; j<100; j++)
                    assertEquals((Integer)i, c.get(j));
            }
        };

        check.accept(srv);

        stopAllGrids();

        srv = startGrids(3);

        srv.cluster().state(ClusterState.ACTIVE);

        check.accept(srv);

        List<NodeFileTree> fts = IntStream.range(0, 3)
            .mapToObj(this::grid)
            .map(ign -> ign.context().pdsFolderResolver().fileTree())
            .collect(Collectors.toList());

        stopAllGrids();

        for (NodeFileTree ft : fts) {
            boolean[] flags = new boolean[2];

            for (CacheConfiguration<?, ?> ccfg : ccfgs) {
                File db;

                if (Objects.equals(ccfg.getDataRegionName(), DR_WITH_DFLT_STORAGE)) {
                    db = ensureExists(new File(ft.root(), DB_DEFAULT_FOLDER));

                    flags[0] = true;
                }
                else {
                    File customRoot = ensureExists(new File(
                        ft.root(),
                        ccfg.getDataRegionName() == null ? DEFAULT_DR_STORAGE_PATH : CUSTOM_STORAGE_PATH)
                    );

                    db = ensureExists(new File(customRoot, DB_DEFAULT_FOLDER));

                    flags[1] = true;
                }

                File nodeStorage = ensureExists(new File(db, ft.folderName()));

                ensureExists(new File(nodeStorage, cacheDirName(ccfg)));
            }

            for (boolean flag : flags)
                assertTrue(flag);
        }
    }

    /** */
    private static File ensureExists(File file) {
        assertTrue(file.getAbsolutePath() + " must exists", file.exists());

        return file;
    }

    /** */
    private static CacheConfiguration<?, ?> ccfg(String name, String grp, String dr) {
        return new CacheConfiguration<>(name).setGroupName(grp).setDataRegionName(dr);
    }
}
