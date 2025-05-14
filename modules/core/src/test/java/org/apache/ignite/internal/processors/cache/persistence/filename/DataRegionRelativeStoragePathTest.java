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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree.DB_DIR;

/**
 * Test cases when {@link DataRegionConfiguration#setStoragePath(String)} used to set custom data region storage path.
 */
public class DataRegionRelativeStoragePathTest extends AbstractDataRegionRelativeStoragePathTest {
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

        dsCfg.getDefaultDataRegionConfiguration().setStoragePath(storagePath(DEFAULT_DR_STORAGE_PATH)).setPersistenceEnabled(true);

        dsCfg.setDataRegionConfigurations(
            new DataRegionConfiguration().setName(DR_WITH_STORAGE)
                .setStoragePath(storagePath(CUSTOM_STORAGE_PATH))
                .setPersistenceEnabled(true),
            new DataRegionConfiguration()
                .setName(DR_WITH_DFLT_STORAGE)
                .setPersistenceEnabled(true)
        );

        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(U.maskForFileName(igniteInstanceName))
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfgs);
    }

    /** {@inheritDoc} */
    @Override CacheConfiguration[] ccfgs() {
        return ccfgs;
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

    /** {@inheritDoc} */
    @Override void checkFileTrees(List<NodeFileTree> fts) throws IgniteCheckedException {
        for (NodeFileTree ft : fts) {
            boolean[] flags = new boolean[2];

            for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                File db;

                if (Objects.equals(ccfg.getDataRegionName(), DR_WITH_DFLT_STORAGE)) {
                    db = ensureExists(new File(ft.root(), DB_DIR));

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
}
