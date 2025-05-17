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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree.DB_DIR;

/**
 * Test cases when {@link CacheConfiguration#setStoragePath(String...)} used to set custom data region storage path.
 */
public class CacheConfigStoragePathTest extends AbstractDataRegionRelativeStoragePathTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setExtraStoragePathes(storagePath(STORAGE_PATH), storagePath(STORAGE_PATH_2));

        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(U.maskForFileName(igniteInstanceName))
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfgs());
    }

    /** {@inheritDoc} */
    @Override CacheConfiguration[] ccfgs() {
        return new CacheConfiguration[]{
            ccfg("cache0", null, null),
            ccfg("cache1", "grp1", null),
            ccfg("cache2", "grp1", null),
            ccfg("cache3", null, storagePath(STORAGE_PATH)),
            ccfg("cache4", "grp2", storagePath(STORAGE_PATH)),
            ccfg("cache5", null, storagePath(STORAGE_PATH_2)),
            ccfg("cache6", "grp3", storagePath(STORAGE_PATH_2)),
            ccfg("cache7", "grp3", storagePath(STORAGE_PATH_2))
        };
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

        srv.snapshot().createSnapshot("mysnp").get();

        File fullPathSnp = new File(U.defaultWorkDirectory(), SNP_PATH);

        srv.context().cache().context().snapshotMgr().createSnapshot("mysnp2", fullPathSnp.getAbsolutePath(), false, false).get();

        checkSnapshotFiles("mysnp", null);
        checkSnapshotFiles("mysnp2", fullPathSnp.getAbsolutePath());

        restoreAndCheck("mysnp", null);
        restoreAndCheck("mysnp2", fullPathSnp.getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override void checkFileTrees(List<NodeFileTree> fts) {
        for (NodeFileTree ft : fts) {
            for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                String storagePath = F.isEmpty(ccfg.getStoragePath()) ? null : ccfg.getStoragePath()[0];

                File customRoot = storagePath == null ? ft.root() : ensureExists(useAbsStoragePath
                    ? new File(storagePath)
                    : new File(ft.root(), storagePath)
                );
                File db = ensureExists(new File(customRoot, DB_DIR));
                File nodeStorage = ensureExists(new File(db, ft.folderName()));

                ensureExists(new File(nodeStorage, ft.cacheStorage(ccfg).getName()));
            }
        }
    }

    /**
     * @param name Snapshot name.
     * @param path Snapshot path.
     */
    private void checkSnapshotFiles(String name, String path) {
        NodeFileTree ft = grid(0).context().pdsFolderResolver().fileTree();

        Function<String, File> snpRootF = storage -> {
            File snpRoot;

            if (path != null)
                snpRoot = new File(path);
            else if (storage == null)
                snpRoot = ft.snapshotsRoot();
            else {
                File nodeStorage = useAbsStoragePath
                    ? new File(storage)
                    : new File(ft.root(), storage);

                snpRoot = new File(nodeStorage, DFLT_SNAPSHOT_DIRECTORY);
            }

            return new File(snpRoot, name);
        };

        // Snapshot root directories.
        Set<File> roots = new HashSet<>(Arrays.asList(
            snpRootF.apply(null),
            snpRootF.apply(storagePath(STORAGE_PATH)),
            snpRootF.apply(storagePath(STORAGE_PATH_2))
        ));

        assertEquals(path != null ? 1 : 3, roots.size());

        // Root -> cache -> partition set.
        Map<File, Map<String, Set<Integer>>> snpFiles = new HashMap<>();

        // Collecting all partition files under each snapshot root.
        roots.forEach(snpRoot -> {
            assertTrue(snpRoot.exists());
            assertTrue(snpRoot.isDirectory());

            try (Stream<Path> files = Files.walk(snpRoot.toPath())) {
                files.filter(p -> NodeFileTree.partitionFile(p.toFile())).forEach(part -> {
                    File root = roots.stream().filter(r -> part.startsWith(r.toPath())).findFirst().orElseThrow();

                    snpFiles
                        .computeIfAbsent(root, r -> new HashMap<>())
                        .computeIfAbsent(NodeFileTree.cacheName(part.getParent().toFile()), c -> new HashSet<>())
                        .add(NodeFileTree.partId(part.toFile()));
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Set<String> seenGrps = new HashSet<>();

        Arrays.stream(ccfgs()).forEach(ccfg -> {
            String cname = CU.cacheOrGroupName(ccfg);

            if (seenGrps.contains(cname))
                return;

            seenGrps.add(cname);

            File expRoot = snpRootF.apply(F.isEmpty(ccfg.getStoragePath()) ? null : ccfg.getStoragePath()[0]);

            assertTrue(cname + " must be found", snpFiles.containsKey(expRoot));

            Set<Integer> parts = snpFiles.get(expRoot).get(cname);

            assertNotNull(cname + " partitions must be found", parts);
            assertEquals("All partitions for " + cname + " must be found", PARTS_CNT, parts.size());

            IntStream.range(0, PARTS_CNT).forEach(i -> assertTrue(i + " partition must be found", parts.contains(i)));
        });

        assertEquals(6, seenGrps.size());
    }
}
