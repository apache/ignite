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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;

/**
 * Test cases when {@link CacheConfiguration#setStoragePaths(String...)} used to set custom data region storage path.
 */
public class CacheConfigStoragePathTest extends AbstractDataRegionRelativeStoragePathTest {
    /** {@inheritDoc} */
    @Override protected DataStorageConfiguration dataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setExtraStoragePaths(storagePath(STORAGE_PATH), storagePath(STORAGE_PATH_2), storagePath(IDX_PATH));
    }

    /** {@inheritDoc} */
    @Override CacheConfiguration[] ccfgs() {
        return new CacheConfiguration[]{
            ccfg("cache0", null),
            ccfg("cache1", "grp1"),
            ccfg("cache2", "grp1"),
            ccfg("cache3", null, storagePaths(STORAGE_PATH, STORAGE_PATH_2)),
            ccfg("cache4", "grp2", storagePaths(STORAGE_PATH_2, STORAGE_PATH)),
            ccfg("cache5", null, storagePaths(STORAGE_PATH_2, STORAGE_PATH)),
            ccfg("cache6", "grp3", storagePaths(STORAGE_PATH_2, STORAGE_PATH)),
            ccfg("cache7", "grp3", storagePaths(STORAGE_PATH_2, STORAGE_PATH))
        };
    }

    /** Sanity checks - all paths for all partitions are different and contain cacheDir. */
    @Test
    public void testPathGeneration() throws Exception {
        IgniteEx srv = startAndActivate();

        NodeFileTree ft = srv.context().pdsFolderResolver().fileTree();

        IntConsumer checkPart = i -> {
            Set<File> parts = new HashSet<>();

            Set<String> grps = new HashSet<>();

            for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                if (!grps.add(CU.cacheOrGroupName(ccfg)))
                    continue;

                File part = ft.partitionFile(ccfg, i);

                assertTrue(Arrays.asList(ft.cacheStorages(ccfg)).contains(part.getParentFile()));
                assertTrue(parts.add(part));
            }

            assertEquals(grpCount(), grps.size());
        };

        for (int i = 0; i < PARTS_CNT; i++)
            checkPart.accept(i);

        checkPart.accept(INDEX_PARTITION);

        stopAllGrids();
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
                String[] csp = ccfg.getStoragePaths();

                for (File cacheDir : ft.cacheStorages(ccfg)) {
                    ensureExists(cacheDir);

                    if (!F.isEmpty(csp)) {
                        for (File partfile : cacheDir.listFiles(NodeFileTree::partitionFile)) {
                            int part = NodeFileTree.partId(partfile);

                            assertTrue(partfile.getAbsolutePath().contains(csp[part % csp.length]));
                        }
                    }
                }
            }
        }
    }

    /**
     * @param name Snapshot name.
     * @param path Snapshot path.
     */
    private void checkSnapshotFiles(String name, String path) {
        // Only in "separate root" mode snapshots for nodes will have different roots.
        List<NodeFileTree> fts = pathMode == PathMode.SEPARATE_ROOT
            ? Ignition.allGrids().stream().map(n -> ((IgniteEx)n).context().pdsFolderResolver().fileTree()).collect(Collectors.toList())
            : Collections.singletonList(grid(0).context().pdsFolderResolver().fileTree());

        Map<Integer, Set<Integer>> partsMap = new HashMap<>();

        for (NodeFileTree ft : fts) {
            Function<String, File> snpRootF = storage -> {
                File snpRoot;

                if (path != null)
                    snpRoot = new File(path);
                else if (storage == null)
                    snpRoot = ft.snapshotsRoot();
                else {
                    File nodeStorage = pathMode == PathMode.ABS
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

            boolean idxPathUsed = idxStorage && idxPartMustExistsInSnapshot();

            if (idxPathUsed)
                roots.add(snpRootF.apply(storagePath(IDX_PATH)));

            // Sanity check storagePath returns different paths.
            assertEquals(path != null ? 1 : (idxPathUsed ? 4 : 3), roots.size());


            // Root -> cache -> partition set.
            Map<File, Map<String, Set<Integer>>> snpFiles = new HashMap<>();

            // Collecting all partition files under each snapshot root.
            for (File snpRoot : roots) {
                assertTrue(snpRoot.exists());
                assertTrue(snpRoot.isDirectory());

                Predicate<Path> pathPredicate = p -> NodeFileTree.partitionFile(p.toFile())
                    || p.getFileName().toString().equals(NodeFileTree.partitionFileName(INDEX_PARTITION));

                try (Stream<Path> files = Files.walk(snpRoot.toPath())) {
                    files.filter(pathPredicate).forEach(partFile -> {
                        File root = roots.stream().filter(r -> partFile.startsWith(r.toPath())).findFirst().orElseThrow();

                        String cacheName = NodeFileTree.cacheName(partFile.getParent().toFile());

                        if (cacheName.equals(METASTORAGE_CACHE_NAME))
                            return;

                        int part = NodeFileTree.partId(partFile.toFile());

                        String[] cs = Arrays.stream(ccfgs())
                            .filter(ccfg -> CU.cacheOrGroupName(ccfg).equals(cacheName))
                            .findFirst().orElseThrow().getStoragePaths();

                        File expStorage = (idxPathUsed && part == INDEX_PARTITION)
                            ? snpRootF.apply(storagePath(IDX_PATH))
                            : snpRootF.apply(F.isEmpty(cs) ? null : cs[part % cs.length]);

                        assertEquals(expStorage, root);

                        snpFiles
                            .computeIfAbsent(root, r -> new HashMap<>())
                            .computeIfAbsent(cacheName, c -> new HashSet<>())
                            .add(part);
                    });
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            Set<String> seenGrps = new HashSet<>();

            Arrays.stream(ccfgs()).forEach(ccfg -> {
                String cname = CU.cacheOrGroupName(ccfg);

                if (seenGrps.contains(cname))
                    return;

                seenGrps.add(cname);

                Set<Integer> parts = new HashSet<>();

                List<String> storagePaths = new ArrayList<>(F.isEmpty(ccfg.getStoragePaths())
                    ? Collections.singletonList(null)
                    : Arrays.asList(ccfg.getStoragePaths()));

                if (!F.isEmpty(ccfg.getIndexPath()) && idxPartMustExistsInSnapshot())
                    storagePaths.add(ccfg.getIndexPath());

                for (String storagePath : storagePaths) {
                    File expRoot = snpRootF.apply(storagePath);

                    assertTrue(cname + " must be found", snpFiles.containsKey(expRoot));

                    parts.addAll(snpFiles.get(expRoot).get(cname));
                }

                assertFalse(cname + " partitions must be found", parts.isEmpty());

                partsMap.computeIfAbsent(CU.cacheGroupId(ccfg), key -> new HashSet<>()).addAll(parts);
            });

            assertEquals(grpCount(), seenGrps.size());
        }

        Arrays.stream(ccfgs()).forEach(ccfg -> {
            String cname = CU.cacheOrGroupName(ccfg);

            Set<Integer> parts = partsMap.get(CU.cacheGroupId(ccfg));

            assertFalse(cname + " partitions must be found", parts.isEmpty());

            assertEquals(
                "All partitions for " + cname + " must be found",
                PARTS_CNT + (idxPartMustExistsInSnapshot() ? 1 : 0),
                parts.size()
            );

            IntStream.range(0, PARTS_CNT).forEach(i -> assertTrue(i + " partition must be found", parts.contains(i)));

            if (idxPartMustExistsInSnapshot())
                assertTrue(parts.contains(INDEX_PARTITION));
        });
    }

    /** */
    private int grpCount() {
        return Arrays.stream(ccfgs()).map(CU::cacheOrGroupName).collect(Collectors.toSet()).size();
    }

    /** */
    protected boolean idxPartMustExistsInSnapshot() {
        return false;
    }
}
