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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class SnapshotFileTreeSelfTest {
    /** */
    private static final String TEST_CONSISTENT_ID = "node1";

    /** */
    public String workDir;

    /** */
    public boolean snpAbsPath;

    /** */
    @Before
    public void setUp() throws IgniteCheckedException {
        workDir = U.defaultWorkDirectory();
        snpAbsPath = false;
    }

    /** */
    @Test
    public void testNamedSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            "/path/to/storage2",
            "/path/to/storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            "/path/to/storage1/node1/",
            "/path/to/storage2/db/node1/",
            "/path/to/storage3/db/node1/"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            "/path/to/storage2/snapshots/snap/db/node1",
            "/path/to/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            "/path/to/storage2/snapshots/snap/db/node2",
            "/path/to/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            new String[]{
                "/path/to/storage2",
                "/path/to/storage3"
            },
            new String[]{
                "/extra_snp2",
                "/extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            "/extra_snp2/snapshots/snap/db/node1",
            "/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            "/extra_snp2/snapshots/snap/db/node2",
            "/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths2() {
        IgniteConfiguration cfg = config(
            "storage1",
            "storage2",
            "storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            workDir + "/storage1/node1",
            workDir + "/storage2/db/node1",
            workDir + "/storage3/db/node1"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            workDir + "/storage2/snapshots/snap/db/node1",
            workDir + "/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            workDir + "/storage2/snapshots/snap/db/node2",
            workDir + "/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths2_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "storage1",
            new String[]{
                "storage2",
                "storage3"
            },
            new String[]{
                "extra_snp2",
                "extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            workDir + "/extra_snp2/snapshots/snap/db/node1",
            workDir + "/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            workDir + "/extra_snp2/snapshots/snap/db/node2",
            workDir + "/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths3() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            "storage2",
            "storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            "/path/to/storage1/node1/",
            workDir + "/storage2/db/node1",
            workDir + "/storage3/db/node1"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            workDir + "/storage2/snapshots/snap/db/node1",
            workDir + "/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            workDir + "/storage2/snapshots/snap/db/node2",
            workDir + "/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths3_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            new String[]{
                "storage2",
                "storage3"
            },
            new String[]{
                "extra_snp2",
                "extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            workDir + "/extra_snp2/snapshots/snap/db/node1",
            workDir + "/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            workDir + "/extra_snp2/snapshots/snap/db/node2",
            workDir + "/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths4() {
        IgniteConfiguration cfg = config(
            "storage1",
            "/path/to/storage2",
            "/path/to/storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            workDir + "/storage1/node1/",
            "/path/to/storage2/db/node1/",
            "/path/to/storage3/db/node1/"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            "/path/to/storage2/snapshots/snap/db/node1",
            "/path/to/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            "/path/to/storage2/snapshots/snap/db/node2",
            "/path/to/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths4_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "storage1",
            new String[]{
                "/path/to/storage2",
                "/path/to/storage3"
            },
            new String[]{
                "/path/to/extra_snp2",
                "/path/to/extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            "/path/to/extra_snp2/snapshots/snap/db/node1",
            "/path/to/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            "/path/to/extra_snp2/snapshots/snap/db/node2",
            "/path/to/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths5() {
        IgniteConfiguration cfg = config(
            null,
            "/path/to/storage2",
            "/path/to/storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            workDir + "/db/node1/",
            "/path/to/storage2/db/node1/",
            "/path/to/storage3/db/node1/"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            "/path/to/storage2/snapshots/snap/db/node1",
            "/path/to/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            "/path/to/storage2/snapshots/snap/db/node2",
            "/path/to/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths5_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            null,
            new String[]{
                "/path/to/storage2",
                "/path/to/storage3"
            },
            new String[]{
                "/path/to/extra_snp2",
                "/path/to/extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            "/path/to/extra_snp2/snapshots/snap/db/node1",
            "/path/to/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            "/path/to/extra_snp2/snapshots/snap/db/node2",
            "/path/to/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths6() {
        IgniteConfiguration cfg = config(
            null,
            "storage2",
            "storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            workDir + "/db/node1/",
            workDir + "/storage2/db/node1/",
            workDir + "/storage3/db/node1/"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            workDir + "/storage2/snapshots/snap/db/node1",
            workDir + "/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            workDir + "/storage2/snapshots/snap/db/node2",
            workDir + "/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths6_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            null,
            new String[]{
                "storage2",
                "storage3"
            },
            new String[]{
                "extra_snp2",
                "extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            workDir + "/extra_snp2/snapshots/snap/db/node1",
            workDir + "/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            workDir + "/extra_snp2/snapshots/snap/db/node2",
            workDir + "/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths7() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            "storage2",
            "/path/to/storage3"
        );

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            "/path/to/storage1/node1/",
            workDir + "/storage2/db/node1",
            "/path/to/storage3/db/node1"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node1",
            workDir + "/storage2/snapshots/snap/db/node1",
            "/path/to/storage3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            workDir + "/snapshots/snap/db/node2",
            workDir + "/storage2/snapshots/snap/db/node2",
            "/path/to/storage3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testNamedSnapshotPaths7_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            new String[]{
                "storage2",
                "/path/to/storage3"
            },
            new String[]{
                "extra_snp2",
                "/path/to/extra_snp3"
            }
        );

        checkSnapshotDirs(
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node1",
            workDir + "/extra_snp2/snapshots/snap/db/node1",
            "/path/to/extra_snp3/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            fileTree(cfg),
            workDir + "/snapshots/snap/db/node2",
            workDir + "/extra_snp2/snapshots/snap/db/node2",
            "/path/to/extra_snp3/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testAbsoluteDeafultSnapshotPath() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            "/path/to/storage2",
            "/path/to/storage3"
        ).setSnapshotPath("/path/to/snapshots");

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotTempDirs(
            ft,
            "/path/to/storage1/node1/",
            "/path/to/storage2/db/node1/",
            "/path/to/storage3/db/node1/"
        );

        checkSnapshotDirs(
            cfg,
            ft,
            "/path/to/snapshots/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            "/path/to/snapshots/snap/db/node2"
        );
    }

    /** */
    @Test
    public void testAbsoluteDeafultSnapshotPath_extraSnapshotPaths() {
        IgniteConfiguration cfg = config(
            "/path/to/storage1",
            new String[]{
                "/path/to/storage2",
                "/path/to/storage3"
            },
            new String[]{
                "/extra_snp2",
                "/extra_snp3"
            }
        ).setSnapshotPath("/extra_snp");

        NodeFileTree ft = fileTree(cfg);

        checkSnapshotDirs(
            cfg,
            ft,
            "/extra_snp/snap/db/node1",
            "/extra_snp2/snap/db/node1",
            "/extra_snp3/snap/db/node1"
        );

        checkSnapshotDirs(
            "node2",
            cfg,
            ft,
            "/extra_snp/snap/db/node2",
            "/extra_snp2/snap/db/node2",
            "/extra_snp3/snap/db/node2"
        );
    }

    /** */
    private NodeFileTree fileTree(IgniteConfiguration cfg) {
        NodeFileTree ft = new NodeFileTree(cfg, TEST_CONSISTENT_ID);

        if (new File(cfg.getSnapshotPath()).isAbsolute())
            assertEquals(new File(cfg.getSnapshotPath()), ft.snapshotsRoot());
        else
            assertEquals(new File(workDir, cfg.getSnapshotPath()), ft.snapshotsRoot());

        return ft;
    }

    /** */
    private IgniteConfiguration config(String storagePath, String... extraPaths) {
        return new IgniteConfiguration()
            .setConsistentId(TEST_CONSISTENT_ID)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(storagePath)
                .setExtraStoragePaths(extraPaths)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** */
    private IgniteConfiguration config(String storagePath, String[] extraPaths, String[] extraSnpPaths) {
        return new IgniteConfiguration()
            .setConsistentId(TEST_CONSISTENT_ID)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(storagePath)
                .setExtraStoragePaths(extraPaths)
                .setExtraSnapshotPaths(extraSnpPaths)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** */
    private void checkSnapshotTempDirs(NodeFileTree ft, String... expStoragesStr) {
        List<File> expStorages = Arrays.stream(expStoragesStr).map(File::new).collect(Collectors.toList());
        Set<File> ftStorages = ft.allStorages().collect(Collectors.toSet());

        assertEquals(expStorages.size(), ftStorages.size());
        assertTrue(
            ftStorages + " must contains all " + expStorages,
            ftStorages.containsAll(expStorages)
        );

        List<File> snpTmps = ft.snapshotsTempRoots();

        assertEquals(expStorages.size(), snpTmps.size());
        assertTrue(snpTmps.containsAll(
            expStorages.stream().map(s -> new File(s, NodeFileTree.SNAPSHOT_TMP_DIR)).collect(Collectors.toList())));
    }

    /** */
    private void checkSnapshotDirs(IgniteConfiguration cfg, NodeFileTree ft, String... expSnpStoragesStr) {
        checkSnapshotDirs(ft.folderName(), cfg, ft, expSnpStoragesStr);
    }

    /** */
    private void checkSnapshotDirs(String folderName, IgniteConfiguration cfg, NodeFileTree ft, String... expSnpStoragesStr) {
        SnapshotFileTree sft = new SnapshotFileTree(cfg, ft, "snap", snpAbsPath ? "/snpdir" : null, folderName, TEST_CONSISTENT_ID);

        Set<File> snpStorages = sft.allStorages().collect(Collectors.toSet());
        List<File> expSnpStorages = Arrays.stream(expSnpStoragesStr).map(File::new).collect(Collectors.toList());

        assertEquals(expSnpStorages.size(), snpStorages.size());
        assertTrue(expSnpStorages + " must contains all " + snpStorages, expSnpStorages.containsAll(snpStorages));

        // With absolute path any configuration must return one snapshot dir.
        if (!snpAbsPath) {
            snpAbsPath = true;

            try {
                checkSnapshotDirs(
                    folderName,
                    cfg,
                    ft,
                    "/snpdir/snap/db/" + folderName
                );
            }
            finally {
                snpAbsPath = false;
            }
        }
    }
}
