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

package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** */
@RunWith(Parameterized.class)
public class NodeFileTreeCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = Collections.max(
        Arrays.asList(IgniteReleasedVersion.values()),
        Comparator.comparing(IgniteReleasedVersion::version)
    ).toString();

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    private static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    private static final int BASE_CACHE_SIZE = 100;

    /** */
    private static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    private static final String SNP_PART_SUFFIX = ".bin";

    /** */
    private static final String DUMP_PART_SUFFIX = ".dump";

    /** */
    private static final String CACHE_DATA_SUFFIX = "cache_data.dat";

    /** */
    private static final String CACHE_GROUP_PREFIX = "cacheGroup";

    /** */
    private static final String CACHE_PREFIX = "cache";

    /** */
    private static final String NODE_PREFIX = "node";

    /** */
    private static final String PART_PREFIX = "part";

    /** */
    private static final String DB_FOLDER = "db";

    /** */
    private static final String INCREMENTS_FOLDER = "increments";

    /** */
    private static final String DFLT_SNAPSHOTS_FOLDER = "snapshots";

    /** */
    private static final String EX_SNAPSHOTS_FOLDER = "ex_snapshots";

    /** */
    private static final Map<String, String> cacheToGrp = Map.of(
        "singleCache", "singleCache",
        "testCache1", "testCacheGrp",
        "testCache2", "testCacheGrp"
    );

    /** */
    @Parameter
    public boolean customConsId;

    /** */
    @Parameter(1)
    public int nodesCnt;

    /** */
    private String oldWorkDir;

    /** */
    @Parameters(name = "customConsId={0}, nodesCnt={1}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            List.of(true, false),
            List.of(1, 3)
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        oldWorkDir = String.format("%s-%s", U.defaultWorkDirectory(), OLD_IGNITE_VERSION);
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();

        U.delete(Path.of(oldWorkDir));

        snpPath(U.defaultWorkDirectory(), EX_SNAPSHOTS_FOLDER, true);
    }

    /** */
    @Test
    public void testNodeFileTree() throws Exception {
        for (int i = 1; i <= nodesCnt; ++i)
            startGrid(i, OLD_IGNITE_VERSION, new ConfigurationClosure(i, oldWorkDir), i == nodesCnt ? new CreateSnapshotClosure() : null);

        stopAllGrids();

        List<IgniteEx> curNodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; ++i)
            curNodes.add(startGrid(i, new ConfigurationClosure(i, U.defaultWorkDirectory())::apply));

        new CreateSnapshotClosure().apply(curNodes.get(0));

        assertEquals(
            scanFileTree(Path.of(oldWorkDir), SNP_PART_SUFFIX),
            scanFileTree(Path.of(U.defaultWorkDirectory()), SNP_PART_SUFFIX)
        );

        assertEquals(
            scanCacheDump(snpPath(oldWorkDir, DFLT_SNAPSHOTS_FOLDER, false)),
            scanCacheDump(snpPath(U.defaultWorkDirectory(), DFLT_SNAPSHOTS_FOLDER, false))
        );

        for (String snpFolder : List.of(DFLT_SNAPSHOTS_FOLDER, EX_SNAPSHOTS_FOLDER)) {
            assertEquals(
                scanSnp(snpPath(oldWorkDir, snpFolder, false)),
                scanSnp(snpPath(U.defaultWorkDirectory(), snpFolder, false))
            );
        }
    }

    /** */
    private static Path snpPath(String workDir, String path, boolean delIfExists) {
        try {
            return Path.of(U.resolveWorkDirectory(workDir, path, delIfExists).getAbsolutePath());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private SnpScanResult scanSnp(Path snpPath) throws IOException {
        Path snpFolder = snpPath.resolve(SNAPSHOT_NAME);

        Path incsDir = snpFolder.resolve(INCREMENTS_FOLDER);

        int incsCnt = 0;

        if (Files.isDirectory(incsDir)) {
            try (Stream<Path> stream = Files.list(incsDir)) {
                incsCnt = (int)stream.count();
            }
        }

        return new SnpScanResult(incsCnt, scanFileTree(snpFolder, SNP_PART_SUFFIX));
    }

    /** */
    private Map<String, CacheGroupScanResult> scanCacheDump(Path snpPath) throws IOException {
        return scanFileTree(snpPath.resolve(CACHE_DUMP_NAME), DUMP_PART_SUFFIX);
    }

    /** */
    private Map<String, CacheGroupScanResult> scanFileTree(Path root, String partSuffix) throws IOException {
        Map<String, CacheGroupScanResult> res = new HashMap<>();

        try (DirectoryStream<Path> nodes = Files.newDirectoryStream(root.resolve(DB_FOLDER), NODE_PREFIX + "*")) {
            for (Path nodeDir : nodes) {
                for (CacheGroupScanResult scan : scanNode(nodeDir, partSuffix))
                    res.merge(scan.cacheGrpName(), scan, CacheGroupScanResult::merge);
            }
        }

        return res;
    }

    /** */
    private List<CacheGroupScanResult> scanNode(Path nodeDir, String partSuffix) throws IOException {
        assertTrue(Files.isDirectory(nodeDir));

        List<CacheGroupScanResult> res = new ArrayList<>();

        try (DirectoryStream<Path> caches = Files.newDirectoryStream(nodeDir, CACHE_PREFIX + '*')) {
            for (Path cacheDir : caches) {
                if (Files.isDirectory(cacheDir))
                    res.add(scanCacheGrp(cacheDir, partSuffix));
            }
        }

        return res;
    }

    /** */
    private CacheGroupScanResult scanCacheGrp(Path cacheGrpDir, String partSuffix) throws IOException {
        assertTrue(Files.isDirectory(cacheGrpDir));

        String cacheGrpDirName = cacheGrpDir.getFileName().toString();
        String cacheGrpNamePrefix = cacheGrpDirName.startsWith(CACHE_GROUP_PREFIX) ? CACHE_GROUP_PREFIX : CACHE_PREFIX;
        String cacheGrpName = cacheGrpDirName.substring(cacheGrpNamePrefix.length() + 1);

        CacheGroupScanResult res = new CacheGroupScanResult(cacheGrpName);

        try (DirectoryStream<Path> files = Files.newDirectoryStream(cacheGrpDir)) {
            for (Path file : files) {
                String name = file.getFileName().toString();

                if (name.endsWith(CACHE_DATA_SUFFIX)) {
                    String cacheName = name.substring(0, name.length() - CACHE_DATA_SUFFIX.length());
                    res.addCacheName(cacheName);
                }

                if (name.startsWith(PART_PREFIX) && name.endsWith(partSuffix))
                    res.addPartName(name);
            }
        }

        return res;
    }

    /** */
    private static class SnpScanResult {
        /** */
        private final int incsCnt;

        /** */
        private final Map<String, CacheGroupScanResult> cacheGrpScans;

        /** */
        public SnpScanResult(int incsCnt, Map<String, CacheGroupScanResult> cacheGrpScans) {
            this.incsCnt = incsCnt;

            this.cacheGrpScans = cacheGrpScans;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof SnpScanResult))
                return false;

            SnpScanResult other = (SnpScanResult)o;

            return incsCnt == other.incsCnt && Objects.equals(cacheGrpScans, other.cacheGrpScans);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(incsCnt, cacheGrpScans);
        }
    }

    /** */
    private static class CacheGroupScanResult {
        /** */
        private final String cacheGrpName;

        /** */
        private final Set<String> cacheNames = new HashSet<>();

        /** */
        private final Set<String> partNames = new HashSet<>();

        /** */
        public CacheGroupScanResult(String cacheGrpName) {
            this.cacheGrpName = cacheGrpName;
        }

        /** */
        public String cacheGrpName() {
            return cacheGrpName;
        }

        /** */
        public void addCacheName(String cacheName) {
            cacheNames.add(cacheName);
        }

        /** */
        public void addPartName(String partName) {
            partNames.add(partName);
        }

        /** */
        public CacheGroupScanResult merge(CacheGroupScanResult that) {
            assert Objects.equals(cacheGrpName(), that.cacheGrpName());

            cacheNames.addAll(that.cacheNames);
            partNames.addAll(that.partNames);

            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (!(o instanceof CacheGroupScanResult)) return false;

            CacheGroupScanResult other = (CacheGroupScanResult)o;

            return Objects.equals(cacheGrpName, other.cacheGrpName) &&
                Objects.equals(cacheNames, other.cacheNames) &&
                Objects.equals(partNames, other.partNames);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(cacheGrpName, cacheNames, partNames);
        }
    }

    /** Snapshot creating closure both for old and current Ignite version. */
    private static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheToGrp.forEach((key, value) -> {
                IgniteCache<Integer, String> cache = ign.createCache(new CacheConfiguration<Integer, String>(key)
                    .setGroupName(Objects.equals(key, value) ? null : value)
                    .setAffinity(new RendezvousAffinityFunction(false, 10)));

                addItemsToCache(cache, 0, BASE_CACHE_SIZE);
            });

            List<String> snpPaths = Arrays.asList(
                null,
                snpPath(ign.configuration().getWorkDirectory(), EX_SNAPSHOTS_FOLDER, true).toString()
            );

            for (String snpPath : snpPaths)
                ((IgniteEx)ign).context().cache().context().snapshotMgr().createSnapshot(SNAPSHOT_NAME, snpPath, false, false).get();

            ign.snapshot().createDump(CACHE_DUMP_NAME, cacheToGrp.values()).get();

            // Incremental snapshots require same consistentID
            // https://issues.apache.org/jira/browse/IGNITE-25096
            if (ign.configuration().getConsistentId() != null && ign.cluster().nodes().size() == 1) {
                cacheToGrp.keySet().forEach(
                    cacheName -> addItemsToCache(ign.cache(cacheName), BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT)
                );

                for (String snpPath : snpPaths)
                    ((IgniteEx)ign).context().cache().context().snapshotMgr().createSnapshot(SNAPSHOT_NAME, snpPath, true, false).get();
            }
        }

        /** */
        private static void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, cache.getName() + "-organization-" + i);
        }
    }

    /** Configuration closure both for old and current Ignite version. */
    private class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final int nodeIdx;

        /** */
        private final String workDir;

        /** */
        public ConfigurationClosure(int nodeIdx, String workDir) {
            this.nodeIdx = nodeIdx;
            this.workDir = workDir;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(customConsId ? "node-" + nodeIdx : null);

            storageCfg.setWalCompactionEnabled(true);

            cfg.setWorkDirectory(workDir);
        }
    }
}
