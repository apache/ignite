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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.io.FileUtils;
import org.apache.ignite.compatibility.persistence.CompatibilityTestCore.ConfigurationClosure;
import org.apache.ignite.compatibility.persistence.CompatibilityTestCore.CreateSnapshotClosure;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.CACHE_DUMP_NAME;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.OLD_IGNITE_VERSION;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.SNAPSHOT_NAME;
import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
public class NodeFileTreeCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String SNP_PART_SUFFIX = ".bin";

    /** */
    private static final String DUMP_PART_SUFFIX = ".dump";

    /** */
    public static final String CACHE_DATA_SUFFIX = "cache_data.dat";

    /** */
    public static final String CACHE_GROUP_PREFIX = "cacheGroup";

    /** */
    public static final String CACHE_PREFIX = "cache";

    /** */
    @Parameter
    public boolean incSnp;

    /** */
    @Parameter(1)
    public boolean customConsId;

    /** */
    @Parameter(2)
    public boolean cacheDump;

    /** */
    @Parameter(3)
    public boolean customSnpPath;

    /** */
    @Parameter(4)
    public boolean testCacheGrp;

    /** */
    @Parameter(5)
    public int nodesCnt;

    /** */
    @Parameters(name = "incSnp={0}, customConsId={1}, cacheDump={2}, customSnpPath={3}, testCacheGrp={4}, nodesCnt={5}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(1, 3)
        );
    }

    /** */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir(false);
    }

    /** */
    @Test
    public void testNodeFileTree() throws Exception {
        assumeFalse(CompatibilityTestCore.INCREMENTAL_SNAPSHOTS_FOR_CACHE_DUMP_NOT_SUPPORTED, incSnp && cacheDump);

        CompatibilityTestCore core = new CompatibilityTestCore(customConsId, customSnpPath, testCacheGrp);

        final String oldWorkDir = String.format("%s-%s", U.defaultWorkDirectory(), OLD_IGNITE_VERSION);

        try {
            for (int i = 1; i <= nodesCnt; ++i) {
                startGrid(
                    i,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, core.consId(i), customSnpPath, true, core.cacheGrpInfo(), oldWorkDir),
                    i == nodesCnt ? new CreateSnapshotClosure(incSnp, cacheDump, core.cacheGrpInfo()) : null
                );
            }

            stopAllGrids();

            List<IgniteEx> curNodes = new ArrayList<>(nodesCnt);

            for (int i = 0; i < nodesCnt; ++i) {
                curNodes.add(
                    startGrid(
                        i,
                        new ConfigurationClosure(incSnp, core.consId(i), customSnpPath, true, core.cacheGrpInfo())::apply
                    )
                );
            }

            new CreateSnapshotClosure(incSnp, cacheDump, core.cacheGrpInfo()).apply(curNodes.get(0));

            assertEquals(scanFileTree(oldWorkDir, SNP_PART_SUFFIX), scanFileTree(U.defaultWorkDirectory(), SNP_PART_SUFFIX));

            if (cacheDump) {
                assertEquals(
                    scanFileTree(core.snpPath(oldWorkDir, CACHE_DUMP_NAME, false), DUMP_PART_SUFFIX),
                    scanFileTree(core.snpPath(U.defaultWorkDirectory(), CACHE_DUMP_NAME, false), DUMP_PART_SUFFIX)
                );
            }
            else {
                assertEquals(
                    scanSnp(core.snpPath(oldWorkDir, SNAPSHOT_NAME, false)),
                    scanSnp(core.snpPath(U.defaultWorkDirectory(), SNAPSHOT_NAME, false))
                );
            }
        }
        finally {
            FileUtils.deleteDirectory(new File(oldWorkDir));
        }
    }

    /** */
    private SnpScanResult scanSnp(String snpPath) {
        File incsDir = new File(snpPath, "increments");

        int incsCnt = incsDir.exists() ? incsDir.list().length : 0;

        return new SnpScanResult(incsCnt, scanFileTree(snpPath, SNP_PART_SUFFIX));
    }

    /** */
    private Map<String, CacheGrpScanResult> scanFileTree(String rootPath, String partSuffix) {
        Map<String, CacheGrpScanResult> res = new HashMap<>();

        File dbDir = new File(rootPath, "db");

        BiFunction<CacheGrpScanResult, CacheGrpScanResult, CacheGrpScanResult> mergeScans = (to, from) -> {
            to.cacheNames().addAll(from.cacheNames());
            to.partNames().addAll(from.partNames());

            return to;
        };

        for (File child : dbDir.listFiles()) {
            if (child.getName().startsWith("node"))
                scanNode(child, partSuffix).forEach(scan -> res.merge(scan.cacheGrpName(), scan, mergeScans));
        }

        return res;
    }

    /** */
    private List<CacheGrpScanResult> scanNode(File nodeDir, String partSuffix) {
        assertTrue(nodeDir.isDirectory());

        List<CacheGrpScanResult> res = new ArrayList<>();

        for (File child : nodeDir.listFiles())
            if (child.getName().startsWith(CACHE_PREFIX))
                res.add(scanCacheGrp(child, partSuffix));

        return res;
    }

    /** */
    private CacheGrpScanResult scanCacheGrp(File cacheGrpDir, String partSuffix) {
        assertTrue(cacheGrpDir.isDirectory());

        String cacheGrpNamePrefix = cacheGrpDir.getName().startsWith(CACHE_GROUP_PREFIX) ? CACHE_GROUP_PREFIX : CACHE_PREFIX;
        String cacheGrpName = cacheGrpDir.getName().substring(cacheGrpNamePrefix.length() + 1);

        CacheGrpScanResult res = new CacheGrpScanResult(cacheGrpName);

        for (String childFileName : cacheGrpDir.list()) {
            if (childFileName.endsWith(CACHE_DATA_SUFFIX)) {
                String cacheName = childFileName.substring(0, childFileName.length() - CACHE_DATA_SUFFIX.length());
                res.addCacheName(cacheName);
            }

            if (childFileName.startsWith("part") && childFileName.endsWith(partSuffix))
                res.addPartName(childFileName);
        }

        return res;
    }

    /** */
    private static class SnpScanResult {
        /** */
        private final int incsCnt;

        /** */
        private final Map<String, CacheGrpScanResult> cacheGrpScans;

        /** */
        public SnpScanResult(int incsCnt, Map<String, CacheGrpScanResult> cacheGrpScans) {
            this.incsCnt = incsCnt;

            this.cacheGrpScans = cacheGrpScans;
        }

        /** */
        public int incsCnt() {
            return incsCnt;
        }

        /** */
        public Map<String, CacheGrpScanResult> cacheGrpScans() {
            return cacheGrpScans;
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
    }

    /** */
    private static class CacheGrpScanResult {
        /** */
        private final String cacheGrpName;

        /** */
        private final Set<String> cacheNames = new HashSet<>();

        /** */
        private final Set<String> partNames = new HashSet<>();

        /** */
        public CacheGrpScanResult(String cacheGrpName) {
            this.cacheGrpName = cacheGrpName;
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
        public String cacheGrpName() {
            return cacheGrpName;
        }

        /** */
        public Set<String> cacheNames() {
            return cacheNames;
        }

        /** */
        public Set<String> partNames() {
            return partNames;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (!(o instanceof CacheGrpScanResult)) return false;

            CacheGrpScanResult other = (CacheGrpScanResult)o;

            return Objects.equals(cacheGrpName, other.cacheGrpName) &&
                Objects.equals(cacheNames, other.cacheNames) &&
                Objects.equals(partNames, other.partNames);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(cacheGrpName, cacheNames, partNames);
        }
    }
}
