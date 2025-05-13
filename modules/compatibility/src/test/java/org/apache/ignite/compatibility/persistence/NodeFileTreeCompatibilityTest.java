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

import org.apache.commons.io.FileUtils;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assume.assumeFalse;

/** */
public class NodeFileTreeCompatibilityTest extends NodeFileTreeCompatibilityAbstractTest {
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
    @Test
    public void testNodeFileTree() throws Exception {
        if (incSnp)
            assumeFalse(INCREMENTAL_SNAPSHOTS_FOR_CACHE_DUMP_NOT_SUPPORTED, cacheDump);

        final String oldWorkDir = String.format("%s-%s", U.defaultWorkDirectory(), OLD_IGNITE_VERSION);

        try {
            ArrayList<IgniteEx> oldNodes = new ArrayList<>(nodesCnt);
            ArrayList<IgniteEx> curNodes = new ArrayList<>(nodesCnt);

            for (int i = 1; i < nodesCnt; ++i) {
                oldNodes.add(
                    startGrid(
                        i,
                        OLD_IGNITE_VERSION,
                        new ConfigurationClosure(incSnp, consId(i), customSnpPath, true, cacheGrpInfo, oldWorkDir)
                    )
                );
            }

            oldNodes.add(
                startGrid(
                    nodesCnt,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, consId(nodesCnt), customSnpPath, true, cacheGrpInfo, oldWorkDir),
                    new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo)
                )
            );

            stopAllGrids();

            for (int i = 0; i < nodesCnt; ++i) {
                curNodes.add(
                    startGrid(
                        i,
                        new ConfigurationClosure(incSnp, consId(i), customSnpPath, true, cacheGrpInfo)::apply
                    )
                );
            }

            new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo).apply(curNodes.get(0));

            assertEquals(scanFileTree(oldWorkDir, ".bin"), scanFileTree(U.defaultWorkDirectory(), ".bin"));

            if (cacheDump) {
                assertEquals(
                    scanFileTree(snpPath(oldWorkDir, CACHE_DUMP_NAME), ".dump"),
                    scanFileTree(snpPath(U.defaultWorkDirectory(), CACHE_DUMP_NAME), ".dump")
                );
            }
            else {
                assertEquals(
                    scanSnp(snpPath(oldWorkDir, SNAPSHOT_NAME)),
                    scanSnp(snpPath(U.defaultWorkDirectory(), SNAPSHOT_NAME))
                );
            }
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir(false);

            FileUtils.deleteDirectory(new File(oldWorkDir));
        }
    }

    /** */
    private SnpScanResult scanSnp(String snpPath) {
        File incsDir = new File(snpPath, "increments");

        return new SnpScanResult(incsDir.exists() ? incsDir.list().length : 0, scanFileTree(snpPath, ".bin"));
    }

    /** */
    private Map<String, CacheGrpScanResult> scanFileTree(String rootPath, String partPostfix) {
        Map<String, CacheGrpScanResult> res = new HashMap<>();

        File dbDir = new File(rootPath, "db");

        for (File child : dbDir.listFiles()) {
            if (child.getName().startsWith("node") && !"cache-ignite-sys-cache".equals(child.getName())) {
                List<CacheGrpScanResult> cacheGrpScans = scanNode(child, partPostfix);

                for (CacheGrpScanResult cacheGrpScan : cacheGrpScans) {
                    res.merge(
                        cacheGrpScan.cacheGrpName(),
                        cacheGrpScan,
                        (to, from) -> {
                            to.cacheNames().addAll(from.cacheNames());

                            to.partNames().addAll(from.partNames());

                            return to;
                        }
                    );
                }
            }
        }

        return res;
    }

    /** */
    private List<CacheGrpScanResult> scanNode(File nodeDir, String partPostfix) {
        assertTrue(nodeDir.isDirectory());

        List<CacheGrpScanResult> res = new ArrayList<>();

        for (File child : nodeDir.listFiles())
            if (child.getName().startsWith("cache"))
                res.add(scanCacheGrp(child, partPostfix));

        return res;
    }

    /** */
    private CacheGrpScanResult scanCacheGrp(File cacheGrpDir, String partPostfix) {
        assertTrue(cacheGrpDir.isDirectory());

        String cacheGrpNamePrefix = cacheGrpDir.getName().startsWith("cacheGroup") ? "cacheGroup" : "cache";
        String cacheGrpName = cacheGrpDir.getName().substring(cacheGrpNamePrefix.length() + 1);

        CacheGrpScanResult res = new CacheGrpScanResult(cacheGrpName);

        for (String childFileName : cacheGrpDir.list()) {
            if (childFileName.endsWith("cache_data.dat")) {
                String cacheName = childFileName.substring(0, childFileName.length() - "cache_data.dat".length());
                res.addCacheName(cacheName);
            }

            if (childFileName.startsWith("part") && childFileName.endsWith(partPostfix))
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
