package org.apache.ignite.compatibility.persistence;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.runners.Parameterized.Parameter;

/** */
public abstract class SnapshotCompatibilityAbstractTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    public static final String OLD_IGNITE_VERSION = Collections.max(
        Arrays.asList(IgniteReleasedVersion.values()),
        Comparator.comparing(IgniteReleasedVersion::version)
    ).toString();

    /** */
    public static final String OLD_WORK_DIR;

    static {
        try {
            OLD_WORK_DIR = String.format("%s-%s", U.defaultWorkDirectory(), OLD_IGNITE_VERSION);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    public static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    public static final int BASE_CACHE_SIZE = 100;

    /** */
    public static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    @Parameter
    public boolean customConsId;

    /** */
    @Parameter(1)
    public boolean customSnpDir;

    /** */
    @Parameter(2)
    public boolean testCacheGrp;

    /** */
    protected final CacheGroupInfo cacheGrpInfo = new CacheGroupInfo("test-cache", testCacheGrp ? 2 : 1);

    /** */
    public String snpDir(String workDirPath, boolean delIfExist) throws IgniteCheckedException {
        return U.resolveWorkDirectory(workDirPath, customSnpDir ? "ex_snapshots" : "snapshots", delIfExist).getAbsolutePath();
    }

    /** */
    public String snpPath(String workDirPath, String snpName, boolean delIfExist) throws IgniteCheckedException {
        return Paths.get(snpDir(workDirPath, delIfExist), snpName).toString();
    }

    /** */
    public String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }

    /**
     * Configuration closure both for old and current Ignite version.
     */
    protected static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final boolean incSnp;

        /** */
        private final String consId;

        /** */
        private final String snpDir;

        /** */
        private final boolean delIfExist;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        private String workDir;

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            String snpDir,
            boolean delIfExist,
            CacheGroupInfo cacheGrpInfo
        ) throws IgniteCheckedException {
            this.incSnp = incSnp;
            this.consId = consId;
            this.snpDir = snpDir;
            this.delIfExist = delIfExist;
            this.cacheGrpInfo = cacheGrpInfo;
            workDir = U.defaultWorkDirectory();
        }

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            String snpDir,
            boolean delIfExist,
            CacheGroupInfo cacheGrpInfo,
            String workDir
        ) throws IgniteCheckedException {
            this(incSnp, consId, snpDir, delIfExist, cacheGrpInfo);

            this.workDir = workDir;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setWorkDirectory(workDir);

            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(consId);

            storageCfg.setWalCompactionEnabled(incSnp);

            if (delIfExist) {
                cfg.setCacheConfiguration(
                    cacheGrpInfo.cacheNames().stream()
                        .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                            .setGroupName(cacheGrpInfo.name())
                            .setAffinity(new RendezvousAffinityFunction(false, 10))
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }

            cfg.setSnapshotPath(snpDir);
        }
    }

    /**
     * Snapshot creating closure both for old and current Ignite version.
     */
    protected static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean incSnp;

        /** */
        private final boolean cacheDump;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public CreateSnapshotClosure(boolean incSnp, boolean cacheDump, CacheGroupInfo cacheGrpInfo) {
            this.incSnp = incSnp;
            this.cacheDump = cacheDump;
            this.cacheGrpInfo = cacheGrpInfo;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheGrpInfo.addItemsToCacheGrp(ign, 0, BASE_CACHE_SIZE);

            if (cacheDump)
                ign.snapshot().createDump(CACHE_DUMP_NAME, Collections.singleton(cacheGrpInfo.name())).get();
            else
                ign.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            if (incSnp) {
                cacheGrpInfo.addItemsToCacheGrp(ign, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT);

                ign.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }
        }
    }

    /** */
    protected static class CacheGroupInfo {
        /** */
        private final String name;

        /** */
        private final List<String> cacheNames;

        /** */
        public CacheGroupInfo(String name, int cachesCnt) {
            this.name = name;

            cacheNames = new ArrayList<>();

            for (int i = 0; i < cachesCnt; ++i)
                cacheNames.add("test-cache-" + i);
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public List<String> cacheNames() {
            return cacheNames;
        }

        /** */
        public static String calcValue(String cacheName, int key) {
            return cacheName + "-organization-" + key;
        }

        /** */
        public void addItemsToCacheGrp(Ignite ign, int startIdx, int cnt) {
            for (String cacheName : cacheNames)
                addItemsToCache(ign.cache(cacheName), startIdx, cnt);
        }

        /** */
        private void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, calcValue(cache.getName(), i));
        }

        /** */
        public void checkCaches(Ignite ign, int expectedCacheSize) {
            for (String cacheName : cacheNames) {
                IgniteCache<Integer, String> cache = ign.cache(cacheName);

                assertNotNull(cache);

                checkCache(cache, expectedCacheSize);
            }
        }

        /** */
        private void checkCache(IgniteCache<Integer, String> cache, int expectedSize) {
            assertEquals(expectedSize, cache.size());

            for (int i = 0; i < expectedSize; ++i)
                assertEquals(calcValue(cache.getName(), i), cache.get(i));
        }
    }
}
