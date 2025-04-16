package org.apache.ignite.compatibility.persistence;

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
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/**
 *
 */
@RunWith(Parameterized.class)
public abstract class IgniteNodeFileTreeCompatibilityAbstractTest extends IgniteCompatibilityAbstractTest {
    /** */
    protected static final String OLD_IGNITE_VERSION = Arrays.stream(IgniteReleasedVersion.values())
        .max(Comparator.comparing(IgniteReleasedVersion::version))
        .map(IgniteReleasedVersion::toString)
        .orElseThrow(() -> new IllegalStateException("Enum is empty"));

    /** */
    protected static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    protected static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    protected static final int BASE_CACHE_SIZE = 100;

    /** */
    protected static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    protected static final String CUSTOM_SNP_RELATIVE_PATH = "ex_snapshots";

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
    protected CacheGroupInfo cacheGrpInfo;

    /** */
    @Before
    public void setUp() {
        cacheGrpInfo = new CacheGroupInfo("test-cache", testCacheGrp ? 2 : 1);
    }

    /** */
    protected static String calcValue(String cacheName, int key) {
        return cacheName + "-organization-" + key;
    }

    /** */
    protected static String consId(boolean custom, int nodeIdx) {
        return custom ? "node-" + nodeIdx : null;
    }

    /** */
    protected static String customSnapshotPath(String workDirPath, String snpPath, boolean delIfExist) throws IgniteCheckedException {
        return U.resolveWorkDirectory(workDirPath, snpPath, delIfExist).getAbsolutePath();
    }

    /**
     * Configuration closure both for old and current Ignite version.
     */
    protected static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consId;

        /** */
        private final boolean incSnp;

        /** */
        private final boolean customSnpPath;

        /** */
        private final boolean delIfExist;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        private String workDirectory;

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            boolean customSnpPath,
            boolean delIfExist,
            CacheGroupInfo cacheGrpInfo
        ) throws IgniteCheckedException {
            this.consId = consId;
            this.incSnp = incSnp;
            this.customSnpPath = customSnpPath;
            this.delIfExist = delIfExist;
            this.cacheGrpInfo = cacheGrpInfo;
            workDirectory = U.defaultWorkDirectory();
        }

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            boolean customSnpPath,
            boolean delIfExist,
            CacheGroupInfo cacheGrpInfo,
            String workDirectory
        ) throws IgniteCheckedException {
            this(incSnp, consId, customSnpPath, delIfExist, cacheGrpInfo);

            this.workDirectory = workDirectory;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setWorkDirectory(workDirectory);

            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(consId);

            storageCfg.setWalCompactionEnabled(incSnp);

            if (delIfExist) {
                cfg.setCacheConfiguration(
                    cacheGrpInfo.cacheNamesList().stream()
                        .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                            .setGroupName(cacheGrpInfo.name())
                            .setAffinity(new RendezvousAffinityFunction(false, 10))
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }

            if (customSnpPath) {
                try {
                    cfg.setSnapshotPath(customSnapshotPath(workDirectory, CUSTOM_SNP_RELATIVE_PATH, delIfExist));
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Snapshot creating closure for old Ignite version.
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
        public List<String> cacheNamesList() {
            return cacheNames;
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
