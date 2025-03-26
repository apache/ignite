package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/** */
public class NodeFileTreeTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = "2.16.0";

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    private static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    private static final int BASE_CACHE_SIZE = 10_000;

    /** */
    private static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /** */
    private static final String CUSTOM_SNP_RELATIVE_PATH = "ex_snapshots";

    /** */
    private static final String CONSISTENT_ID = "db3e5e20-91c1-4b2d-95c9-f7e5f7a0b8d3";

    /** */
    private CacheGroupInfo cacheGrpInfo;

    /** */
    private class CacheGroupInfo {
        /** */
        public CacheGroupInfo(String name, List<String> cacheNames) {
            this.name = name;
            this.cacheNames = cacheNames;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public List<String> getCacheNamesList() {
            return Collections.unmodifiableList(cacheNames);
        }

        /** */
        public Set<String> getCacheNamesSet() {
            return Set.copyOf(cacheNames);
        }

        /** */
        private final String name;

        /** */
        private final List<String> cacheNames;
    }

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId.
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222.
     * Also restoring cache dump and any kind of snapshot is pointless.
     */
    @Parameters(name = "incrementalSnp={0}, consistentID={1}, oldNodesCnt={2}, cacheDump={3}, customSnpPath={4}, testCacheGrp={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        List<Boolean> incrementalSnpValues = Arrays.asList(true, false);
        List<String> consistentIdValues = Arrays.asList(CONSISTENT_ID, null);
        List<Integer> oldNodesCntValues = Arrays.asList(1, 3);
        List<Boolean> createDumpValues = Arrays.asList(true, false);
        List<Boolean> customSnpPathValues = Arrays.asList(true, false);
        List<Boolean> cachesCntValues = Arrays.asList(true, false);

        for (Boolean incrementalSnp : incrementalSnpValues)
            for (String consistentId : consistentIdValues)
                for (Integer oldNodesCnt : oldNodesCntValues)
                    for (Boolean cacheDump : createDumpValues)
                        for (Boolean customSnpPath : customSnpPathValues)
                            for (Boolean testCacheGrp : cachesCntValues)
                                if ((!incrementalSnp || !cacheDump) && (!incrementalSnp || consistentId != null))
                                    data.add(
                                        new Object[]{incrementalSnp, consistentId, oldNodesCnt, cacheDump, customSnpPath, testCacheGrp}
                                    );

        return data;
    }

    /** */
    @Before
    public void setUp() {
        final int cachesCnt = testCacheGrp ? 2 : 1;

        List<String> cacheNames = new ArrayList<>();

        for (int i = 0; i < cachesCnt; ++i)
            cacheNames.add("test-cache-" + i);

        cacheGrpInfo = new CacheGroupInfo("test-cache", cacheNames);
    }

    /** */
    @Parameterized.Parameter
    public boolean incrementalSnp;

    /** */
    @Parameterized.Parameter(1)
    @Nullable public String consistentId;

    /** */
    @Parameterized.Parameter(2)
    public int oldNodesCnt;

    /** */
    @Parameterized.Parameter(3)
    public boolean cacheDump;

    /** */
    @Parameterized.Parameter(4)
    public boolean customSnpPath;

    /** */
    @Parameterized.Parameter(5)
    public boolean testCacheGrp;

    @Test
    public void testNodeFileTree() {

    }

    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /** */
        private final boolean incrementalSnp;

        /** */
        private final boolean customSnpPath;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public ConfigurationClosure(
            boolean incrementalSnp,
            String consistentId,
            boolean customSnpPath,
            boolean forSnapshotTake,
            CacheGroupInfo cacheGrpInfo
        ) {
            this.consistentId = consistentId;
            this.incrementalSnp = incrementalSnp;
            this.customSnpPath = customSnpPath;
            this.forSnapshotTake = forSnapshotTake;
            this.cacheGrpInfo = cacheGrpInfo;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration igniteConfiguration) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            igniteConfiguration.setDataStorageConfiguration(storageCfg);

            igniteConfiguration.setConsistentId(consistentId);

            if (incrementalSnp)
                storageCfg.setWalCompactionEnabled(true);

            igniteConfiguration.setCacheConfiguration(
                cacheGrpInfo.getCacheNamesList().stream()
                    .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName).setGroupName(cacheGrpInfo.getName()))
                    .toArray(CacheConfiguration[]::new)
            );

            if (customSnpPath) {
                try {
                    igniteConfiguration.setSnapshotPath(getCustomSnapshotPath(CUSTOM_SNP_RELATIVE_PATH, true));
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    /** */
    private static String getCustomSnapshotPath(String relativePath, boolean forSnapshotTake) throws IgniteCheckedException {
        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), relativePath, forSnapshotTake);

        return exSnpDir.getAbsolutePath();
    }

    /** */
    private static void addItemsToCacheGrp(Ignite ign, CacheGroupInfo cacheGrpInfo, int startIdx, int cnt) {
        for (String cacheName : cacheGrpInfo.getCacheNamesList())
            addItemsToCache(ign.cache(cacheName), startIdx, cnt);
    }

    /** */
    private static void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < startIdx + cnt; ++i)
            cache.put(i, calcValue(cache.getName(), i));
    }

    /** */
    private static String calcValue(String cacheName, int key) {
        return cacheName + "-organization-" + key;
    }
}
