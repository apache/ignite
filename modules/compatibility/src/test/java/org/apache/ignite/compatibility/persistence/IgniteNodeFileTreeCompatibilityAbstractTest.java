package org.apache.ignite.compatibility.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

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
    protected static final String CONSISTENT_ID = UUID.randomUUID().toString();

    /** */
    @Parameter
    public boolean incSnp;

    /** */
    @Parameter(1)
    @Nullable public String consId;

    /** */
    @Parameter(2)
    public int oldNodesCnt;

    /** */
    @Parameter(3)
    public boolean cacheDump;

    /** */
    @Parameter(4)
    public boolean customSnpPath;

    /** */
    @Parameter(5)
    public boolean testCacheGrp;

    /** */
    protected CacheGroupInfo cacheGrpInfo;

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId.
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222.
     * Also restoring cache dump and any kind of snapshot is pointless.
     */
    @Parameters(name = "incrementalSnp={0}, consistentID={1}, oldNodesCnt={2}, cacheDump={3}, customSnpPath={4}, testCacheGrp={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (boolean incSnp : Arrays.asList(true, false))
            for (String consId : Arrays.asList(CONSISTENT_ID, null))
                for (int oldNodesCnt : Arrays.asList(1, 3))
                    for (boolean cacheDump : Arrays.asList(true, false))
                        for (boolean customSnpPath : Arrays.asList(true, false))
                            for (boolean testCacheGrp : Arrays.asList(true, false))
                                if ((!incSnp || !cacheDump) && (!incSnp || consId != null))
                                    data.add(new Object[]{incSnp, consId, oldNodesCnt, cacheDump, customSnpPath, testCacheGrp});

        return data;
    }

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
