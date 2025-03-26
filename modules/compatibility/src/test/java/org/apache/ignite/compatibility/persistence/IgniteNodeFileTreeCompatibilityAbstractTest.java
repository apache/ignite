package org.apache.ignite.compatibility.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public abstract class IgniteNodeFileTreeCompatibilityAbstractTest extends IgniteCompatibilityAbstractTest {
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

    /** */
    protected static final String OLD_IGNITE_VERSION = "2.16.0";

    /** */
    protected static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    protected static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    protected static final int BASE_CACHE_SIZE = 10_000;

    /** */
    protected static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /** */
    protected static final String CUSTOM_SNP_RELATIVE_PATH = "ex_snapshots";

    /** */
    protected static final String CONSISTENT_ID = "db3e5e20-91c1-4b2d-95c9-f7e5f7a0b8d3";

    /** */
    protected CacheGroupInfo cacheGrpInfo;

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId.
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222.
     * Also restoring cache dump and any kind of snapshot is pointless.
     */
    @Parameterized.Parameters(name = "incrementalSnp={0}, consistentID={1}, oldNodesCnt={2}, cacheDump={3}, customSnpPath={4}, testCacheGrp={5}")
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
    protected static class CacheGroupInfo {
        /** */
        private final String name;

        /** */
        private final List<String> cacheNames;

        /** */
        public CacheGroupInfo(String name, List<String> cacheNames) {
            this.name = name;
            this.cacheNames = Collections.unmodifiableList(cacheNames);
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public List<String> getCacheNamesList() {
            return cacheNames;
        }

        /** */
        public Set<String> getCacheNamesSet() {
            return Set.copyOf(cacheNames);
        }
    }
}
