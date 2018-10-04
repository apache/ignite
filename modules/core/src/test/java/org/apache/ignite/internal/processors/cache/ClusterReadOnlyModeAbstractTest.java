package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class ClusterReadOnlyModeAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 3;

    /** Replicated atomic cache. */
    private static final String REPL_ATOMIC_CACHE = "repl_atomic_cache";

    /** Replicated transactional cache. */
    private static final String REPL_TX_CACHE = "repl_tx_cache";

    /** Partitioned atomic cache. */
    private static final String PART_ATOMIC_CACHE = "part_atomic_cache";

    /** Partitioned transactional cache. */
    private static final String PART_TX_CACHE = "part_tx_cache";

    /** Cache names. */
    protected static final Collection<String> CACHE_NAMES = F.asList(REPL_ATOMIC_CACHE, REPL_TX_CACHE,
        PART_ATOMIC_CACHE, PART_TX_CACHE);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        changeClusterReadOnlyMode(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(REPL_ATOMIC_CACHE, REPLICATED, ATOMIC, null),
            cacheConfiguration(REPL_TX_CACHE, REPLICATED, TRANSACTIONAL, null),
            cacheConfiguration(PART_ATOMIC_CACHE, PARTITIONED, ATOMIC, "part_grp"),
            cacheConfiguration(PART_TX_CACHE, PARTITIONED, TRANSACTIONAL, "part_grp")
        );

        return cfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param grpName Cache group name.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode cacheMode,
        CacheAtomicityMode atomicityMode, String grpName) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(name)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setGroupName(grpName)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)));
    }

    /**
     * Change read only mode on all nodes.
     *
     * @param readOnly Read only.
     */
    protected void changeClusterReadOnlyMode(boolean readOnly) {
        for (int idx = 0; idx < SRVS; idx++) {
            IgniteEx ignite = grid(idx);

            ignite.context().cache().context().readOnlyMode(readOnly);
        }
    }
}
