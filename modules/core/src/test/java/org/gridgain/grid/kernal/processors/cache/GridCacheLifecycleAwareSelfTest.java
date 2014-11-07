/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.cloner.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Test for {@link GridLifecycleAware} support in {@link GridCacheConfiguration}.
 */
public class GridCacheLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridCacheDistributionMode distroMode;

    /** */
    private boolean writeBehind;

    /**
     */
    private static class TestStore extends TestLifecycleAware implements GridCacheStore {
        /**
         */
        TestStore() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(@Nullable GridCacheTx tx, Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(GridBiInClosure clo, @Nullable Object... args) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable GridCacheTx tx, @Nullable Collection keys,
            GridBiInClosure c) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable GridCacheTx tx, Object key,
            @Nullable Object val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable GridCacheTx tx, @Nullable Map map) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable GridCacheTx tx, Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable GridCacheTx tx,
            @Nullable Collection keys) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(GridCacheTx tx, boolean commit) {
            // No-op.
        }
    }

    /**
     */
    private static class TestAffinityFunction extends TestLifecycleAware implements GridCacheAffinityFunction {
        /**
         */
        TestAffinityFunction() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public List<List<GridNode>> assignPartitions(GridCacheAffinityFunctionContext affCtx) {
            List<List<GridNode>> res = new ArrayList<>();

            res.add(nodes(0, affCtx.currentTopologySnapshot()));

            return res;
        }

        /** {@inheritDoc} */
        public List<GridNode> nodes(int part, Collection<GridNode> nodes) {
            return new ArrayList<>(nodes);
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }

    /**
     */
    private static class TestEvictionPolicy extends TestLifecycleAware implements GridCacheEvictionPolicy {
        /**
         */
        TestEvictionPolicy() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, GridCacheEntry entry) {
            // No-op.
        }
    }

    /**
     */
    private static class TestEvictionFilter extends TestLifecycleAware implements GridCacheEvictionFilter {
        /**
         */
        TestEvictionFilter() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public boolean evictAllowed(GridCacheEntry entry) {
            return false;
        }
    }

    /**
     */
    private static class TestCloner extends TestLifecycleAware implements GridCacheCloner {
        /**
         */
        TestCloner() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T cloneValue(T val) throws GridException {
            return val;
        }
    }

    /**
     */
    private static class TestAffinityKeyMapper extends TestLifecycleAware implements GridCacheAffinityKeyMapper {
        /**
         */
        TestAffinityKeyMapper() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }

    /**
     */
    private static class TestInterceptor extends TestLifecycleAware implements GridCacheInterceptor {
        /**
         */
        private TestInterceptor() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, @Nullable Object val) {
            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Object key, Object val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked") @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
            return new GridBiTuple(false, val);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Object key, Object val) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override protected final GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new GridTcpDiscoverySpi());

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setDistributionMode(distroMode);

        ccfg.setWriteBehindEnabled(writeBehind);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        TestStore store = new TestStore();

        ccfg.setStore(store);

        lifecycleAwares.add(store);

        TestAffinityFunction affinity = new TestAffinityFunction();

        ccfg.setAffinity(affinity);

        lifecycleAwares.add(affinity);

        TestEvictionPolicy evictionPlc = new TestEvictionPolicy();

        ccfg.setEvictionPolicy(evictionPlc);

        lifecycleAwares.add(evictionPlc);

        TestEvictionPolicy nearEvictionPlc = new TestEvictionPolicy();

        ccfg.setNearEvictionPolicy(nearEvictionPlc);

        lifecycleAwares.add(nearEvictionPlc);

        TestEvictionFilter evictionFilter = new TestEvictionFilter();

        ccfg.setEvictionFilter(evictionFilter);

        lifecycleAwares.add(evictionFilter);

        TestCloner cloner = new TestCloner();

        ccfg.setCloner(cloner);

        lifecycleAwares.add(cloner);

        TestAffinityKeyMapper mapper = new TestAffinityKeyMapper();

        ccfg.setAffinityMapper(mapper);

        lifecycleAwares.add(mapper);

        TestInterceptor interceptor = new TestInterceptor();

        lifecycleAwares.add(interceptor);

        ccfg.setInterceptor(interceptor);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ErrorNotRethrown")
    @Override public void testLifecycleAware() throws Exception {
        for (GridCacheDistributionMode mode : new GridCacheDistributionMode[] {PARTITIONED_ONLY, NEAR_PARTITIONED}) {
            distroMode = mode;

            writeBehind = false;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }

            writeBehind = true;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }
        }
    }
}
