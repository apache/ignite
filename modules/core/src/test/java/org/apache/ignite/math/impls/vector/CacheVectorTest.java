package org.apache.ignite.math.impls.vector;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Tests for {@link CacheVector}.
 */
@GridCommonTest(group = "Distributed Models")
public class CacheVectorTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** Grid instance. */
    private Ignite ignite;
    // Default kep mapper.
    private VectorKeyMapper<Integer> keyMapper = new VectorKeyMapper<Integer>() {
        @Override
        public Integer apply(int i) {
            return i;
        }

        @Override
        public boolean isValid(Integer o) {
            return true;
        }
    };

    /**
     * Default constructor.
     */
    public CacheVectorTest(){
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** */
    public void testGetSet() throws Exception {
        final int cols = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(cols, getCache(), keyMapper, valueMapper);

        for (int i = 0; i < cols; i++) {
            double random = Math.random();
            cacheVector.set(i, random);
            assertEquals("Unexpected value.", random, cacheVector.get(i), 0d);
        }
    }

    /** */
    public void testMap(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int cols = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(cols, getCache(), keyMapper, valueMapper);

        cacheVector.map(value -> 110d);

        for (int i = 0; i < cols; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 110d, 0d);
    }

    /** */
    public void testSum(){

    }

    /** */
    private IgniteCache<Integer, Double> getCache() {
        assert ignite != null;

        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setName(CACHE_NAME);

        IgniteCache<Integer, Double> cache = ignite.getOrCreateCache(CACHE_NAME);

        assert cache != null;
        return cache;
    }
}