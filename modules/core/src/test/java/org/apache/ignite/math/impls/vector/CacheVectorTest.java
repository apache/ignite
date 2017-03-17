package org.apache.ignite.math.impls.vector;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.math.IdentityValueMapper;
import org.apache.ignite.math.KeyMapper;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.math.impls.matrix.CacheMatrix;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(cols, getCache(), (x) -> x, valueMapper);

        for (int i = 0; i < cols; i++) {
            double random = Math.random();
            cacheVector.set(i, random);
            assertEquals("Unexpected value.", random, cacheVector.get(i), 0d);
        }
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