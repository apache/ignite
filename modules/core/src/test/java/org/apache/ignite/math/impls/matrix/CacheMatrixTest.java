package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.math.IdentityValueMapper;
import org.apache.ignite.math.KeyMapper;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link CacheMatrix}.
 *
 * TODO: wip
 */
public class CacheMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_NUMBER = 3;
    /** Grid instance. */
    private Ignite ignite;

    public CacheMatrixTest(){
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(NODE_NUMBER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(1);
    }

    /** TODO: wip, make this work */
    public void getSetTest() throws Exception {
        int rows = MathTestConstants.STORAGE_SIZE;
        int cols = MathTestConstants.STORAGE_SIZE;

        IgniteCache<Integer, Double> cache = ignite.cache("matrixCache");

        KeyMapper<Integer> keyMapper = new KeyMapper<Integer>() {
            @Override public Integer apply(int x, int y) {
                return null;
            }

            @Override public boolean isValid(Integer integer) {
                return false;
            }
        };

        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());
    }

}