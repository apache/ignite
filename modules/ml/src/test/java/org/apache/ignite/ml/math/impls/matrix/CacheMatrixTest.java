/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math.impls.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.IdentityValueMapper;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.MatrixKeyMapper;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link CacheMatrix}.
 */
@GridCommonTest(group = "Distributed Models")
public class CacheMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** */
    private static final String UNEXPECTED_ATTRIBUTE_VALUE = "Unexpected attribute value.";
    /** Grid instance. */
    private Ignite ignite;
    /** Matrix rows */
    private final int rows = MathTestConstants.STORAGE_SIZE;
    /** Matrix cols */
    private final int cols = MathTestConstants.STORAGE_SIZE;

    /**
     * Default constructor.
     */
    public CacheMatrixTest() {
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
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(CACHE_NAME);
    }

    /** */
    public void testGetSet() throws Exception {
        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double v = Math.random();
                cacheMatrix.set(i, j, v);

                assert Double.compare(v, cacheMatrix.get(i, j)) == 0;
                assert Double.compare(v, cache.get(keyMapper.apply(i, j))) == 0;
            }
        }
    }

    /** */
    public void testCopy() throws Exception {
        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        fillMatrix(cacheMatrix);

        try {
            cacheMatrix.copy();

            fail("UnsupportedOperationException expected");
        }
        catch (UnsupportedOperationException e) {
            // No-op.
        }
    }

    /** */
    public void testLike() throws Exception {
        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        try {
            cacheMatrix.like(rows, cols);

            fail("UnsupportedOperationException expected");
        }
        catch (UnsupportedOperationException e) {
            // No-op.
        }
    }

    /** */
    public void testLikeVector() throws Exception {
        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        try {
            cacheMatrix.likeVector(cols);

            fail("UnsupportedOperationException expected");
        }
        catch (UnsupportedOperationException e) {
            // No-op.
        }
    }

    /** */
    public void testPlus() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double plusVal = 2;

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        initMatrix(cacheMatrix);

        cacheMatrix.plus(plusVal);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertEquals(plusVal, cacheMatrix.get(i, j));
    }

    /** */
    public void testDivide() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double initVal = 1;
        double divVal = 2;

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        initMatrix(cacheMatrix);
        cacheMatrix.assign(initVal);
        cacheMatrix.divide(divVal);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertTrue(Double.compare(cacheMatrix.get(i, j), initVal / divVal) == 0);
    }

    /** */
    public void testTimes() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double initVal = 1;
        double timVal = 2;

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        initMatrix(cacheMatrix);
        cacheMatrix.assign(initVal);
        cacheMatrix.times(timVal);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertTrue(Double.compare(cacheMatrix.get(i, j), initVal * timVal) == 0);
    }

    /** */
    public void testSum() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double initVal = 1;

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        double sum = 0;

        initMatrix(cacheMatrix);
        sum = cacheMatrix.sum();

        assertTrue(Double.compare(sum, 0d) == 0);

        cacheMatrix.assign(1d);
        sum = cacheMatrix.sum();

        assertTrue(Double.compare(sum, rows * cols) == 0);
    }

    /** */
    public void testAssignSingleValue() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double initVal = 1;

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        initMatrix(cacheMatrix);

        cacheMatrix.assign(initVal);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertEquals(initVal, cacheMatrix.get(i, j));
    }

    /** */
    public void testAssignArray() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] initVal = new double[rows][cols];

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                initVal[i][j] = Math.random();

        cacheMatrix.assign(initVal);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertEquals(initVal[i][j], cacheMatrix.get(i, j));
    }

    /** */
    public void testAttributes() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        assertFalse(UNEXPECTED_ATTRIBUTE_VALUE, cacheMatrix.isSequentialAccess());
        assertFalse(UNEXPECTED_ATTRIBUTE_VALUE, cacheMatrix.isDense());
        assertFalse(UNEXPECTED_ATTRIBUTE_VALUE, cacheMatrix.isArrayBased());
        assertTrue(UNEXPECTED_ATTRIBUTE_VALUE, cacheMatrix.isRandomAccess());
        assertTrue(UNEXPECTED_ATTRIBUTE_VALUE, cacheMatrix.isDistributed());
    }

    /** */
    public void testExternalization() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        final CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        ExternalizeTest<CacheMatrix<Integer, Double>> externalizeTest = new ExternalizeTest<CacheMatrix<Integer, Double>>() {

            @Override public void externalizeTest() {
                super.externalizeTest(cacheMatrix);
            }
        };

        externalizeTest.externalizeTest();
    }

    /** */
    public void testMinMax() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        final CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                cacheMatrix.set(i, j, i * rows + j);

        assertEquals(0.0, cacheMatrix.minValue(), 0.0);
        assertEquals(rows * cols - 1, cacheMatrix.maxValue(), 0.0);
    }

    /** */
    public void testMap() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        MatrixKeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        final CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        initMatrix(cacheMatrix);

        cacheMatrix.map(value -> value + 10);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                assertEquals(10.0, cacheMatrix.getX(i, j), 0.0);
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

    /** */
    private MatrixKeyMapper<Integer> getKeyMapper(final int rows, final int cols) {
        return new MatrixKeyMapperForTests(rows, cols);
    }

    /** Init the given matrix by random values. */
    private void fillMatrix(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

    /** Init the given matrix by zeros. */
    private void initMatrix(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, 0d);
    }
}
