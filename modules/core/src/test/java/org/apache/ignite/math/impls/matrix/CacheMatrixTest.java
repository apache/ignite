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

package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.math.IdentityValueMapper;
import org.apache.ignite.math.KeyMapper;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link CacheMatrix}.
 *
 * TODO: wip
 */
@GridCommonTest(group = "Distributed Models")
public class CacheMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** Grid instance. */
    private Ignite ignite;

    /**
     * Default constructor.
     */
    public CacheMatrixTest(){
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
    }

    /** */
    public void testGetSet() throws Exception {
        final int rows = MathTestConstants.STORAGE_SIZE;
        final int cols = MathTestConstants.STORAGE_SIZE;

        KeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
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
    public void testCopy() throws Exception{
        final int rows = MathTestConstants.STORAGE_SIZE;
        final int cols = MathTestConstants.STORAGE_SIZE;

        KeyMapper<Integer> keyMapper = getKeyMapper(rows, cols);
        IgniteCache<Integer, Double> cache = getCache();
        CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(rows, cols, cache, keyMapper, new IdentityValueMapper());

        fillMatrix(cacheMatrix);

        Matrix copy = cacheMatrix.copy();

        assertTrue("Copy of cache matrix is not equal to original.", copy.equals(cacheMatrix));
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
    private KeyMapper<Integer> getKeyMapper(final int rows, final int cols) {
        return new KeyMapper<Integer>() {
            @Override public Integer apply(int x, int y) {
                return x * cols + y;
            }

            @Override public boolean isValid(Integer integer) {
                return (rows * cols) > integer;
            }
        };
    }

    /** */
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }
}