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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.DistributedStorage;
import org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.ml.math.impls.storage.matrix.BlockMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.ml.math.impls.MathTestConstants.UNEXPECTED_VAL;

/**
 * Tests for {@link SparseBlockDistributedMatrix}.
 */
@GridCommonTest(group = "Distributed Models")
public class SparseDistributedBlockMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;

    /** Precision. */
    private static final double PRECISION = 0.0;

    /** Grid instance. */
    private Ignite ignite;

    /** Matrix rows */
    private final int rows = MathTestConstants.STORAGE_SIZE;

    /** Matrix cols */
    private final int cols = MathTestConstants.STORAGE_SIZE;

    /** Matrix for tests */
    private SparseBlockDistributedMatrix cacheMatrix;

    /**
     * Default constructor.
     */
    public SparseDistributedBlockMatrixTest() {
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
        if (cacheMatrix != null) {
            cacheMatrix.destroy();
            cacheMatrix = null;
        }
    }

    /** */
    public void testGetSet() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double v = Math.random();
                cacheMatrix.set(i, j, v);

                assertEquals("Unexpected value for matrix element[" + i + " " + j + "]", v, cacheMatrix.get(i, j), PRECISION);
            }
        }
    }

    /** */
    public void testExternalize() throws IOException, ClassNotFoundException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        cacheMatrix.set(1, 1, 1.0);

        ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

        objOutputStream.writeObject(cacheMatrix);

        ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
        ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

        SparseBlockDistributedMatrix objRestored = (SparseBlockDistributedMatrix)objInputStream.readObject();

        assertTrue(MathTestConstants.VAL_NOT_EQUALS, cacheMatrix.equals(objRestored));
        assertEquals(MathTestConstants.VAL_NOT_EQUALS, objRestored.get(1, 1), 1.0, PRECISION);
    }

    /** Test simple math. */
    public void testMath() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);
        initMtx(cacheMatrix);

        cacheMatrix.assign(2.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 2.0, cacheMatrix.get(i, j), PRECISION);

        cacheMatrix.plus(3.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 5.0, cacheMatrix.get(i, j), PRECISION);

        cacheMatrix.times(2.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 10.0, cacheMatrix.get(i, j), PRECISION);

        cacheMatrix.divide(10.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 1.0, cacheMatrix.get(i, j), PRECISION);

        assertEquals(UNEXPECTED_VAL, cacheMatrix.rowSize() * cacheMatrix.columnSize(), cacheMatrix.sum(), PRECISION);
    }

    /** */
    public void testMinMax() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, i * cols + j + 1);

        assertEquals(UNEXPECTED_VAL, 1.0, cacheMatrix.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, rows * cols, cacheMatrix.maxValue(), PRECISION);

        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, -1.0 * (i * cols + j + 1));

        assertEquals(UNEXPECTED_VAL, -rows * cols, cacheMatrix.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, -1.0, cacheMatrix.maxValue(), PRECISION);

        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, i * cols + j);

        assertEquals(UNEXPECTED_VAL, 0.0, cacheMatrix.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, rows * cols - 1.0, cacheMatrix.maxValue(), PRECISION);
    }

    /** */
    public void testMap() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);
        initMtx(cacheMatrix);

        cacheMatrix.map(i -> 100.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 100.0, cacheMatrix.get(i, j), PRECISION);
    }

    /** */
    public void testCopy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        cacheMatrix.set(rows - 1, cols - 1, 1);

        Matrix newMatrix = cacheMatrix.copy();
        assert newMatrix.columnSize() == cols;
        assert newMatrix.rowSize() == rows;
        assert newMatrix.get(rows - 1, cols - 1) == 1;

    }

    /** Test cache behaviour for matrix with different blocks */
    public void testCacheBehaviour() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        cacheBehaviorLogic(rows);
    }

    /** Test cache behaviour for matrix with homogeneous blocks */
    public void testCacheBehaviourWithHomogeneousBlocks() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int size = MatrixBlockEntry.MAX_BLOCK_SIZE * 3;
        cacheBehaviorLogic(size);
    }

    /** */
    private void cacheBehaviorLogic(int size) {
        SparseBlockDistributedMatrix cacheMatrix1 = new SparseBlockDistributedMatrix(size, size);
        SparseBlockDistributedMatrix cacheMatrix2 = new SparseBlockDistributedMatrix(size, size);

        initMtx(cacheMatrix1);
        initMtx(cacheMatrix2);

        Collection<String> cacheNames = ignite.cacheNames();

        assert cacheNames.contains(((DistributedStorage)cacheMatrix1.getStorage()).cacheName());

        IgniteCache<MatrixBlockKey, Object> cache = ignite.getOrCreateCache(((DistributedStorage)cacheMatrix1.getStorage()).cacheName());

        Set<MatrixBlockKey> keySet1 = buildKeySet(cacheMatrix1);
        Set<MatrixBlockKey> keySet2 = buildKeySet(cacheMatrix2);

        assert cache.containsKeys(keySet1);
        assert cache.containsKeys(keySet2);

        cacheMatrix2.destroy();

        assert cache.containsKeys(keySet1);
        assert !cache.containsKeys(keySet2);

        cacheMatrix1.destroy();

        assert !cache.containsKeys(keySet1);
    }

    /** */
    public void testLike() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        assertNotNull(cacheMatrix.like(1, 1));
    }

    /** */
    public void testLikeVector() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseBlockDistributedMatrix(rows, cols);

        Vector v = cacheMatrix.likeVector(1);
        assert v.size() == 1;
        assert v instanceof SparseBlockDistributedVector;

    }

    /**
     * Simple test for two square matrices.
     */
    public void testSquareMatrixTimes() {
        squareMatrixTimesLogic(rows);
    }

    /**
     * Simple test for two square matrices with size which is proportional to MAX_BLOCK_SIZE constant
     */
    public void testSquareMatrixTimesWithHomogeneousBlocks() {
        int size = MatrixBlockEntry.MAX_BLOCK_SIZE * 3;

        squareMatrixTimesLogic(size);
    }

    /** Build two square matrices, multiply them and check main diagonal elements */
    private void squareMatrixTimesLogic(int size) {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        Matrix cacheMatrix1 = new SparseBlockDistributedMatrix(size, size);
        Matrix cacheMatrix2 = new SparseBlockDistributedMatrix(size, size);

        for (int i = 0; i < size; i++) {
            cacheMatrix1.setX(i, i, i);
            cacheMatrix2.setX(i, i, i);
        }

        Matrix res = cacheMatrix1.times(cacheMatrix2);

        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                if (i == j)
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, i * i, res.get(i, j), PRECISION);
                else
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, 0, res.get(i, j), PRECISION);
    }

    /**
     *
     */
    public void testNonSquareMatrixTimes() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int size = MatrixBlockEntry.MAX_BLOCK_SIZE + 1;
        int size2 = MatrixBlockEntry.MAX_BLOCK_SIZE * 2 + 1;

        Matrix cacheMatrix1 = new SparseBlockDistributedMatrix(size2, size);
        Matrix cacheMatrix2 = new SparseBlockDistributedMatrix(size, size2);

        for (int i = 0; i < size; i++) {
            cacheMatrix1.setX(i, i, i);
            cacheMatrix2.setX(i, i, i);
        }

        Matrix res = cacheMatrix1.times(cacheMatrix2);

        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                if (i == j)
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, i * i, res.get(i, j), PRECISION);
                else
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, 0, res.get(i, j), PRECISION);
    }

    /**
     *
     */
    public void testNonSquareMatrixTimes2() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int size = MatrixBlockEntry.MAX_BLOCK_SIZE + 1;
        int size2 = MatrixBlockEntry.MAX_BLOCK_SIZE * 2 + 1;

        Matrix cacheMatrix1 = new SparseBlockDistributedMatrix(size, size2);
        Matrix cacheMatrix2 = new SparseBlockDistributedMatrix(size2, size);

        for (int i = 0; i < size; i++) {
            cacheMatrix1.setX(i, i, i);
            cacheMatrix2.setX(i, i, i);
        }

        Matrix res = cacheMatrix1.times(cacheMatrix2);

        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                if (i == j)
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, i * i, res.get(i, j), PRECISION);
                else
                    assertEquals(UNEXPECTED_VAL + " for " + i + ":" + j, 0, res.get(i, j), PRECISION);
    }

    /** */
    public void testMatrixVectorTimes() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix a = new SparseBlockDistributedMatrix(new double[][] {{2.0, 4.0, 0.0}, {-2.0, 1.0, 3.0}, {-1.0, 0.0, 1.0}});
        SparseBlockDistributedVector b = new SparseBlockDistributedVector(new double[] {1.0, 2.0, -1.0});
        SparseBlockDistributedVector res = new SparseBlockDistributedVector(new double[] {10, -3.0, -2.0});

        Vector calculatedRes = a.times(b);

        for (int i = 0; i < calculatedRes.size(); i++)
            assertEquals(UNEXPECTED_VAL + " for " + i, res.get(i), calculatedRes.get(i), PRECISION);
    }

    /** */
    private void initMtx(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, 1.0);
    }

    /** Build key set for SparseBlockDistributedMatrix. */
    private Set<MatrixBlockKey> buildKeySet(SparseBlockDistributedMatrix m) {

        BlockMatrixStorage storage = (BlockMatrixStorage)m.getStorage();

        return storage.getAllKeys();
    }
}
