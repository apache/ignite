// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

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
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.ml.math.impls.MathTestConstants.UNEXPECTED_VAL;

/**
 * Tests for {@link SparseDistributedMatrix}.
 */
@GridCommonTest(group = "Distributed Models")
public class SparseDistributedMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** Precision. */
    private static final double PRECISION = 0.0;
    /** Grid instance. */
    private Ignite ignite;
    /** Matrix rows */
    private final int rows = MathTestConstants.STORAGE_SIZE;
    /** Matrix cols */
    private final int cols = MathTestConstants.STORAGE_SIZE;
    /** Matrix for tests */
    private SparseDistributedMatrix cacheMatrix;

    /**
     * Default constructor.
     */
    public SparseDistributedMatrixTest() {
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

        if (cacheMatrix != null) {
            cacheMatrix.destroy();
            cacheMatrix = null;
        }
    }

    /** */
    public void testGetSet() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double v = Math.random();
                cacheMatrix.set(i, j, v);

                assert Double.compare(v, cacheMatrix.get(i, j)) == 0;
            }
        }
    }

    /** */
    public void testExternalize() throws IOException, ClassNotFoundException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        cacheMatrix.set(1, 1, 1.0);

        ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

        objOutputStream.writeObject(cacheMatrix);

        ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
        ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

        SparseDistributedMatrix objRestored = (SparseDistributedMatrix)objInputStream.readObject();

        assertTrue(MathTestConstants.VAL_NOT_EQUALS, cacheMatrix.equals(objRestored));
        assertEquals(MathTestConstants.VAL_NOT_EQUALS, objRestored.get(1, 1), 1.0, 0.0);
    }

    /** Test simple math. */
    public void testMath() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
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

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

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

        assertEquals(UNEXPECTED_VAL, 1.0, cacheMatrix.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, rows * cols - 1.0, cacheMatrix.maxValue(), PRECISION);
    }

    /** */
    public void testMap() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        initMtx(cacheMatrix);

        cacheMatrix.map(i -> 100.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VAL, 100.0, cacheMatrix.get(i, j), PRECISION);
    }

    /** */
    public void testCopy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.copy();
            fail("UnsupportedOperationException expected.");
        }
        catch (UnsupportedOperationException e) {
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    public void testLike() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.like(1, 1);
            fail("UnsupportedOperationException expected.");
        }
        catch (UnsupportedOperationException e) {
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    public void testLikeVector() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.likeVector(1);
            fail("UnsupportedOperationException expected.");
        }
        catch (UnsupportedOperationException e) {
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    private void initMtx(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, 1.0);
    }
}
