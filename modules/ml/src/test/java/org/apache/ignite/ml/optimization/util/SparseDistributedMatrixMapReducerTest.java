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

package org.apache.ignite.ml.optimization.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link SparseDistributedMatrixMapReducer}.
 */
public class SparseDistributedMatrixMapReducerTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 2;

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Tests that matrix 100x100 filled by "1.0" and distributed across nodes successfully processed (calculate sum of
     * all elements) via {@link SparseDistributedMatrixMapReducer}.
     */
    public void testMapReduce() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        SparseDistributedMatrix distributedMatrix = new SparseDistributedMatrix(100, 100);
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < 100; j++)
                distributedMatrix.set(i, j, 1);
        SparseDistributedMatrixMapReducer mapReducer = new SparseDistributedMatrixMapReducer(distributedMatrix);
        double total = mapReducer.mapReduce(
            (matrix, args) -> {
                double partialSum = 0.0;
                for (int i = 0; i < matrix.rowSize(); i++)
                    for (int j = 0; j < matrix.columnSize(); j++)
                        partialSum += matrix.get(i, j);
                return partialSum;
            },
            sums -> {
                double totalSum = 0;
                for (Double partialSum : sums)
                    if (partialSum != null)
                        totalSum += partialSum;
                return totalSum;
            }, 0.0);
        assertEquals(100.0 * 100.0, total, 1e-18);
    }

    /**
     * Tests that matrix 100x100 filled by "1.0" and distributed across nodes successfully processed via
     * {@link SparseDistributedMatrixMapReducer} even when mapping function returns {@code null}.
     */
    public void testMapReduceWithNullValues() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        SparseDistributedMatrix distributedMatrix = new SparseDistributedMatrix(100, 100);
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < 100; j++)
                distributedMatrix.set(i, j, 1);
        SparseDistributedMatrixMapReducer mapReducer = new SparseDistributedMatrixMapReducer(distributedMatrix);
        double total = mapReducer.mapReduce(
            (matrix, args) -> null,
            sums -> {
                double totalSum = 0;
                for (Double partialSum : sums)
                    if (partialSum != null)
                        totalSum += partialSum;
                return totalSum;
            }, 0.0);
        assertEquals(0, total, 1e-18);
    }

    /**
     * Tests that matrix 1x100 filled by "1.0" and distributed across nodes successfully processed (calculate sum of
     * all elements) via {@link SparseDistributedMatrixMapReducer} even when not all nodes contains data.
     */
    public void testMapReduceWithOneEmptyNode() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        SparseDistributedMatrix distributedMatrix = new SparseDistributedMatrix(1, 100);
        for (int j = 0; j < 100; j++)
            distributedMatrix.set(0, j, 1);
        SparseDistributedMatrixMapReducer mapReducer = new SparseDistributedMatrixMapReducer(distributedMatrix);
        double total = mapReducer.mapReduce(
            (matrix, args) -> {
                double partialSum = 0.0;
                for (int i = 0; i < matrix.rowSize(); i++)
                    for (int j = 0; j < matrix.columnSize(); j++)
                        partialSum += matrix.get(i, j);
                return partialSum;
            },
            sums -> {
                double totalSum = 0;
                for (Double partialSum : sums)
                    if (partialSum != null)
                        totalSum += partialSum;
                return totalSum;
            }, 0.0);
        assertEquals(100.0, total, 1e-18);
    }
}
