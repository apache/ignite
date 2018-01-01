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

package org.apache.ignite.ml.trees;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVectorDouble;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for decision trees test.
 */
public class BaseDecisionTreeTest extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 4;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * Default constructor.
     */
    public BaseDecisionTreeTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
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
     * Convert double array to  {@link LabeledVectorDouble}
     *
     * @param arr Array for conversion.
     * @return LabeledVectorDouble.
     */
    protected static LabeledVectorDouble<DenseLocalOnHeapVector> asLabeledVector(double arr[]) {
        return new LabeledVectorDouble<>(new DenseLocalOnHeapVector(Arrays.copyOf(arr, arr.length - 1)), arr[arr.length - 1]);
    }
}
