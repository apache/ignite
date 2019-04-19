/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.nn;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link LossFunctions}.
 */
public class LossFunctionsTest {
    /** */
    @Test
    public void testMSE() {
        IgniteDifferentiableVectorToDoubleFunction f = LossFunctions.MSE.apply(new DenseVector(new double[] {2.0, 1.0}));

        assertNotNull(f);

        test(new double[] {1.0, 3.0}, f);
    }

    /** */
    @Test
    public void testLOG() {
        IgniteDifferentiableVectorToDoubleFunction f = LossFunctions.LOG.apply(new DenseVector(new double[] {2.0, 1.0}));

        assertNotNull(f);

        test(new double[] {1.0, 3.0}, f);
    }

    /** */
    @Test
    public void testL2() {
        IgniteDifferentiableVectorToDoubleFunction f = LossFunctions.L2.apply(new DenseVector(new double[] {2.0, 1.0}));

        assertNotNull(f);

        test(new double[] {1.0, 3.0}, f);
    }

    /** */
    @Test
    public void testL1() {
        IgniteDifferentiableVectorToDoubleFunction f = LossFunctions.L1.apply(new DenseVector(new double[] {2.0, 1.0}));

        assertNotNull(f);

        test(new double[] {1.0, 3.0}, f);
    }

    /** */
    @Test
    public void testHINGE() {
        IgniteDifferentiableVectorToDoubleFunction f = LossFunctions.HINGE.apply(new DenseVector(new double[] {2.0, 1.0}));

        assertNotNull(f);

        test(new double[] {1.0, 3.0}, f);
    }

    /** */
    private void test(double[] expData, IgniteDifferentiableVectorToDoubleFunction f) {
        verify(expData, f.differential(new DenseVector(new double[] {3.0, 4.0})));
    }

    /** */
    private void verify(double[] expData, Vector actual) {
        assertArrayEquals(expData, new DenseVector(actual.size()).assign(actual).getStorage().data(), 0);
    }
}
