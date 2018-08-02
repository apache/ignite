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

package org.apache.ignite.ml.nn;

import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link LossFunctions}.
 */
public class LossFunctionsTest {
    /** */
    @Test
    public void testMSE() {
        test(new double[] {1.0, 3.0}, LossFunctions.MSE);
    }

    /** */
    @Test
    public void testLOG() {
        test(new double[] {1.0, 3.0}, LossFunctions.LOG);
    }

    /** */
    @Test
    public void testL2() {
        test(new double[] {1.0, 3.0}, LossFunctions.L2);
    }

    /** */
    @Test
    public void testL1() {
        test(new double[] {1.0, 3.0}, LossFunctions.L1);
    }

    /** */
    @Test
    public void testHINGE() {
        test(new double[] {1.0, 3.0}, LossFunctions.HINGE);
    }

    /** */
    private void test(double[] expData, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        verify(expData, loss
            .apply(new DenseVector(new double[] {2.0, 1.0}))
            .differential(new DenseVector(new double[] {3.0, 4.0})));
    }

    /** */
    private void verify(double[] expData, Vector actual) {
        Tracer.showAscii(actual);

        assertArrayEquals(expData, new DenseVector(actual.size()).assign(actual).getStorage().data(), 0);
    }
}
