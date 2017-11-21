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

package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link OLSMultipleLinearRegressionModel}.
 */
public class OLSMultipleLinearRegressionModelTest {
    /** */
    @Test
    public void testPerfectFit() {
        Vector val = new DenseLocalOnHeapVector(new double[] {11.0, 12.0, 13.0, 14.0, 15.0, 16.0});

        Matrix x = new DenseLocalOnHeapMatrix(new double[][] {
            new double[] {0, 0, 0, 0, 0},
            new double[] {2.0, 0, 0, 0, 0},
            new double[] {0, 3.0, 0, 0, 0},
            new double[] {0, 0, 4.0, 0, 0},
            new double[] {0, 0, 0, 5.0, 0},
            new double[] {0, 0, 0, 0, 6.0}});

        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(val, x);

        OLSMultipleLinearRegressionModel mdl = new OLSMultipleLinearRegressionModel(regression);

        TestUtils.assertEquals(new double[] {0d, 0d, 0d, 0d, 0d, 0d},
            val.minus(mdl.predict(val)).getStorage().data(), 1e-13);
    }
}
