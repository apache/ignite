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

package org.apache.ignite.yardstick.ml.decomposition;

import java.util.Map;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.decompositions.LUDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.PivotedMatrixView;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.ml.DataChanger;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteLUDecompositionBenchmark extends IgniteAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        runLUDecomposition();

        return true;
    }

    /**
     * Based on LUDecompositionTest.
     */
    private void runLUDecomposition() {
        Matrix testMatrix = new DenseLocalOnHeapMatrix(new DataChanger.Scale().mutate(new double[][] {
            {2.0d, 1.0d, 1.0d, 0.0d},
            {4.0d, 3.0d, 3.0d, 1.0d},
            {8.0d, 7.0d, 9.0d, 5.0d},
            {6.0d, 7.0d, 9.0d, 8.0d}}));

        LUDecomposition dec1 = new LUDecomposition(new PivotedMatrixView(testMatrix));

        dec1.solve(new DenseLocalOnHeapVector(testMatrix.rowSize()));

        dec1.destroy();

        LUDecomposition dec2 = new LUDecomposition(new PivotedMatrixView(testMatrix));

        dec2.solve(new DenseLocalOnHeapMatrix(testMatrix.rowSize(), testMatrix.rowSize()));

        dec2.destroy();

        LUDecomposition dec3 = new LUDecomposition(testMatrix);

        dec3.getL();

        dec3.getU();

        dec3.getP();

        dec3.getPivot();

        dec3.destroy();
    }
}
