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

package org.apache.ignite.examples.ml.math.matrix;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;

/**
 * Utility functions for {@link Matrix} API examples.
 */
class MatrixExampleUtil {
    /**
     * Verifies matrix transposition.
     *
     * @param m Original matrix.
     * @param transposed Transposed matrix.
     */
    static void verifyTransposition(Matrix m, Matrix transposed) {
        for (int row = 0; row < m.rowSize(); row++)
            for (int col = 0; col < m.columnSize(); col++) {
                double val = m.get(row, col);
                double valTransposed = transposed.get(col, row);

                assert val == valTransposed : "Values not equal at (" + row + "," + col
                    + "), original: " + val + " transposed: " + valTransposed;
            }
    }

    /**
     * Prints matrix values to console.
     *
     * @param m Matrix to print.
     */
    static void print(Matrix m) {
        Tracer.showAscii(m);
    }
}
