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

package org.apache.ignite.examples.ml.math.decompositions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.decompositions.CholeskyDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link CholeskyDecomposition}.
 */
public class CholeskyDecompositionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> Cholesky decomposition example started.");
        // Let's compute a Cholesky decomposition of Hermitian matrix m:
        // m = l l^{*}, where
        // l is a lower triangular matrix
        // l^{*} is its conjugate transpose

        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        });
        System.out.println("\n>>> Matrix m for decomposition: ");
        Tracer.showAscii(m);

        // This decomposition is useful when dealing with systems of linear equations of the form
        // m x = b where m is a Hermitian matrix.
        // For such systems Cholesky decomposition provides
        // more effective method of solving compared to LU decomposition.
        // Suppose we want to solve system
        // m x = b for various bs. Then after we computed Cholesky decomposition, we can feed various bs
        // as a matrix of the form
        // (b1, b2, ..., bm)
        // to the method Cholesky::solve which returns solutions in the form
        // (sol1, sol2, ..., solm)
        CholeskyDecomposition dec = new CholeskyDecomposition(m);
        System.out.println("\n>>> Made decomposition m = l * l^{*}.");
        System.out.println(">>> Matrix l is ");
        Tracer.showAscii(dec.getL());
        System.out.println(">>> Matrix l^{*} is ");
        Tracer.showAscii(dec.getLT());

        Matrix bs = new DenseLocalOnHeapMatrix(new double[][] {
            {4.0, -6.0, 7.0},
            {1.0, 1.0, 1.0}
        }).transpose();
        System.out.println("\n>>> Solving systems of linear equations of the form m x = b for various bs represented by columns of matrix");
        Tracer.showAscii(bs);
        Matrix sol = dec.solve(bs);

        System.out.println("\n>>> List of solutions: ");
        for (int i = 0; i < sol.columnSize(); i++)
            Tracer.showAscii(sol.viewColumn(i));

        System.out.println("\n>>> Cholesky decomposition example completed.");
    }
}
