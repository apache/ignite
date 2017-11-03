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
import org.apache.ignite.ml.math.decompositions.LUDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link LUDecomposition}.
 */
public class LUDecompositionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> LU decomposition example started.");
        // Let's compute a LU decomposition for some (n x n) matrix m:
        // m = p l u, where
        // p is an (n x n) is a row-permutation matrix
        // l is a (n x n) lower triangular matrix
        // u is a (n x n) upper triangular matrix

        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 1.0d, -1.0d},
            {1.0d, -2.0d, 3.0d},
            {2.0d, 3.0d, 1.0d}
        });
        System.out.println("\n>>> Matrix m for decomposition: ");
        Tracer.showAscii(m);

        // This decomposition is useful when dealing with systems of linear equations.
        // (see https://en.wikipedia.org/wiki/LU_decomposition)
        // suppose we want to solve system
        // m x = b for various bs. Then after we computed LU decomposition, we can feed various bs
        // as a matrix of the form
        // (b1, b2, ..., bm)
        // to the method LUDecomposition::solve which returns solutions in the form
        // (sol1, sol2, ..., solm)

        LUDecomposition dec = new LUDecomposition(m);
        System.out.println("\n>>> Made decomposition.");
        System.out.println(">>> Matrix getL is ");
        Tracer.showAscii(dec.getL());
        System.out.println(">>> Matrix getU is ");
        Tracer.showAscii(dec.getU());
        System.out.println(">>> Matrix getP is ");
        Tracer.showAscii(dec.getP());

        Matrix bs = new DenseLocalOnHeapMatrix(new double[][] {
            {4.0, -6.0, 7.0},
            {1.0, 1.0, 1.0}
        });
        System.out.println("\n>>> Matrix to solve: ");
        Tracer.showAscii(bs);

        Matrix sol = dec.solve(bs.transpose());

        System.out.println("\n>>> List of solutions: ");
        for (int i = 0; i < sol.columnSize(); i++)
            Tracer.showAscii(sol.viewColumn(i));

        System.out.println("\n>>> LU decomposition example completed.");
    }
}
