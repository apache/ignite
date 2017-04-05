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

package org.apache.ignite.examples.java8.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Tracer;
import org.apache.ignite.math.decompositions.CholeskyDecomposition;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;

/** */
public class CholeskyDecompositionExample {

    /** */
    public static void main(String[] args) {
        // Let's compute a Cholesky decomposition of Hermitian matrix m:
        // m = l l^{*}, where
        // l is a lower triangular matrix
        // l^{*} is its conjugate transpose

        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][]{
            {2.0d,  -1.0d,  0.0d},
            {-1.0d, 2.0d,  -1.0d},
            {0.0d, -1.0d, 2.0d}
        });

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

        Matrix bs = new DenseLocalOnHeapMatrix(new double[][]{
            {4.0, -6.0, 7.0},
            {1.0, 1.0, 1.0}
        });
        Matrix sol = dec.solve(bs.transpose());

        System.out.println("List of solutions: ");
        for (int i = 0; i < sol.columnSize(); i++)
            Tracer.showAscii(sol.viewColumn(i));
    }

}
