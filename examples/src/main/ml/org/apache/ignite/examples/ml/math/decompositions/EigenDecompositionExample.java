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

import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.decompositions.EigenDecomposition;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link EigenDecomposition}.
 */
public class EigenDecompositionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> Eigen decomposition example started.");

        // Let's compute EigenDecomposition for some square (n x n) matrix m with real eigenvalues:
        // m = v d v^{-1}, where d is diagonal matrix having eigenvalues of m on diagonal
        // and v is matrix where i-th column is eigenvector for i-th eigenvalue (i from 0 to n - 1)
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {0.0d, 1.0d, 0.0d, 0.0d},
            {0.0d, 0.0d, 2.0d, 0.0d},
            {1.0d, 1.0d, 0.0d, 2.0d}
        });
        System.out.println("\n>>> Matrix m for decomposition: ");
        Tracer.showAscii(m);

        EigenDecomposition dec = new EigenDecomposition(m);
        System.out.println("\n>>> Made decomposition.");
        System.out.println(">>> Matrix getV is ");
        Tracer.showAscii(dec.getV());
        System.out.println(">>> Matrix getD is ");
        Tracer.showAscii(dec.getD());

        // From this decomposition we, for example, can easily compute determinant of matrix m
        // det (m) = det (v d v^{-1}) =
        // det(v) det (d) det(v^{-1}) =
        // det(v) det(v)^{-1} det(d) =
        // det (d) =
        // product of diagonal elements of d =
        // product of eigenvalues
        double det = dec.getRealEigenValues().foldMap(Functions.MULT, Functions.IDENTITY, 1.0);
        System.out.println("\n>>> Determinant is " + det);

        System.out.println("\n>>> Eigen decomposition example completed.");
    }

}
