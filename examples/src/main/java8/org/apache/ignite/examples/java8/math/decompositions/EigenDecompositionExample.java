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

import org.apache.ignite.math.decompositions.EigenDecomposition;
import org.apache.ignite.math.functions.Functions;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;

/** */
public class EigenDecompositionExample {

    /** */
    public static void main(String[] args) {
        // Let's compute EigenDecomposition for some square (n x n) matrix m with real eigenvalues:
        // m = v d v^{-1}, where d is diagonal matrix having eigenvalues of m on diagonal
        // and v is matrix where i-th column is eigenvector for i-th eigenvalue (i from 0 to n - 1)
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {0.0d, 1.0d, 0.0d, 0.0d},
            {0.0d, 0.0d, 2.0d, 0.0d},
            {1.0d, 1.0d, 0.0d, 2.0d}
        });

        EigenDecomposition dec = new EigenDecomposition(m);

        // From this decomposition we, for example, can easily compute determinant of matrix m
        // det (m) = det (v d v^{-1}) =
        // det(v) det (d) det(v^{-1}) =
        // det(v) det(v)^{-1} det(d) =
        // det (d) =
        // product of diagonal elements of d =
        // product of eigenvalues
        double det = dec.getRealEigenValues().foldMap(Functions.MULT, Functions.IDENTITY, 1.0);
        System.out.println("Determinant is " + det);
    }

}
