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
import org.apache.ignite.ml.math.decompositions.SingularValueDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link SingularValueDecomposition}.
 */
public class SingularValueDecompositionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> Singular value decomposition (SVD) example started.");

        // Let's compute a SVD of (l x k) matrix m. This decomposition can be thought as extension of EigenDecomposition to
        // rectangular matrices. The factorization we get is following:
        // m = u * s * v^{*}, where
        // u is a real or complex unitary matrix
        // s is a rectangular diagonal matrix with non-negative real numbers on diagonal (this numbers are singular values of m)
        // v is a real or complex unitary matrix
        // If m is real then u and v are also real.
        // Complex case is not supported for the moment.
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 0.0d, 0.0d, 0.0d, 2.0d},
            {0.0d, 0.0d, 3.0d, 0.0d, 0.0d},
            {0.0d, 0.0d, 0.0d, 0.0d, 0.0d},
            {0.0d, 2.0d, 0.0d, 0.0d, 0.0d}
        });
        System.out.println("\n>>> Matrix m for decomposition: ");
        Tracer.showAscii(m);

        SingularValueDecomposition dec = new SingularValueDecomposition(m);
        System.out.println("\n>>> Made decomposition m = u * s * v^{*}.");
        System.out.println(">>> Matrix u is ");
        Tracer.showAscii(dec.getU());
        System.out.println(">>> Matrix s is ");
        Tracer.showAscii(dec.getS());
        System.out.println(">>> Matrix v is ");
        Tracer.showAscii(dec.getV());

        // This decomposition can in particular help with solving problem of finding x minimizing 2-norm of m x such
        // that 2-norm of x is 1. It appears that it is the right singular vector corresponding to minimal singular
        // value, which is always last.
        System.out.println("\n>>> Vector x minimizing 2-norm of m x such that 2 norm of x is 1: ");
        Tracer.showAscii(dec.getV().viewColumn(dec.getSingularValues().length - 1));

        System.out.println("\n>>> Singular value decomposition (SVD) example completed.");
    }
}
