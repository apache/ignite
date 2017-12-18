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
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link QRDecomposition}.
 */
public class QRDecompositionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> QR decomposition example started.");
        Matrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        });

        System.out.println("\n>>> Input matrix:");
        Tracer.showAscii(m);

        QRDecomposition dec = new QRDecomposition(m);
        System.out.println("\n>>> Value for full rank in decomposition: [" + dec.hasFullRank() + "].");

        Matrix q = dec.getQ();
        Matrix r = dec.getR();

        System.out.println("\n>>> Orthogonal matrix Q:");
        Tracer.showAscii(q);
        System.out.println("\n>>> Upper triangular matrix R:");
        Tracer.showAscii(r);

        Matrix qSafeCp = safeCopy(q);

        Matrix identity = qSafeCp.times(qSafeCp.transpose());

        System.out.println("\n>>> Identity matrix obtained from Q:");
        Tracer.showAscii(identity);

        Matrix recomposed = qSafeCp.times(r);

        System.out.println("\n>>> Recomposed input matrix:");
        Tracer.showAscii(recomposed);

        Matrix sol = dec.solve(new DenseLocalOnHeapMatrix(3, 10));

        System.out.println("\n>>> Solved matrix:");
        Tracer.showAscii(sol);

        dec.destroy();

        System.out.println("\n>>> QR decomposition example completed.");
    }

    /** */
    private static Matrix safeCopy(Matrix orig) {
        return new DenseLocalOnHeapMatrix(orig.rowSize(), orig.columnSize()).assign(orig);
    }
}
