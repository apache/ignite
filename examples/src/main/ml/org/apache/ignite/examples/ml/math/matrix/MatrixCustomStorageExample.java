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
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.AbstractMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * This example shows how to create custom {@link Matrix} based on custom {@link MatrixStorage}.
 */
public final class MatrixCustomStorageExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Matrix API usage example started.");

        System.out.println("\n>>> Creating a matrix to be transposed.");
        double[][] data = new double[][] {{1, 2, 3}, {4, 5, 6}};
        Matrix m = new MatrixCustomStorage(data);
        Matrix transposed = m.transpose();

        System.out.println(">>> Matrix: ");
        MatrixExampleUtil.print(m);
        System.out.println(">>> Transposed matrix: ");
        MatrixExampleUtil.print(transposed);

        MatrixExampleUtil.verifyTransposition(m, transposed);

        System.out.println("\n>>> Creating matrices to be multiplied.");
        double[][] data1 = new double[][] {{1, 2}, {3, 4}};
        double[][] data2 = new double[][] {{5, 6}, {7, 8}};

        Matrix m1 = new MatrixCustomStorage(data1);
        Matrix m2 = new MatrixCustomStorage(data2);
        Matrix mult = m1.times(m2);

        System.out.println(">>> First matrix: ");
        MatrixExampleUtil.print(m1);
        System.out.println(">>> Second matrix: ");
        MatrixExampleUtil.print(m2);
        System.out.println(">>> Matrix product: ");
        MatrixExampleUtil.print(mult);

        System.out.println("\n>>> Calculating matrices determinants.");
        double det1 = m1.determinant();
        double det2 = m2.determinant();
        double detMult = mult.determinant();
        boolean detMultIsAsExp = Math.abs(detMult - det1 * det2) < 0.0001d;

        System.out.println(">>> First matrix determinant: [" + det1 + "].");
        System.out.println(">>> Second matrix determinant: [" + det2 + "].");
        System.out.println(">>> Matrix product determinant: [" + detMult
            + "], equals product of two other matrices determinants: [" + detMultIsAsExp + "].");

        System.out.println("Determinant of product matrix [" + detMult
            + "] should be equal to product of determinants [" + (det1 * det2) + "].");

        System.out.println("\n>>> Matrix API usage example completed.");
    }

    /**
     * Example of vector with custom storage, modeled after
     * {@link org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix}.
     */
    static class MatrixCustomStorage extends AbstractMatrix {
        /**
         *
         */
        public MatrixCustomStorage() {
            // No-op.
        }

        /**
         * @param rows Amount of rows in a matrix.
         * @param cols Amount of columns in a matrix.
         */
        MatrixCustomStorage(int rows, int cols) {
            assert rows > 0;
            assert cols > 0;

            setStorage(new ExampleMatrixStorage(rows, cols));
        }

        /**
         * @param mtx Source matrix.
         */
        MatrixCustomStorage(double[][] mtx) {
            assert mtx != null;

            setStorage(new ExampleMatrixStorage(mtx));
        }

        /**
         * @param orig original matrix to be copied.
         */
        private MatrixCustomStorage(MatrixCustomStorage orig) {
            assert orig != null;

            setStorage(new ExampleMatrixStorage(orig.rowSize(), orig.columnSize()));

            assign(orig);
        }

        /** {@inheritDoc} */
        @Override public Matrix copy() {
            return new MatrixCustomStorage(this);
        }

        /** {@inheritDoc} */
        @Override public Matrix like(int rows, int cols) {
            return new MatrixCustomStorage(rows, cols);
        }

        /** {@inheritDoc} */
        @Override public Vector likeVector(int crd) {
            return new DenseLocalOnHeapVector(crd);
        }
    }
}
