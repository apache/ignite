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

package org.apache.ignite.ml.math.primitives.matrix;

import java.io.Externalizable;
import java.util.Spliterator;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.MetaAttributes;
import org.apache.ignite.ml.math.StorageOpsMetrics;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.functions.IntIntToDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * A matrix interface.
 * <p>
 * Based on its flavor it can have vastly different implementations tailored for
 * for different types of data (e.g. dense vs. sparse), different sizes of data or different operation
 * optimizations.</p>
 * <p>
 * Note also that not all operations can be supported by all underlying implementations. If an operation is not
 * supported a {@link UnsupportedOperationException} is thrown. This exception can also be thrown in partial cases
 * where an operation is unsupported only in special cases, e.g. where a given operation cannot be deterministically
 * completed in polynomial time.</p>
 * <p>
 * Based on ideas from <a href="http://mahout.apache.org/">Apache Mahout</a>.</p>
 */
public interface Matrix extends MetaAttributes, Externalizable, StorageOpsMetrics, Destroyable {
    /**
     * Holder for matrix's element.
     */
    interface Element {
        /**
         * Gets element's value.
         *
         * @return The value of this matrix element.
         */
        double get();

        /**
         * Gets element's row index.
         *
         * @return The row index of this element.
         */
        int row();

        /**
         * Gets element's column index.
         *
         * @return The column index of this element.
         */
        int column();

        /**
         * Sets element's value.
         *
         * @param val Value to set.
         */
        void set(double val);
    }

    /**
     * Gets the maximum value in this matrix.
     *
     * @return Maximum value in this matrix.
     */
    public double maxValue();

    /**
     * Gets the minimum value in this matrix.
     *
     * @return Minimum value in this matrix.
     */
    public double minValue();

    /**
     * Gets the maximum element in this matrix.
     *
     * @return Maximum element in this matrix.
     */
    public Element maxElement();

    /**
     * Gets the minimum element in this matrix.
     *
     * @return Minimum element in this matrix.
     */
    public Element minElement();

    /**
     * Gets the matrix's element at the given coordinates.
     *
     * @param row Row index.
     * @param col Column index.
     * @return Element at the given coordinates.
     */
    public Element getElement(int row, int col);

    /**
     * Swaps two rows in this matrix.
     *
     * @param row1 Row #1.
     * @param row2 Row #2.
     * @return This matrix.
     */
    public Matrix swapRows(int row1, int row2);

    /**
     * Swaps two columns in this matrix.
     *
     * @param col1 Column #1.
     * @param col2 Column #2.
     * @return This matrix.
     */
    public Matrix swapColumns(int col1, int col2);

    /**
     * Assigns given value to all elements of this matrix.
     *
     * @param val Value to assign to all elements.
     * @return This matrix.
     */
    public Matrix assign(double val);

    /**
     * Assigns given values to this matrix.
     *
     * @param vals Values to assign.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix assign(double[][] vals);

    /**
     * Assigns values from given matrix to this matrix.
     *
     * @param mtx Matrix to assign to this matrix.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix assign(Matrix mtx);

    /**
     * Assigns each matrix element to the value generated by given function.
     *
     * @param fun Function that takes the row and column and returns the value to assign.
     * @return This matrix.
     */
    public Matrix assign(IntIntToDoubleFunction fun);

    /**
     * Maps all values in this matrix through a given function.
     *
     * @param fun Mapping function.
     * @return This matrix.
     */
    public Matrix map(IgniteDoubleFunction<Double> fun);

    /**
     * Maps all values in this matrix through a given function.
     * <p>
     * For this matrix {@code A}, argument matrix {@code B} and the
     * function {@code F} this method maps every cell {@code x, y} as:
     * {@code A(x,y) = fun(A(x,y), B(x,y))}.</p>
     *
     * @param mtx Argument matrix.
     * @param fun Mapping function.
     * @return This function.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix map(Matrix mtx, IgniteBiFunction<Double, Double, Double> fun);

    /**
     * Gets number of non-zero elements in this matrix.
     *
     * @return Number of non-zero elements in this matrix.
     */
    public int nonZeroElements();

    /**
     * Gets spliterator for all values in this matrix.
     *
     * @return Spliterator for all values.
     */
    public Spliterator<Double> allSpliterator();

    /**
     * Gets spliterator for all non-zero values in this matrix.
     *
     * @return Spliterator for all non-zero values.
     */
    public Spliterator<Double> nonZeroSpliterator();

    /**
     * Assigns values from given vector to the specified column in this matrix.
     *
     * @param col Column index.
     * @param vec Vector to get values from.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix assignColumn(int col, Vector vec);

    /**
     * Assigns values from given vector to the specified row in this matrix.
     *
     * @param row Row index.
     * @param vec Vector to get values from.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix assignRow(int row, Vector vec);

    /**
     * Collects the results of applying a given function to all rows in this matrix.
     *
     * @param fun Aggregating function.
     * @return Vector of row aggregates.
     */
    public Vector foldRows(IgniteFunction<Vector, Double> fun);

    /**
     * Collects the results of applying a given function to all columns in this matrix.
     *
     * @param fun Aggregating function.
     * @return Vector of column aggregates.
     */
    public Vector foldColumns(IgniteFunction<Vector, Double> fun);

    /**
     * Folds this matrix into a single value.
     *
     * @param foldFun Folding function that takes two parameters: accumulator and the current value.
     * @param mapFun Mapping function that is called on each matrix cell before its passed to the accumulator (as its
     * second parameter).
     * @param <T> Type of the folded value.
     * @param zeroVal Zero value for fold function.
     * @return Folded value of this matrix.
     */
    public <T> T foldMap(IgniteBiFunction<T, Double, T> foldFun, IgniteDoubleFunction<Double> mapFun, T zeroVal);

    /**
     * Calculates the density of the matrix based on supplied criteria.
     * Returns {@code true} if this matrix is denser than threshold with at least 80% confidence.
     *
     * @param threshold the threshold value [0, 1] of non-zero elements above which the matrix is considered dense.
     */
    public boolean density(double threshold);

    /**
     * Gets number of columns in this matrix.
     *
     * @return The number of columns in this matrix.
     */
    public int columnSize();

    /**
     * Gets number of rows in this matrix.
     *
     * @return The number of rows in this matrix.
     */
    public int rowSize();

    /**
     * Divides each value in this matrix by the argument.
     *
     * @param x Divider value.
     * @return This matrix.
     */
    public Matrix divide(double x);

    /**
     * Gets the matrix value at the provided location.
     *
     * @param row Row index.
     * @param col Column index.
     * @return Matrix value.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    public double get(int row, int col);

    /**
     * Gets the matrix value at the provided location without checking boundaries.
     * This method is marginally quicker than its {@link #get(int, int)} sibling.
     *
     * @param row Row index.
     * @param col Column index.
     * @return Matrix value.
     */
    public double getX(int row, int col);

    /**
     * Gets matrix storage model.
     */
    public MatrixStorage getStorage();

    /**
     * Clones this matrix.
     * <p>
     * NOTE: new matrix will have the same flavor as the this matrix but a different ID.</p>
     *
     * @return New matrix of the same underlying class, the same size and the same values.
     */
    public Matrix copy();

    /**
     * Creates new empty matrix of the same underlying class but of different size.
     * <p>
     * NOTE: new matrix will have the same flavor as the this matrix but a different ID.</p>
     *
     * @param rows Number of rows for new matrix.
     * @param cols Number of columns for new matrix.
     * @return New matrix of the same underlying class and size.
     */
    public Matrix like(int rows, int cols);

    /**
     * Creates new empty vector of compatible properties (similar or the same flavor) to this matrix.
     *
     * @param crd Cardinality of the vector.
     * @return Newly created empty vector "compatible" to this matrix.
     */
    public Vector likeVector(int crd);

    /**
     * Creates new matrix where each value is a difference between corresponding value of this matrix and
     * passed in argument matrix.
     *
     * @param mtx Argument matrix.
     * @return New matrix of the same underlying class and size.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix minus(Matrix mtx);

    /**
     * Creates new matrix where each value is a sum of the corresponding value of this matrix and
     * argument value.
     *
     * @param x Value to add.
     * @return New matrix of the same underlying class and size.
     */
    public Matrix plus(double x);

    /**
     * Creates new matrix where each value is a sum of corresponding values of this matrix and
     * passed in argument matrix.
     *
     * @param mtx Argument matrix.
     * @return New matrix of the same underlying class and size.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix plus(Matrix mtx);

    /**
     * Auto-generated globally unique matrix ID.
     *
     * @return Matrix GUID.
     */
    public IgniteUuid guid();

    /**
     * Sets given value.
     *
     * @param row Row index.
     * @param col Column index.
     * @param val Value to set.
     * @return This matrix.
     * @throws IndexException Thrown in case of either index is out of bound.
     */
    public Matrix set(int row, int col, double val);

    /**
     * Sets values for given row.
     *
     * @param row Row index.
     * @param data Row data to set.
     * @return This matrix.
     * @throws IndexException Thrown in case of index is out of bound.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix setRow(int row, double[] data);

    /**
     * Get a specific row from matrix.
     *
     * @param row Row index.
     * @return row.
     */
    public Vector getRow(int row);

    /**
     * Sets values for given column.
     *
     * @param col Column index.
     * @param data Column data to set.
     * @return This matrix.
     * @throws IndexException Thrown in case of index is out of bound.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix setColumn(int col, double[] data);

    /**
     * Get a specific row from matrix.
     *
     * @param col Col index.
     * @return Col.
     */
    public Vector getCol(int col);

    /**
     * Sets given value without checking for index bounds. This method is marginally faster
     * than its {@link #set(int, int, double)} sibling.
     *
     * @param row Row index.
     * @param col Column index.
     * @param val Value to set.
     * @return This matrix.
     */
    public Matrix setX(int row, int col, double val);

    /**
     * Creates new matrix containing the product of given value and values in this matrix.
     *
     * @param x Value to multiply.
     * @return New matrix.
     */
    public Matrix times(double x);

    /**
     * Creates new matrix that is the product of multiplying this matrix and the argument matrix.
     *
     * @param mtx Argument matrix.
     * @return New matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Matrix times(Matrix mtx);

    /**
     * Creates new matrix that is the product of multiplying this matrix and the argument vector.
     *
     * @param vec Argument vector.
     * @return New matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector times(Vector vec);

    /**
     * Gets sum of all elements in the matrix.
     *
     * @return Sum of all elements in this matrix.
     */
    public double sum();

    /**
     * Creates new matrix that is transpose of this matrix.
     *
     * @return New transposed matrix.
     */
    public Matrix transpose();

    /**
     * Creates new view into matrix row. Changes to the view will be propagated to this matrix.
     *
     * @param row Row index.
     * @return New view.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    public Vector viewRow(int row);

    /**
     * Creates new view into matrix column . Changes to the view will be propagated to this matrix.
     *
     * @param col Column index.
     * @return New view.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    public Vector viewColumn(int col);

    /**
     * Creates new view into matrix diagonal. Changes to the view will be propagated to this matrix.
     *
     * @return New view.
     */
    public Vector viewDiagonal();

    /**
     * Destroys matrix if managed outside of JVM. It's a no-op in all other cases.
     */
    public default void destroy() {
        // No-op.
    }

    /**
     * Replace matrix entry with value oldVal at (row, col) with result of computing f(row, col, oldVal).
     *
     * @param row Row.
     * @param col Column.
     * @param f Function used for replacing.
     */
    public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f);
}
