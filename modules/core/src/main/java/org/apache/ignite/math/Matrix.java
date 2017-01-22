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

package org.apache.ignite.math;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import java.util.*;
import java.util.function.*;

/**
 * A matrix interface.
 *
 * Based on its flavor it can have vastly different implementations tailored for
 * for different types of data (e.g. dense vs. sparse), different sizes of data or different operation
 * optimizations.
 *
 * Note also that not all operations can be supported by all underlying implementations. If an operation is not
 * supported a {@link UnsupportedOperationException} is thrown. This exception can also be thrown in partial cases
 * where an operation is unsupported only in special cases, e.g. where a given operation cannot be deterministically
 * completed in polynomial time.
 *
 * Based on ideas from <a href="http://mahout.apache.org/">Apache Mahout</a>.
 */
public interface Matrix {
    /**
     * Assigns given value to all elements of this matrix.
     *
     * @param val Value to assign to all elements.
     * @return This matrix.
     */
    Matrix assign(double val);

    /**
     * Assigns given values to this matrix.
     *
     * @param vals Values to assign.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix assign(double[][] vals);

    /**
     * Assigns values from given matrix to this matrix.
     *
     * @param mtx Matrix to assign to this matrix.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix assign(Matrix mtx);

    /**
     * Maps all values in this matrix through a given function.
     *
     * @param fun Mapping function.
     * @return This matrix.
     */
    Matrix map(DoubleFunction<Double> fun);

    /**
     * Maps all values in this matrix through a given function.
     *
     * For this matrix <code>A</code>, argument matrix <code>B</code> and the
     * function <code>F</code> this method maps every cell <code>x, y</code> as:
     * <code>A(x,y) = fun(A(x,y), B(x,y))</code>
     *
     * @param mtx Argument matrix.
     * @param fun Mapping function.
     * @return This function.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix map(Matrix mtx, BiFunction<Double, Double, Double> fun);

    /**
     * Assigns values from given vector to the specified column in this matrix.
     *
     * @param col Column index.
     * @param vec Vector to get values from.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix assignColumn(int col, Vector vec);

    /**
     * Assigns values from given vector to the specified row in this matrix.
     *
     * @param row Row index.
     * @param vec Vector to get values from.
     * @return This matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix assignRow(int row, Vector vec);

    /**
     * Collects the results of applying a given function to all rows in this matrix.
     *
     * @param fun Aggregating function.
     * @return Vector of row aggregates.
     */
    Vector aggregateRows(Function<Vector, Double> fun);

    /**
     * Collects the results of applying a given function to all columns in this matrix.
     *
     * @param fun Aggregating function.
     * @return Vector of column aggregates.
     */
    Vector aggregateColumns(Function<Vector, Double> fun);

    /**
     * Folds this matrix into a single value.
     *
     * @param foldFun Folding function that takes two parameters: accumulator and the current value.
     * @param mapFun Mapping function that is called on each matrix cell before its passed to the accumulator
     *      (as its second parameter).
     * @return Folded value of this matrix.
     * @param <T> Type of the folded value.
     */
    <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction mapFun);

    /**
     * Gets number of columns in this matrix.
     *
     * @return The number of columns in this matrix.
     */
    int columnSize();

    /**
     * Gets number of rows in this matrix.
     *
     * @return The number of rows in this matrix.
     */
    int rowSize();

    /**
     * Returns matrix determinator using Laplace theorem.
     *
     * @return A determinator for this matrix.
     */
    double determinant();

    /**
     * Divides each value in this matrix by the argument.
     *
     * @param x Divider value.
     * @return This matrix.
     */
    Matrix divide(double x);

    /**
     * Gets the matrix value at the provided location.
     *
     * @param row Row index.
     * @param col Column index.
     * @return Matrix value.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    double get(int row, int col);

    /**
     * Gets the matrix value at the provided location without checking boundaries.
     * This method is marginally quicker than its {@link #get(int, int)} sibling.
     *
     * @param row Row index.
     * @param col Column index.
     * @return Matrix value.
     */
    double getX(int row, int col);

    /**
     * Creates new empty matrix of the same underlying class and the same size as this matrix.
     *
     * NOTE: new matrix will have the same flavor as the this matrix but a different ID.
     *
     * @return New matrix of the same size using the same underlying class.
     */
    Matrix cloneEmpty();

    /**
     * Clones this matrix.
     *
     * NOTE: new matrix will have the same flavor as the this matrix but a different ID.
     *
     * @return New matrix of the same underlying class, the same size and the same values.
     */
    Matrix clone();

    /**
     * Creates new empty matrix of the same underlying class but of different size.
     *
     * NOTE: new matrix will have the same flavor as the this matrix but a different ID.
     *
     * @param rows Number of rows for new matrix.
     * @param cols Number of columns for new matrix.
     * @return New matrix of the same underlying class and size.
     */
    Matrix cloneEmpty(int rows, int cols);

    /**
     * Creates new matrix where each value is a difference between corresponding value of this matrix and
     * passed in argument matrix.
     *
     * @param mtx Argument matrix.
     * @return New matrix of the same underlying class and size.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix minus(Matrix mtx);

    /**
     * Creates new matrix where each value is a sum of the corresponding value of this matrix and
     * argument value.
     *
     * @param x Value to add.
     * @return New matrix of the same underlying class and size.
     */
    Matrix plus(double x);

    /**
     * Creates new matrix where each value is a sum of corresponding values of this matrix and
     * passed in argument matrix.
     *
     * @param mtx Argument matrix.
     * @return New matrix of the same underlying class and size.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix plus(Matrix mtx);

    /**
     * Auto-generated globally unique matrix ID.
     *
     * @return Matrix GUID.
     */
    IgniteUuid guid();

    /**
     * Sets given value.
     *
     * @param row Row index.
     * @param col Column index.
     * @param val Value to set.
     * @return This matrix.
     * @throws IndexException  Thrown in case of either index is out of bound.
     */
    Matrix set(int row, int col, double val);

    /**
     * Sets values for given row.
     *
     * @param row Row index.
     * @param data Row data to set.
     * @return This matrix.
     * @throws IndexException  Thrown in case of index is out of bound.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix setRow(int row, double[] data);

    /**
     * Sets values for given column.
     *
     * @param col Column index.
     * @param data Column data to set.
     * @return This matrix.
     * @throws IndexException  Thrown in case of index is out of bound.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix setColumn(int col, double[] data);

    /**
     * Sets given value without checking for index bounds. This method is marginally faster
     * than its {@link #set(int, int, double)} sibling.
     *
     * @param row Row index.
     * @param col Column index.
     * @param val Value to set.
     * @return This matrix.
     */
    Matrix setX(int row, int col, double val);

    /**
     * Creates new matrix containing the product of given value and values in this matrix.
     *
     * @param x Value to multiply.
     * @return New matrix.
     */
    Matrix times(double x);

    /**
     * Creates new matrix that is the product of multiplying this matrix and the argument matrix.
     * 
     * @param mtx Argument matrix.
     * @return New matrix.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Matrix times(Matrix mtx);

    /**
     * Gets sum of all elements in the matrix.
     *
     * @return Sum of all elements in this matrix.
     */
    double sum();

    /**
     * Creates new matrix that is transpose of this matrix.
     *
     * @return New transposed matrix.
     */
    Matrix transpose();

    /**
     * Creates new view into this matrix. Changes to the view will be propagated to this matrix.
     *
     * @param offset View offset as <code>int[x,y]</code>.
     * @param size View size as <code>int[rows, cols]</code>
     * @return New view.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     * @throws IndexException Thrown in case of offset is out of bound.
     */
    Matrix viewPart(int[] offset, int[] size);

    /**
     * Creates new view into matrix row. Changes to the view will be propagated to this matrix.
     *
     * @param row Row index.
     * @return New view.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    Vector viewRow(int row);

    /**
     * Creates new view into matrix column . Changes to the view will be propagated to this matrix.
     *
     * @param col Column index.
     * @return New view.
     * @throws IndexException Thrown in case of index is out of bound.
     */
    Vector viewColumn(int col);

    /**
     * Creates new view into matrix diagonal. Changes to the view will be propagated to this matrix.
     *
     * @return New view.
     */
    Vector viewDiagonal();

    /**
     *
     * @return Matrix flavor.
     */
    String flavor();

    /**
     * Gets optional cluster group this matrix is stored on. In case of local JVM storage it may
     * return an empty option or a cluster group consisting of only the local Ignite node.
     *
     * @return Optional cluster group for this matrix to be stored on.
     */
    Optional<ClusterGroup> clusterGroup();
}
