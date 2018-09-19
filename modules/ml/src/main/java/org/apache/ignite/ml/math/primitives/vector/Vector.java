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

package org.apache.ignite.ml.math.primitives.vector;

import java.io.Externalizable;
import java.util.Spliterator;
import java.util.function.IntToDoubleFunction;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.MetaAttributes;
import org.apache.ignite.ml.math.StorageOpsMetrics;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteIntDoubleToDoubleBiFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;

/**
 * A vector interface.
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
public interface Vector extends MetaAttributes, Externalizable, StorageOpsMetrics, Destroyable {
    /**
     * Holder for vector's element.
     */
    interface Element {
        /**
         * Gets element's value.
         *
         * @return The value of this vector element.
         */
        double get();

        /**
         * Gets element's index in the vector.
         *
         * @return The index of this vector element.
         */
        int index();

        /**
         * Sets element's value.
         *
         * @param val Value to set.
         */
        void set(double val);
    }

    /**
     * Gets cardinality of this vector (maximum number of the elements).
     *
     * @return This vector's cardinality.
     */
    public int size();

    /**
     * Creates new copy of this vector.
     *
     * @return New copy vector.
     */
    public Vector copy();

    /**
     * Gets iterator over all elements in this vector.
     *
     * NOTE: implementation can choose to reuse {@link Element} instance so you need to copy it
     * if you want to retain it outside of iteration.
     *
     * @return Iterator.
     */
    public Iterable<Element> all();

    /**
     * Iterates ove all non-zero elements in this vector.
     *
     * NOTE: implementation can choose to reuse {@link Element} instance so you need to copy it
     * if you want to retain it outside of iteration.
     *
     * @return Iterator.
     */
    public Iterable<Element> nonZeroes();

    /**
     * Gets spliterator for all values in this vector.
     *
     * @return Spliterator for all values.
     */
    public Spliterator<Double> allSpliterator();

    /**
     * Gets spliterator for all non-zero values in this vector.
     *
     * @return Spliterator for all non-zero values.
     */
    public Spliterator<Double> nonZeroSpliterator();

    /**
     * Sorts this vector in ascending order.
     */
    public Vector sort();

    /**
     * Gets element at the given index.
     *
     * NOTE: implementation can choose to reuse {@link Element} instance so you need to copy it
     * if you want to retain it outside of iteration.
     *
     * @param idx Element's index.
     * @return Vector's element at the given index.
     * @throws IndexException Throw if index is out of bounds.
     */
    public Element getElement(int idx);

    /**
     * Assigns given value to all elements of this vector.
     *
     * @param val Value to assign.
     * @return This vector.
     */
    public Vector assign(double val);

    /**
     * Assigns values from given array to this vector.
     *
     * @param vals Values to assign.
     * @return This vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector assign(double[] vals);

    /**
     * Copies values from the argument vector to this one.
     *
     * @param vec Argument vector.
     * @return This vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector assign(Vector vec);

    /**
     * Assigns each vector element to the value generated by given function.
     *
     * @param fun Function that takes the index and returns value.
     * @return This vector.
     */
    public Vector assign(IntToDoubleFunction fun);

    /**
     * Maps all values in this vector through a given function.
     *
     * @param fun Mapping function.
     * @return This vector.
     */
    public Vector map(IgniteDoubleFunction<Double> fun);

    /**
     * Maps all values in this vector through a given function.
     *
     * For this vector <code>A</code>, argument vector <code>B</code> and the
     * function <code>F</code> this method maps every element <code>x</code> as:
     * <code>A(x) = F(A(x), B(x))</code>
     *
     * @param vec Argument vector.
     * @param fun Mapping function.
     * @return This function.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector map(Vector vec, IgniteBiFunction<Double, Double, Double> fun);

    /**
     * Maps all elements of this vector by applying given function to each element with a constant
     * second parameter <code>y</code>.
     *
     * @param fun Mapping function.
     * @param y Second parameter for mapping function.
     * @return This vector.
     */
    public Vector map(IgniteBiFunction<Double, Double, Double> fun, double y);

    /**
     * Creates new vector containing values from this vector divided by the argument.
     *
     * @param x Division argument.
     * @return New vector.
     */
    public Vector divide(double x);

    /**
     * Gets dot product of two vectors.
     *
     * @param vec Argument vector.
     * @return Dot product of two vectors.
     */
    public double dot(Vector vec);

    /**
     * Gets the value at specified index.
     *
     * @param idx Vector index.
     * @return Vector value.
     * @throws IndexException Throw if index is out of bounds.
     */
    public double get(int idx);

    /**
     * Gets the value at specified index without checking for index boundaries.
     *
     * @param idx Vector index.
     * @return Vector value.
     */
    public double getX(int idx);

    /**
     * Creates new empty vector of the same underlying class but of different cardinality.
     *
     * @param crd Cardinality for new vector.
     * @return New vector.
     */
    public Vector like(int crd);

    /**
     * Creates new matrix of compatible flavor with given size.
     *
     * @param rows Number of rows.
     * @param cols Number of columns.
     * @return New matrix.
     */
    public Matrix likeMatrix(int rows, int cols);

    /**
     * Converts this vector into [N x 1] or [1 x N] matrix where N is this vector cardinality.
     *
     * @param rowLike {@code true} for rowLike [N x 1], or {@code false} for column [1 x N] matrix.
     * @return Newly created matrix.
     */
    public Matrix toMatrix(boolean rowLike);

    /**
     * Converts this vector into [N+1 x 1] or [1 x N+1] matrix where N is this vector cardinality.
     * (0,0) element of this matrix will be {@code zeroVal} parameter.
     *
     * @param rowLike {@code true} for rowLike [N+1 x 1], or {@code false} for column [1 x N+1] matrix.
     * @return Newly created matrix.
     */
    public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal);

    /**
     * Creates new vector containing element by element difference between this vector and the argument one.
     *
     * @param vec Argument vector.
     * @return New vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector minus(Vector vec);

    /**
     * Creates new vector containing the normalized (L_2 norm) values of this vector.
     *
     * @return New vector.
     */
    public Vector normalize();

    /**
     * Creates new vector containing the normalized (L_power norm) values of this vector.
     * See http://en.wikipedia.org/wiki/Lp_space for details.
     *
     * @param power The power to use. Must be >= 0. May also be {@link Double#POSITIVE_INFINITY}.
     * @return New vector {@code x} such that {@code norm(x, power) == 1}
     */
    public Vector normalize(double power);

    /**
     * Creates new vector containing the {@code log(1 + entry) / L_2 norm} values of this vector.
     *
     * @return New vector.
     */
    public Vector logNormalize();

    /**
     * Creates new vector with a normalized value calculated as {@code log_power(1 + entry) / L_power norm}.
     *
     * @param power The power to use. Must be > 1. Cannot be {@link Double#POSITIVE_INFINITY}.
     * @return New vector
     */
    public Vector logNormalize(double power);

    /**
     * Gets the k-norm of the vector. See http://en.wikipedia.org/wiki/Lp_space for more details.
     *
     * @param power The power to use.
     * @see #normalize(double)
     */
    public double kNorm(double power);

    /**
     * Gets minimal value in this vector.
     *
     * @return Minimal value.
     */
    public double minValue();

    /**
     * Gets maximum value in this vector.
     *
     * @return Maximum c.
     */
    public double maxValue();

    /**
     * Gets minimal element in this vector.
     *
     * @return Minimal element.
     */
    public Element minElement();

    /**
     * Gets maximum element in this vector.
     *
     * @return Maximum element.
     */
    public Element maxElement();

    /**
     * Creates new vector containing sum of each element in this vector and argument.
     *
     * @param x Argument value.
     * @return New vector.
     */
    public Vector plus(double x);

    /**
     * Creates new vector containing element by element sum from both vectors.
     *
     * @param vec Other argument vector to add.
     * @return New vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector plus(Vector vec);

    /**
     * Sets value.
     *
     * @param idx Vector index to set value at.
     * @param val Value to set.
     * @return This vector.
     * @throws IndexException Throw if index is out of bounds.
     */
    public Vector set(int idx, double val);

    /**
     * Sets value without checking for index boundaries.
     *
     * @param idx Vector index to set value at.
     * @param val Value to set.
     * @return This vector.
     */
    public Vector setX(int idx, double val);

    /**
     * Increments value at given index without checking for index boundaries.
     *
     * @param idx Vector index.
     * @param val Increment value.
     * @return This vector.
     */
    public Vector incrementX(int idx, double val);

    /**
     * Increments value at given index.
     *
     * @param idx Vector index.
     * @param val Increment value.
     * @return This vector.
     * @throws IndexException Throw if index is out of bounds.
     */
    public Vector increment(int idx, double val);

    /**
     * Gets number of non-zero elements in this vector.
     *
     * @return Number of non-zero elements in this vector.
     */
    public int nonZeroElements();

    /**
     * Gets a new vector that contains product of each element and the argument.
     *
     * @param x Multiply argument.
     * @return New vector.
     */
    public Vector times(double x);

    /**
     * Gets a new vector that is an element-wie product of this vector and the argument.
     *
     * @param vec Vector to multiply by.
     * @return New vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public Vector times(Vector vec);

    /**
     * @param off Offset into parent vector.
     * @param len Length of the view.
     */
    public Vector viewPart(int off, int len);

    /**
     * Gets vector storage model.
     */
    public VectorStorage getStorage();

    /**
     * Gets the sum of all elements in this vector.
     *
     * @return Vector's sum
     */
    public double sum();

    /**
     * Gets the cross product of this vector and the other vector.
     *
     * @param vec Second vector.
     * @return New matrix as a cross product of two vectors.
     */
    public Matrix cross(Vector vec);

    /**
     * Folds this vector into a single value.
     *
     * @param foldFun Folding function that takes two parameters: accumulator and the current value.
     * @param mapFun Mapping function that is called on each vector element before its passed to the accumulator (as its
     * second parameter).
     * @param <T> Type of the folded value.
     * @param zeroVal Zero value for fold operation.
     * @return Folded value of this vector.
     */
    public <T> T foldMap(IgniteBiFunction<T, Double, T> foldFun, IgniteDoubleFunction<Double> mapFun, T zeroVal);

    /**
     * Combines & maps two vector and folds them into a single value.
     *
     * @param vec Another vector to combine with.
     * @param foldFun Folding function.
     * @param combFun Combine function.
     * @param <T> Type of the folded value.
     * @param zeroVal Zero value for fold operation.
     * @return Folded value of these vectors.
     * @throws CardinalityException Thrown when cardinalities mismatch.
     */
    public <T> T foldMap(Vector vec, IgniteBiFunction<T, Double, T> foldFun,
        IgniteBiFunction<Double, Double, Double> combFun,
        T zeroVal);

    /**
     * Gets the sum of squares of all elements in this vector.
     *
     * @return Length squared value.
     */
    public double getLengthSquared();

    /**
     * Get the square of the distance between this vector and the argument vector.
     *
     * @param vec Another vector.
     * @return Distance squared.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    public double getDistanceSquared(Vector vec);

    /**
     * Auto-generated globally unique vector ID.
     *
     * @return Vector GUID.
     */
    public IgniteUuid guid();

    /**
     * Replace vector entry with value oldVal at i with result of computing f(i, oldVal).
     *
     * @param i Position.
     * @param f Function used for replacing.
     **/
    public void compute(int i, IgniteIntDoubleToDoubleBiFunction f);


    /**
     * Returns array of doubles corresponds to vector components.
     *
     * @return Array of doubles.
     */
    public default double[] asArray() {
        return getStorage().data();
    }
}
