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
public interface Vector {
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
         * @param value Value to set.
         */
        void set(double value);
    }

    /**
     * Gets cardinality of this vector (maximum number of the elements).
     *
     * @return This vector's cardinality.
     */
    int size();

    /**
     * Checks if this implementation should be considered dense so that it explicitly
     * represents every value.
     *
     * @return Density flag.
     */
    boolean isDense();

    /**
     * Checks if this implementation should be considered to be iterable in index order in an efficient way.
     * This implies that methods like {@link #all()} and {@link #nonZeroes()} return elements
     * in ascending order by index.
     *
     * @return Sequential access flag.
     */
    boolean isSequentialAccess();

    /**
     * Creates new copy of this vector.
     *
     * @return New copy vector.
     */
    Vector copy();

    /**
     * Gets iterator over all elements in this vector.
     *
     * NOTE: implementation can choose to reuse {@link Element} instance so you need to copy it
     * if you want to retain it outside of iteration.
     *
     * @return Iterator.
     */
    Iterable<Element> all();

    /**
     * Iterates ove all non-zero elements in this vector.
     *
     * NOTE: implementation can choose to reuse {@link Element} instance so you need to copy it
     * if you want to retain it outside of iteration.
     *
     * @return Iterator.
     */
    Iterable<Element> nonZeroes();

    /**
     * Gets spliterator for all values in this vector.
     *
     * @return Spliterator for all values.
     */
    Spliterator<Double> allSpliterator();

    /**
     * Gets spliterator for all non-zero values in this vector.
     *
     * @return Spliterator for all non-zero values.
     */
    Spliterator<Double> nonZeroSpliterator();

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
    Element getElement(int idx);

    /**
     * Assigns given value to all elements of this vector.
     *
     * @param val Value to assign.
     * @return This vector.
     */
    Vector assign(double val);

    /**
     * Assigns values from given array to this vector.
     *
     * @param vals Values to assign.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     * @return This vector.
     */
    Vector assign(double[] vals);

    /**
     * Copies values from the argument vector to this one.
     *
     * @param vec Argument vector.
     * @return This vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Vector assign(Vector vec);

    /**
     * Assigns each vector element to the value generated by given function.
     *
     * @param fun Function that takes the index and returns value.
     * @return This vector.
     */
    Vector assign(IntToDoubleFunction fun);

    /**
     * Maps all values in this vector through a given function.
     *
     * @param fun Mapping function.
     * @return This vector.
     */
    Vector map(DoubleFunction<Double> fun);

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
    Vector map(Vector vec, BiFunction<Double, Double, Double> fun);

    /**
     * Maps all elements of this vector by applying given function to each element with a constant
     * second parameter <code>y</code>.
     *
     * @param fun Mapping function.
     * @param y Second parameter for mapping function.
     * @return This vector.
     */
    Vector map(BiFunction<Double, Double, Double> fun, double y);

    /**
     * Creates new vector containing values from this vector divided by the argument.
     *
     * @param x Division argument.
     * @return New vector.
     */
    Vector divide(double x);

    /**
     * Gets dot product of two vectors.
     *
     * @param vec Argument vector.
     * @return Dot product of two vectors.
     */
    double dot(Vector vec);

    /**
     * Gets the value at specified index.
     *
     * @param idx Vector index.
     * @return Vector value.
     * @throws IndexException Throw if index is out of bounds.
     */
    double get(int idx);

    /**
     * Gets the value at specified index without checking for index boundaries.
     *
     * @param idx Vector index.
     * @return Vector value.
     */
    double getX(int idx);

    /**
     * Creates new empty vector of the same underlying class but of different cardinality.
     *
     * @param crd Cardinality for new vector.
     * @return New vector.
     */
    Vector like(int crd);

    /**
     * Creates new vector containing element by element difference between this vector and the argument one.
     *
     * @param vec Argument vector.
     * @return New vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Vector minus(Vector vec);

    /**
     * Creates new vector containing the normalized (L_2 norm) values of this vector.
     *
     * @return New vector.
     */
    Vector normalize();

    /**
     * Creates new vector containing the normalized (L_power norm) values of this vector. 
     * See http://en.wikipedia.org/wiki/Lp_space for details.
     *
     * @param power The power to use. Must be >= 0. May also be {@link Double#POSITIVE_INFINITY}.
     * @return New vector {@code x} such that {@code norm(x, power) == 1}
     */
    Vector normalize(double power);

    /**
     * Creates new vector containing the {@code log(1 + entry) / L_2 norm} values of this vector.
     *
     * @return New vector.
     */
    Vector logNormalize();

    /**
     * Creates new vector with a normalized value calculated as {@code log_power(1 + entry) / L_power norm}.
     *
     * @param power The power to use. Must be > 1. Cannot be {@link Double#POSITIVE_INFINITY}.
     * @return New vector
     */
    Vector logNormalize(double power);

    /**
     * Gets the k-norm of the vector. See http://en.wikipedia.org/wiki/Lp_space for more details.
     *
     * @param power The power to use.
     * @see #normalize(double)
     */
    double norm(double power);

    /**
     * Gets minimal element in this vector.
     *
     * @return Minimal element.
     */
    Element minValue();

    /**
     * Gets maximum element in this vector.
     *
     * @return Maximum element.
     */
    Element maxValue();

    /**
     * Creates new vector containing sum of each element in this vector and argument.
     *
     * @param x Argument value.
     * @return New vector.
     */
    Vector plus(double x);

    /**
     * Creates new vector containing element by element sum from both vectors.
     *
     * @param vec Other argument vector to add.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     * @return New vector.
     */
    Vector plus(Vector vec);

    /**
     * Sets value.
     *
     * @param idx Vector index to set value at.
     * @param val Value to set.
     * @throws IndexException Throw if index is out of bounds.
     * @return This vector.
     */
    Vector set(int idx, double val);

    /**
     * Sets value without checking for index boundaries.
     *
     * @param idx Vector index to set value at.
     * @param val Value to set.
     * @return This vector.
     */
    Vector setX(int idx, double val);

    /**
     * Increments value at given index without checking for index boundaries.
     *
     * @param idx Vector index.
     * @param val Increment value.
     * @return This vector.
     */
    Vector incrementX(int idx, double val);

    /**
     * Increments value at given index.
     *
     * @param idx Vector index.
     * @param val Increment value.
     * @throws IndexException Throw if index is out of bounds.
     * @return This vector.
     */
    Vector increment(int idx, double val);

    /**
     * Gets number of non-zero elements in this vector.
     *
     * @return Number of non-zero elements in this vector.
     */
    int nonZeroElements();

    /**
     * Gets a new vector that contains product of each element and the argument.
     * 
     * @param x Multiply argument.
     * @return New vector.
     */
    Vector times(double x);

    /**
     * Gets a new vector that is an element-wie product of this vector and the argument.
     *
     * @param x Vector to multiply by.
     * @return New vector.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Vector times(Vector x);

    /**
     * 
     * @param offset
     * @param length
     * @return
     */
    Vector viewPart(int offset, int length);

    /**
     * Gets the sum of all elements in this vector.
     *
     * @return Vector's sum
     */
    double sum();

    /**
     * Gets the cross product of this vector and the other vector.
     *
     * @param vec Second vector.
     * @return New matrix as a cross product of two vectors.
     */
    Matrix cross(Vector vec);

    /**
     * Folds this vector into a single value.
     *
     * @param foldFun Folding function that takes two parameters: accumulator and the current value.
     * @param mapFun Mapping function that is called on each vector element before its passed to the accumulator
     *      (as its second parameter).
     * @param <T> Type of the folded value.
     * @return Folded value of this vector.
     */
    <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun);

    /**
     * Gets the sum of squares of all elements in this vector.
     *
     * @return Length squared value.
     */
    double getLengthSquared();

    /**
     * Get the square of the distance between this vector and the argument vector.
     *
     * @param vec Another vector.
     * @return Distance squared.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    double getDistanceSquared(Vector vec);

    /**
     * Gets an estimate of the cost *in number of ops* it takes to lookup a random element in this vector.
     *
     * @return Lookup cost in number of ops.
     */
    double getLookupCost();

    /**
     * Checks if adding a non-zero element to this vector is done in a constant time.
     *
     * @return Add constant time flag.
     */
    boolean isAddConstantTime();

    /**
     * Gets optional cluster group this vector is stored on. In case of local JVM storage it may
     * return an empty option or a cluster group consisting of only the local Ignite node.
     *
     * @return Optional cluster group for this vector to be stored on.
     */
    Optional<ClusterGroup> clusterGroup();

    /**
     * Auto-generated globally unique vector ID.
     *
     * @return Vector GUID.
     */
    IgniteUuid guid();
}
