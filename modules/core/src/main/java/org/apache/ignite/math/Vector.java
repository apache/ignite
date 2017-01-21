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
     * <code>A(x) = fun(A(x), B(x))</code>
     *
     * @param vec Argument vector.
     * @param fun Mapping function.
     * @return This function.
     * @throws CardinalityException Thrown if cardinalities mismatch.
     */
    Vector map(Vector vec, DoubleFunction<Double> fun);

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
