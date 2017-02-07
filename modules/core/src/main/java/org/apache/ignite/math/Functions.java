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

import java.util.function.*;

/**
 * Compatibility with Apache Mahout.
 */
public final class Functions {
    /** Function that returns its argument. */
    public static final DoubleFunction<Double> IDENTITY = (a) -> a;

    /** Function that returns <tt>1.0 / a</tt>. */
    public static final DoubleFunction<Double> INV = (a) -> 1.0 / a;

    /** Function that returns <tt>Math.log(a) / Math.log(2)</tt>. */
    public static final DoubleFunction<Double> LOG2 = (a) -> Math.log(a) * 1.4426950408889634;

    /** Function that returns <tt>-a</tt>. */
    public static final DoubleFunction<Double> NEGATE = (a) -> -a;

    /** Function that returns {@code a < 0 ? -1 : a > 0 ? 1 : 0}. */
    public static final DoubleFunction<Double> SIGN = (a) -> a < 0.0 ? -1.0 : a > 0.0 ? 1.0 : 0.0;

    /** Function that returns <tt>a * a</tt>. */
    public static final DoubleFunction<Double> SQUARE = (a) -> a * a;

    /** Function that returns <tt> 1 / (1 + exp(-a) </tt> */
    public static final DoubleFunction<Double> SIGMOID = (a) -> 1.0 / (1.0 + Math.exp(-a));

    /** Function that returns <tt> a * (1-a) </tt> */
    public static final DoubleFunction<Double> SIGMOIDGRADIENT = (a) -> a * (1.0 - a);

    /** Function that returns <tt>a % b</tt>. */
    public static final BiFunction<Double, Double, Double> MOD = (a, b) -> a % b;

    /** Function that returns <tt>a * b</tt>. */
    public static final BiFunction<Double, Double, Double> MULT = (a, b) -> a * b;

    /** Function that returns <tt>a + b</tt>. */
    public static final BiFunction<Double, Double, Double> PLUS = (a, b) ->  a + b;

    /** Function that returns <tt>a + b</tt>. */
    public static final BiFunction<Double, Double, Double> MINUS = (a, b) ->  a - b;

    /** Function that returns <tt>Math.abs(a) + Math.abs(b)</tt>. */
    public static final BiFunction<Double, Double, Double> PLUS_ABS = (a, b) -> Math.abs(a) + Math.abs(b);

    /** Function that returns <tt>(a - b) * (a - b)</tt> */
    public static final BiFunction<Double, Double, Double> MINUS_SQUARED = (a, b) -> (a - b) * (a - b);
    
    /**
     * Function that returns <tt>a &lt; b ? -1 : a &gt; b ? 1 : 0</tt>.
     */
    public static final BiFunction<Double, Double, Double> COMPARE = (a, b) -> {
        return a < b ? -1.0 : a > b ? 1.0 : 0.0;
    };

    /**
     * Function that returns <tt>a + b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed.
     *
     * @param b
     * @return
     */
    public static DoubleFunction<Double> plus(final double b) {
        return (a) -> a + b;
    }

    /**
     * Function that returns <tt>a * b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed.
     *
     * @param b
     * @return
     */
    public static DoubleFunction<Double> mult(final double b) {
        return (a) -> a * b;
    }

    /**
     * 
     * @param b
     * @return
     */
    public static DoubleFunction<Double> pow(final double b) {
        return (a) -> {
            if (b == 2)
                return a * a;
            else
                return Math.pow(a, b);
        };
    }
}
