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

package org.apache.ignite.ml.math.functions;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Compatibility with Apache Mahout.
 */
public final class Functions {
    /** Function that returns {@code Math.abs(a)}. */
    public static final IgniteDoubleFunction<Double> ABS = Math::abs;

    /** Function that returns its argument. */
    public static final IgniteDoubleFunction<Double> IDENTITY = (a) -> a;

    /** Function that returns {@code Math.log(a) / Math.log(2)}. */
    public static final IgniteDoubleFunction<Double> LOG2 = (a) -> Math.log(a) * 1.4426950408889634;

    /** Function that returns {@code -a}. */
    public static final IgniteDoubleFunction<Double> NEGATE = (a) -> -a;

    /** Function that returns {@code  a < 0 ? -1 : a > 0 ? 1 : 0 }. */
    public static final IgniteDoubleFunction<Double> SIGN = (a) -> a < 0.0 ? -1.0 : a > 0.0 ? 1.0 : 0.0;

    /** Function that returns {@code a * a}. */
    public static final IgniteDoubleFunction<Double> SQUARE = (a) -> a * a;

    /** Function that returns {@code  1 / (1 + exp(-a) } */
    public static final IgniteDoubleFunction<Double> SIGMOID = (a) -> 1.0 / (1.0 + Math.exp(-a));

    /** Function that returns {@code  1 / a } */
    public static final IgniteDoubleFunction<Double> INV = (a) -> 1.0 / a;

    /** Function that returns {@code  a * (1-a) } */
    public static final IgniteDoubleFunction<Double> SIGMOIDGRADIENT = (a) -> a * (1.0 - a);

    /** Function that returns {@code a % b}. */
    public static final IgniteBiFunction<Double, Double, Double> MOD = (a, b) -> a % b;

    /** Function that returns {@code a * b}. */
    public static final IgniteBiFunction<Double, Double, Double> MULT = (a, b) -> a * b;

    /** Function that returns {@code Math.log(a) / Math.log(b)}. */
    public static final IgniteBiFunction<Double, Double, Double> LG = (a, b) -> Math.log(a) / Math.log(b);

    /** Function that returns {@code a + b}. */
    public static final IgniteBiFunction<Double, Double, Double> PLUS = (a, b) -> a + b;

    /** Function that returns {@code a - b}. */
    public static final IgniteBiFunction<Double, Double, Double> MINUS = (a, b) -> a - b;

    /** Function that returns {@code min(a, b)}. */
    public static final IgniteBiFunction<Double, Double, Double> MIN = Math::min;

    /** Function that returns {@code abs(a - b)}. */
    public static final IgniteBiFunction<Double, Double, Double> MINUS_ABS = (a, b) -> Math.abs(a - b);

    /** Function that returns {@code max(abs(a), abs(b))}. */
    public static final IgniteBiFunction<Double, Double, Double> MAX_ABS = (a, b) -> Math.max(Math.abs(a), Math.abs(b));

    /**
     * Generic 'max' function.
     *
     * @param a First object to compare.
     * @param b Second object to compare.
     * @param f Comparator.
     * @param <T> Type of objects to compare.
     * @return Maximum between {@code a} and {@code b} in terms of comparator {@code f}.
     */
    public static <T> T MAX_GENERIC(T a, T b, Comparator<T> f) {
        return f.compare(a, b) > 0 ? a : b;
    }

    /**
     * Generic 'min' function.
     *
     * @param a First object to compare.
     * @param b Second object to compare.
     * @param f Comparator.
     * @param <T> Type of objects to compare.
     * @return Minimum between {@code a} and {@code b} in terms of comparator {@code f}.
     */
    public static <T> T MIN_GENERIC(T a, T b, Comparator<T> f) {
        return f.compare(a, b) < 0 ? a : b;
    }

    /** Function that returns {@code min(abs(a), abs(b))}. */
    public static final IgniteBiFunction<Double, Double, Double> MIN_ABS = (a, b) -> Math.min(Math.abs(a), Math.abs(b));

    /** Function that returns {@code Math.abs(a) + Math.abs(b)}. */
    public static final IgniteBiFunction<Double, Double, Double> PLUS_ABS = (a, b) -> Math.abs(a) + Math.abs(b);

    /** Function that returns {@code (a - b) * (a - b)} */
    public static final IgniteBiFunction<Double, Double, Double> MINUS_SQUARED = (a, b) -> (a - b) * (a - b);

    /** Function that returns {@code a &lt; b ? -1 : a &gt; b ? 1 : 0}. */
    public static final IgniteBiFunction<Double, Double, Double> COMPARE = (a, b) -> a < b ? -1.0 : a > b ? 1.0 : 0.0;

    /** */
    public static <A, B extends Comparable<B>> IgniteBiTuple<Integer, A> argmin(List<A> args, IgniteFunction<A, B> f) {
        A res = null;
        B fRes = null;

        if (!args.isEmpty()) {
            res = args.iterator().next();
            fRes = f.apply(res);
        }

        int resInd = 0;
        int i = 0;

        for (A arg : args) {
            B curRes = f.apply(arg);

            if (fRes.compareTo(curRes) > 0) {
                res = arg;
                resInd = i;
                fRes = curRes;
            }

            i++;
        }
        return new IgniteBiTuple<>(resInd, res);
    }

    /**
     * Function that returns {@code a + b}. {@code a} is a variable, {@code b} is fixed.
     *
     * @param b Value to add.
     * @return Function for this operation.
     */
    public static IgniteDoubleFunction<Double> plus(final double b) {
        return (a) -> a + b;
    }

    /**
     * Function that returns {@code a * b}. {@code a} is a variable, {@code b} is fixed.
     *
     * @param b Value to multiply to.
     * @return Function for this operation.
     */
    public static IgniteDoubleFunction<Double> mult(final double b) {
        return (a) -> a * b;
    }

    /**
     * Function that returns {@code a / b}. {@code a} is a variable, {@code b} is fixed.
     *
     * @param b Value to divide to.
     * @return Function for this operation.
     */
    public static IgniteDoubleFunction<Double> div(double b) {
        return mult(1 / b);
    }

    /**
     * Function that returns {@code a + b*constant}. {@code a} and {@code b} are variables,
     * {@code constant} is fixed.
     *
     * @param constant Value to use in multiply.
     * @return Function for this operation.
     */
    public static IgniteBiFunction<Double, Double, Double> plusMult(double constant) {
        return (a, b) -> a + b * constant;
    }

    /**
     * Function that returns {@code a - b*constant}. {@code a} and {@code b} are variables,
     * {@code constant} is fixed.
     *
     * @param constant Value to use in multiply.
     * @return Function for this operation.
     */
    public static IgniteBiFunction<Double, Double, Double> minusMult(double constant) {
        return (a, b) -> a - b * constant;
    }

    /** Function that returns passed constant. */
    public static IgniteDoubleFunction<Double> constant(Double c) {
        return a -> c;
    }

    /**
     * Function that returns {@code Math.pow(a, b)}.
     *
     * @param b Power value.
     * @return Function for given power.
     */
    public static IgniteDoubleFunction<Double> pow(final double b) {
        return (a) -> b == 2 ? a * a : Math.pow(a, b);
    }

    /**
     * Curry bi-function.
     *
     * @param f Bi-function to curry.
     * @param <A> Type of first argument of {@code f}.
     * @param <B> Type of second argument of {@code f}.
     * @param <C> Return type of {@code f}.
     * @return Curried bi-function.
     */
    public static <A, B, C> IgniteCurriedBiFunction<A, B, C> curry(BiFunction<A, B, C> f) {
        return a -> b -> f.apply(a, b);
    }

    /**
     * Transform bi-function of the form (a, b) -> c into a function of form a -> (b -> c).
     *
     * @param f Function to be curried.
     * @param <A> Type of first argument of function to be transformed.
     * @param <B> Type of second argument of function to be transformed.
     * @param <C> Type of third argument of function to be transformed.
     * @return Curried bi-function.
     */
    public static <A, B, C> IgniteCurriedBiFunction<A, B, C> curry(IgniteBiFunction<A, B, C> f) {
        return a -> b -> f.apply(a, b);
    }

    /**
     * Transform tri-function of the form (a, b, c) -> d into a function of form a -> (b -> (c -> d)).
     *
     * @param f Function to be curried.
     * @param <A> Type of first argument of function to be transformed.
     * @param <B> Type of second argument of function to be transformed.
     * @param <C> Type of third argument of function to be transformed.
     * @param <D> Type output of function to be transformed.
     * @return Curried tri-function.
     */
    public static <A, B, C, D> IgniteCurriedTriFunction<A, B, C, D> curry(IgniteTriFunction<A, B, C, D> f) {
        return a -> b -> c -> f.apply(a, b, c);
    }

    /**
     * Transform function of form a -> b into a -> (() -> b).
     *
     * @param f Function to be transformed.
     * @param <A> Type of input of function to be transformed.
     * @param <B> Type of output of function to be transformed.
     * @return Transformed function.
     */
    public static <A, B> IgniteFunction<A, IgniteSupplier<B>> outputSupplier(IgniteFunction<A, B> f) {
        return a -> {
            B res = f.apply(a);
            return () -> res;
        };
    }

    /**
     * Transform function of form (a, b) -> c into (a, b) - () -> c.
     *
     * @param f Function to be transformed.
     * @param <A> Type of first argument of function to be transformed.
     * @param <B> Type of second argument of function to be transformed.
     * @param <C> Type of output of function to be transformed.
     * @return Transformed function.
     */
    public static <A, B, C> IgniteBiFunction<A, B, IgniteSupplier<C>> outputSupplier(IgniteBiFunction<A, B, C> f) {
        return (a, b) -> {
            C res = f.apply(a, b);
            return () -> res;
        };
    }
}
