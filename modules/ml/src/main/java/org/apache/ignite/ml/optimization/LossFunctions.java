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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class containing popular loss functions.
 */
public class LossFunctions {
    /**
     * Mean squared error loss function.
     */
    public static IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> MSE = groundTruth ->
        new IgniteDifferentiableVectorToDoubleFunction() {
            /** {@inheritDoc} */
            @Override public Vector differential(Vector pnt) {
                double multiplier = 2.0 / pnt.size();
                return pnt.minus(groundTruth).times(multiplier);
            }

            /** {@inheritDoc} */
            @Override public Double apply(Vector vector) {
                return groundTruth.copy().map(vector, (a, b) -> {
                    double diff = a - b;
                    return diff * diff;
                }).sum() / (vector.size());
            }
        };
    /**
     * Log loss function.
     */
    public static IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> LOG = groundTruth ->
        new IgniteDifferentiableVectorToDoubleFunction() {
            /** {@inheritDoc} */
            @Override public Vector differential(Vector pnt) {
                double multiplier = 2.0 / pnt.size();
                return pnt.minus(groundTruth).times(multiplier);
            }

            /** {@inheritDoc} */
            @Override public Double apply(Vector vector) {
                return groundTruth.copy().map(vector,
                    (a, b) -> a == 1 ? - Math.log(b) : -Math.log(1 - b)
                ).sum();
            }
        };

    /**
     * L2 loss function.
     */
    public static IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> L2 = groundTruth ->
        new IgniteDifferentiableVectorToDoubleFunction() {
            /** {@inheritDoc} */
            @Override public Vector differential(Vector pnt) {
                double multiplier = 2.0 / pnt.size();
                return pnt.minus(groundTruth).times(multiplier);
            }

            /** {@inheritDoc} */
            @Override public Double apply(Vector vector) {
                return groundTruth.copy().map(vector, (a, b) -> {
                    double diff = a - b;
                    return diff * diff;
                }).sum();
            }
        };

    /**
     * L1 loss function.
     */
    public static IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> L1 = groundTruth ->
        new IgniteDifferentiableVectorToDoubleFunction() {
            /** {@inheritDoc} */
            @Override public Vector differential(Vector pnt) {
                double multiplier = 2.0 / pnt.size();
                return pnt.minus(groundTruth).times(multiplier);
            }

            /** {@inheritDoc} */
            @Override public Double apply(Vector vector) {
                return groundTruth.copy().map(vector, (a, b) -> {
                    double diff = a - b;
                    return Math.abs(diff);
                }).sum();
            }
        };

    /**
     * Hinge loss function.
     */
    public static IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> HINGE = groundTruth ->
        new IgniteDifferentiableVectorToDoubleFunction() {
            /** {@inheritDoc} */
            @Override public Vector differential(Vector pnt) {
                double multiplier = 2.0 / pnt.size();
                return pnt.minus(groundTruth).times(multiplier);
            }

            /** {@inheritDoc} */
            @Override public Double apply(Vector vector) {
                return Math.max(0, 1 - groundTruth.dot(vector));
            }
        };
}
