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

package org.apache.ignite.ml.nn;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;

/**
 * Class containing some common activation functions.
 */
public class Activators {
    /**
     * Sigmoid activation function.
     */
    public static IgniteDifferentiableDoubleToDoubleFunction SIGMOID = new IgniteDifferentiableDoubleToDoubleFunction() {
        /** {@inheritDoc} */
        @Override public double differential(double pnt) {
            double v = apply(pnt);
            return v * (1 - v);
        }

        /** {@inheritDoc} */
        @Override public Double apply(double val) {
            return 1 / (1 + Math.exp(-val));
        }
    };

    /**
     * Rectified linear unit (ReLU) activation function.
     */
    public static IgniteDifferentiableDoubleToDoubleFunction RELU = new IgniteDifferentiableDoubleToDoubleFunction() {
        /**
         * Differential of ReLU at pnt. Formally, function is not differentiable at 0, but we let differential at 0 be 0.
         *
         * @param pnt Point to differentiate at.
         * @return Differential at pnt.
         */
        @Override public double differential(double pnt) {
            return pnt > 0 ? 1 : 0;
        }

        /** {@inheritDoc} */
        @Override public Double apply(double val) {
            return Math.max(val, 0);
        }
    };

    /**
     * Linear unit activation function.
     */
    public static IgniteDifferentiableDoubleToDoubleFunction LINEAR = new IgniteDifferentiableDoubleToDoubleFunction() {
        /** {@inheritDoc} */
        @Override public double differential(double pnt) {
            return 1.0;
        }

        /**
         * Differential of linear at pnt.
         *
         * @param pnt Point to differentiate at.
         * @return Differential at pnt.
         */
        @Override public Double apply(double pnt) {
            return pnt;
        }
    };
}
