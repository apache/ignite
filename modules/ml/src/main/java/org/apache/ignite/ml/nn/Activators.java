/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
