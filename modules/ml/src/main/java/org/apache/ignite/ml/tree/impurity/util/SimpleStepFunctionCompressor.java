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

package org.apache.ignite.ml.tree.impurity.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 * Simple step function compressor.
 *
 * @param <T> Type of step function values.
 */
public class SimpleStepFunctionCompressor<T extends ImpurityMeasure<T>> implements StepFunctionCompressor<T> {
    /** */
    private static final long serialVersionUID = -3231787633598409157L;

    /** Min size of step function to be compressed. */
    private final int minSizeToBeCompressed;

    /** In case of compression min impurity increase that will be recorded. */
    private final double minImpurityIncreaseForRecord;

    /** In case of compression min impurity decrease that will be recorded. */
    private final double minImpurityDecreaseForRecord;

    /**
     * Constructs a new instance of simple step function compressor with default parameters.
     */
    public SimpleStepFunctionCompressor() {
        this(10, 0.1, 0.05);
    }

    /**
     * Constructs a new instance of simple step function compressor.
     *
     * @param minSizeToBeCompressed Min size of step function to be compressed.
     * @param minImpurityIncreaseForRecord In case of compression min impurity increase that will be recorded.
     * @param minImpurityDecreaseForRecord In case of compression min impurity decrease that will be recorded.
     */
    public SimpleStepFunctionCompressor(int minSizeToBeCompressed, double minImpurityIncreaseForRecord,
        double minImpurityDecreaseForRecord) {
        this.minSizeToBeCompressed = minSizeToBeCompressed;
        this.minImpurityIncreaseForRecord = minImpurityIncreaseForRecord;
        this.minImpurityDecreaseForRecord = minImpurityDecreaseForRecord;
    }

    /** {@inheritDoc} */
    @Override public StepFunction<T> compress(StepFunction<T> function) {
        double[] arguments = function.getX();
        T[] values = function.getY();

        if (arguments.length >= minSizeToBeCompressed) {
            List<StepFunctionPoint> points = new ArrayList<>();

            for (int i = 0; i < arguments.length; i++)
                points.add(new StepFunctionPoint(arguments[i], values[i]));

            points = compress(points);

            double[] resX = new double[points.size()];
            T[] resY = Arrays.copyOf(values, points.size());

            for (int i = 0; i < points.size(); i++) {
                StepFunctionPoint pnt = points.get(i);
                resX[i] = pnt.x;
                resY[i] = pnt.y;
            }

            return new StepFunction<>(resX, resY);
        }

        return function;
    }

    /**
     * Compresses list of step function points.
     *
     * @param points Step function points.
     * @return Compressed step function points.
     */
    private List<StepFunctionPoint> compress(List<StepFunctionPoint> points) {
        List<StepFunctionPoint> res = new ArrayList<>();

        double minImpurity = Double.MAX_VALUE, maxImpurity = Double.MIN_VALUE;
        for (int i = 0; i < points.size(); i++) {
            StepFunctionPoint pnt = points.get(i);

            double impurity = pnt.y.impurity();

            if (impurity > maxImpurity)
                maxImpurity = impurity;

            if (impurity < minImpurity)
                minImpurity = impurity;
        }

        Double prev = null;
        for (StepFunctionPoint pnt : points) {
            double impurity = (pnt.y.impurity() - minImpurity) / (maxImpurity - minImpurity);
            if (prev == null ||
                prev - impurity >= minImpurityDecreaseForRecord ||
                impurity - prev >= minImpurityIncreaseForRecord) {
                prev = impurity;
                res.add(pnt);
            }
        }

        return res;
    }

    /**
     * Util class that represents step function point.
     */
    private class StepFunctionPoint {
        /** Argument of the step start. */
        private final double x;

        /** Value of the step. */
        private final T y;

        /**
         * Constructs a new instance of util class that represents step function point.
         *
         * @param x Argument of the step start.
         * @param y Value of the step.
         */
        StepFunctionPoint(double x, T y) {
            this.x = x;
            this.y = y;
        }
    }
}
