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

package org.apache.ignite.ml.tree.impurity.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 *
 *
 * @param <T> Type of step function values.
 */
public class SimpleStepFunctionCompressor<T extends ImpurityMeasure<T>> implements StepFunctionCompressor<T> {
    /** {@inheritDoc} */
    @Override public StepFunction<T> compress(StepFunction<T> function) {
        double[] arguments = function.getX();
        T[] values = function.getY();

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

    /**
     * Compresses list of step function points.
     *
     * @param points Step function points.
     * @return Compressed step function points.
     */
    private List<StepFunctionPoint> compress(List<StepFunctionPoint> points) {
        return points;
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
