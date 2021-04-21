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

import java.util.Arrays;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 * Step function described by {@code x} and {@code y} points.
 *
 * @param <T> Type of function values.
 */
public class StepFunction<T extends ImpurityMeasure<T>> {
    /** Argument of every steps start. Should be ascending sorted all the time. */
    private final double[] x;

    /** Value of every step. */
    private final T[] y;

    /**
     * Constructs a new instance of step function.
     *
     * @param x Argument of every steps start.
     * @param y Value of every step.
     */
    public StepFunction(double[] x, T[] y) {
        assert x.length == y.length : "Argument and value arrays have to be the same length";

        this.x = x;
        this.y = y;

        sort(x, y, 0, x.length - 1);
    }

    /**
     * Adds the given step function to this.
     *
     * @param b Another step function.
     * @return Sum of this and the given function.
     */
    public StepFunction<T> add(StepFunction<T> b) {
        int resSize = 0, leftPtr = 0, rightPtr = 0;
        double previousPnt = 0;

        while (leftPtr < x.length || rightPtr < b.x.length) {
            if (rightPtr >= b.x.length || (leftPtr < x.length && x[leftPtr] < b.x[rightPtr])) {
                if (resSize == 0 || x[leftPtr] != previousPnt) {
                    previousPnt = x[leftPtr];
                    resSize++;
                }

                leftPtr++;
            }
            else {
                if (resSize == 0 || b.x[rightPtr] != previousPnt) {
                    previousPnt = b.x[rightPtr];
                    resSize++;
                }

                rightPtr++;
            }
        }

        double[] resX = new double[resSize];
        T[] resY = Arrays.copyOf(y, resSize);

        leftPtr = 0;
        rightPtr = 0;

        for (int i = 0; leftPtr < x.length || rightPtr < b.x.length; i++) {
            if (rightPtr >= b.x.length || (leftPtr < x.length && x[leftPtr] < b.x[rightPtr])) {
                boolean override = i > 0 && x[leftPtr] == resX[i - 1];
                int target = override ? i - 1 : i;

                resY[target] = override ? resY[target] : null;
                resY[target] = i > 0 ? resY[i - 1] : null;
                resY[target] = resY[target] == null ? y[leftPtr] : resY[target].add(y[leftPtr]);

                if (leftPtr > 0)
                    resY[target] = resY[target].subtract(y[leftPtr - 1]);

                resX[target] = x[leftPtr];
                i = target;

                leftPtr++;
            }
            else {
                boolean override = i > 0 && b.x[rightPtr] == resX[i - 1];
                int target = override ? i - 1 : i;

                resY[target] = override ? resY[target] : null;
                resY[target] = i > 0 ? resY[i - 1] : null;

                resY[target] = resY[target] == null ? b.y[rightPtr] : resY[target].add(b.y[rightPtr]);

                if (rightPtr > 0)
                    resY[target] = resY[target].subtract(b.y[rightPtr - 1]);

                resX[target] = b.x[rightPtr];
                i = target;

                rightPtr++;
            }
        }

        return new StepFunction<>(resX, resY);
    }

    /** */
    private void sort(double[] x, T[] y, int from, int to) {
        if (from < to) {
            double pivot = x[(from + to) / 2];

            int i = from, j = to;
            while (i <= j) {
                while (x[i] < pivot) i++;
                while (x[j] > pivot) j--;

                if (i <= j) {
                    double tmpX = x[i];
                    x[i] = x[j];
                    x[j] = tmpX;

                    T tmpY = y[i];
                    y[i] = y[j];
                    y[j] = tmpY;

                    i++;
                    j--;
                }
            }

            sort(x, y, from, j);
            sort(x, y, i, to);
        }
    }

    /** */
    public double[] getX() {
        return x;
    }

    /** */
    public T[] getY() {
        return y;
    }
}
