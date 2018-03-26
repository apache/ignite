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

import java.util.Arrays;

public class RPropTest {
    /** Minimal speed. */
    private static final double minSpeed = 1e-6;

    /** Maximal speed. */
    private static final double maxSpeed = 50;

    /** Speed initial value. */
    private static final double initSpeed = 0.1;

    /** Acceleration rate. */
    private static final double accRate = 1.5;

    /** Deceleration rate. */
    private static final double decRate = 0.6;

    public static void main(String... args) {
        RPropContext ctx = new RPropContext(1, 0.1);

        System.out.println("Test local function x^2 + 3*x^2...");

        double x = 2.0;
        for (int i = 0; i < 50; i++) {
            double[] g = new double[]{8*x};
            double[] s = step(g, ctx);

            System.out.printf("Iteration %d, point x = %1.5f, gradient = %1.5f, step = %1.5f, initSpeed = %1.5f\n",
                i, x, g[0], s[0], ctx.speed[0]);

            x += s[0];
        }

        System.out.println("Test distributed function x^2 + 3*x^2...");
    }

    public static double[] step(double[] gradient, RPropContext ctx) {
        double[] step = new double[gradient.length];

        for (int i = 0; i < gradient.length; i++) {
            if (gradient[i] * ctx.prevGradient[i] > 0) {
                ctx.speed[i] = Math.min(ctx.speed[i] * accRate, maxSpeed);
                step[i] = -Math.signum(gradient[i]) * ctx.speed[i];
            }
            else if (gradient[i] * ctx.prevGradient[i] < 0) {
                ctx.speed[i] = Math.max(ctx.speed[i] * decRate, minSpeed);
                step[i] = -ctx.prevStep[i];
                gradient[i] = 0;
            }
            else
                step[i] = - Math.signum(gradient[i]) * ctx.speed[i];
        }

        ctx.prevGradient = gradient;
        ctx.prevStep = step;

        return step;
    }

    private static class RPropContext {

        private double[] speed;

        private double[] prevGradient;

        private double[] prevStep;

        private RPropContext(int size, double initSpeed) {
            speed = new double[size];
            prevGradient = new double[size];
            prevStep = new double[size];

            for (int i = 0; i < size; i++)
                speed[i] = initSpeed;
        }

        private RPropContext copy() {
            RPropContext copy = new RPropContext(0, 0);
            copy.speed = Arrays.copyOf(speed, speed.length);
            copy.prevGradient = Arrays.copyOf(prevGradient, prevGradient.length);
            copy.prevStep = Arrays.copyOf(prevStep, prevStep.length);
            return copy;
        }
    }
}
