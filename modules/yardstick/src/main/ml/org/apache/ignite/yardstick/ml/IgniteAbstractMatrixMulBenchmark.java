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

package org.apache.ignite.yardstick.ml;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
abstract class IgniteAbstractMatrixMulBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static final int SIZE = 1 << 8;

    /** */
    private static final AtomicBoolean startLogged = new AtomicBoolean(false);

    /** */
    private double[][] dataSquare = createAndFill(SIZE, SIZE);

    /** */
    private double[][] dataRect1 = createAndFill(SIZE / 2, SIZE);

    /** */
    private double[][] dataRect2 = createAndFill(SIZE, SIZE / 2);

    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (!startLogged.getAndSet(true))
            BenchmarkUtils.println("Starting " + this.getClass().getSimpleName());

        final double scale = DataChanger.next();

        // Create IgniteThread, we may want to work with SparseDistributedMatrix inside IgniteThread
        // because we create ignite cache internally.
        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            this.getClass().getSimpleName(), new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                Matrix m1, m2, m3, m4, m5, m6;

                Matrix m7 = (m1 = createAndFill(dataSquare, scale)).times((m2 = createAndFill(dataSquare, scale)));
                Matrix m8 = (m3 = createAndFill(dataRect1, scale)).times((m4 = createAndFill(dataRect2, scale)));
                Matrix m9 = (m5 = createAndFill(dataRect2, scale)).times((m6 = createAndFill(dataRect1, scale)));

                m1.destroy();
                m2.destroy();
                m3.destroy();
                m4.destroy();
                m5.destroy();
                m6.destroy();
                m7.destroy();
                m8.destroy();
                m9.destroy();
            }
        });

        igniteThread.start();

        igniteThread.join();

        return true;
    }

    /** Override in subclasses with specific type Matrix. */
    abstract Matrix newMatrix(int rowSize, int colSize);

    /** */
    private Matrix createAndFill(double[][] data, double scale) {
        Matrix res = newMatrix(data.length, data[0].length).assign(data);

        for (int i = 0; i < data.length && i < data[0].length; i++)
            res.set(i, i, data[i][i] * scale);

        return res;
    }

    /** */
    private double[][] createAndFill(int rowSize, int colSize) {
        double[][] data = new double[rowSize][colSize];

        for (int i = 0; i < rowSize; i++)
            for (int j = 0; j < colSize; j++)
                data[i][j] = -0.5d + Math.random();

        return data;
    }
}
