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
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.regressions.OLSMultipleLinearRegression;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteOLSMultipleLinearRegressionBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static AtomicBoolean startLogged = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (!startLogged.getAndSet(true))
            BenchmarkUtils.println("Starting " + this.getClass().getSimpleName());

        runLongly();

        return true;
    }

    /**
     * Based on OLSMultipleLinearRegressionTest#testLongly.
     */
    private void runLongly() {
        // Y values are first, then independent vars
        // Each row is one observation
        double[] design = new DataChanger.Scale().mutate(new double[] {
            60323, 83.0, 234289, 2356, 1590, 107608, 1947,
            61122, 88.5, 259426, 2325, 1456, 108632, 1948,
            60171, 88.2, 258054, 3682, 1616, 109773, 1949,
            61187, 89.5, 284599, 3351, 1650, 110929, 1950,
            63221, 96.2, 328975, 2099, 3099, 112075, 1951,
            63639, 98.1, 346999, 1932, 3594, 113270, 1952,
            64989, 99.0, 365385, 1870, 3547, 115094, 1953,
            63761, 100.0, 363112, 3578, 3350, 116219, 1954,
            66019, 101.2, 397469, 2904, 3048, 117388, 1955,
            67857, 104.6, 419180, 2822, 2857, 118734, 1956,
            68169, 108.4, 442769, 2936, 2798, 120445, 1957,
            66513, 110.8, 444546, 4681, 2637, 121950, 1958,
            68655, 112.6, 482704, 3813, 2552, 123366, 1959,
            69564, 114.2, 502601, 3931, 2514, 125368, 1960,
            69331, 115.7, 518173, 4806, 2572, 127852, 1961,
            70551, 116.9, 554894, 4007, 2827, 130081, 1962
        });

        final int nobs = 16;
        final int nvars = 6;

        // Estimate the model
        OLSMultipleLinearRegression mdl = new OLSMultipleLinearRegression();
        mdl.newSampleData(design, nobs, nvars, new DenseLocalOnHeapMatrix());

        // Check expected beta values from NIST
        mdl.estimateRegressionParameters();

        // Check expected residuals from R
        mdl.estimateResiduals();

        // Check standard errors from NIST
        mdl.estimateRegressionParametersStandardErrors();

        // Estimate model without intercept
        mdl.setNoIntercept(true);
        mdl.newSampleData(design, nobs, nvars, new DenseLocalOnHeapMatrix());

        // Check expected beta values from R
        mdl.estimateRegressionParameters();

        // Check standard errors from R
        mdl.estimateRegressionParametersStandardErrors();

        // Check expected residuals from R
        mdl.estimateResiduals();
    }
}
