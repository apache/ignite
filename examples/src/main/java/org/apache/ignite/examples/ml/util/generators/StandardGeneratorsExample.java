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

package org.apache.ignite.examples.ml.util.generators;

import java.io.IOException;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.generators.standard.GaussianMixtureDataStream;
import org.apache.ignite.ml.util.generators.standard.RegressionDataStream;
import org.apache.ignite.ml.util.generators.standard.RingsDataStream;
import org.apache.ignite.ml.util.generators.standard.TwoSeparableClassesDataStream;

public class StandardGeneratorsExample {

    public static void main(String... args) throws IOException {
        GaussianMixtureDataStream gaussianMixture = new GaussianMixtureDataStream.Builder()
            .add(VectorUtils.of(0., 0.), VectorUtils.of(1, 0.1))
            .add(VectorUtils.of(0., -10.), VectorUtils.of(2, 0.1))
            .add(VectorUtils.of(0., -20.), VectorUtils.of(4, 0.1))
            .add(VectorUtils.of(0., 10.), VectorUtils.of(0.05, 0.1))
            .add(VectorUtils.of(0., 20.), VectorUtils.of(0.025, 0.1))
            .add(VectorUtils.of(-10., 0.), VectorUtils.of(0.1, 2))
            .add(VectorUtils.of(-20., 0.), VectorUtils.of(0.1, 4))
            .add(VectorUtils.of(10., 0.), VectorUtils.of(0.1, 0.05))
            .add(VectorUtils.of(20., 0.), VectorUtils.of(0.1, 0.025))
            .build();

        Tracer.showClassificationDatasetHtml("Gaussian mixture", gaussianMixture, 2500, 0, 1, true);

        RingsDataStream ringsDataStream = new RingsDataStream(7, 5.0, 5.0);
        Tracer.showClassificationDatasetHtml("Rings", ringsDataStream, 1500, 0, 1, true);

        TwoSeparableClassesDataStream linearySeparableClasses1 = new TwoSeparableClassesDataStream(0., 20.);
        TwoSeparableClassesDataStream linearySeparableClasses2 = new TwoSeparableClassesDataStream(5., 20.);
        TwoSeparableClassesDataStream linearySeparableClasses3 = new TwoSeparableClassesDataStream(-5., 20.);
        Tracer.showClassificationDatasetHtml("Two separable classes (margin = 0.0)", linearySeparableClasses1, 1500, 0, 1, true);
        Tracer.showClassificationDatasetHtml("Two separable classes (margin = 5.0)", linearySeparableClasses2, 1500, 0, 1, true);
        Tracer.showClassificationDatasetHtml("Two separable classes (margin = -5.0)", linearySeparableClasses3, 1500, 0, 1, true);

        RegressionDataStream regression = RegressionDataStream.twoDimensional(x -> Math.abs(x * x - 10), -10, 10);
        Tracer.showRegressionDatasetInHtml("|x^2 - 10|", regression, 1000, 0);
    }
}
