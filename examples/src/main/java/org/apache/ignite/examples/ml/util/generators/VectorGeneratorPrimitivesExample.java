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
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.DiscreteRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives;

/**
 * Example of using primitive generators and combiners for generators.
 */
public class VectorGeneratorPrimitivesExample {
    /**
     * Run example.
     *
     * @param args Args.
     */
    public static void main(String... args) throws IOException {
        // Vectors from ring-like distribution.
        VectorGenerator fullRing = VectorGeneratorPrimitives.ring(10, 0, 2 * Math.PI);
        // Vectors from ring's sector distribution.
        VectorGenerator partOfRing = VectorGeneratorPrimitives.ring(15, -Math.PI / 2, Math.PI);
        // Vectors from distribution having filled circle shape.
        VectorGenerator circle = VectorGeneratorPrimitives.circle(14.5);
        // Vectors from uniform distribution in n-dimensional space.
        VectorGenerator parallelogram = VectorGeneratorPrimitives.parallelogram(VectorUtils.of(10, 15));
        // Vectors from gaussian.
        VectorGenerator gauss = VectorGeneratorPrimitives.gauss(VectorUtils.of(0.0, 0.0), VectorUtils.of(10., 15.));

        Tracer.showClassificationDatasetHtml("Full ring", fullRing.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Sector", partOfRing.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Circle", circle.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Parallelogram", parallelogram.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Gauss", gauss.asDataStream(), 1500, 0, 1, false);

        // Using of rotate for generator.
        VectorGenerator rotatedParallelogram = parallelogram.rotate(-Math.PI / 8);
        Tracer.showClassificationDatasetHtml("Rotated Parallelogram", rotatedParallelogram.asDataStream(), 1500, 0, 1, false);

        // Sum of generators where vectors from first generator are summed with corresponding vectors from second generator.
        VectorGenerator gaussPlusRing = gauss.plus(fullRing);
        Tracer.showClassificationDatasetHtml("Gauss plus ring", gaussPlusRing.asDataStream(), 1500, 0, 1, false);

        // Example of vector generator filtering.
        VectorGenerator filteredCircle = circle.filter(v -> Math.abs(v.get(0)) > 5);
        Tracer.showClassificationDatasetHtml("Filtered circle", filteredCircle.asDataStream(), 1500, 0, 1, false);

        // Example of using map function for vector generator.
        VectorGenerator mappedCircle = circle.map(v -> v.get(1) < 0 ? v : v.times(VectorUtils.of(2, 4)));
        Tracer.showClassificationDatasetHtml("Mapped circle", mappedCircle.asDataStream(), 1500, 0, 1, false);

        // Example of generators concatenation where each vector of first generator are concatenated with corresponding
        // vector from second generator.
        DataStreamGenerator ringAndGauss = fullRing.concat(gauss).asDataStream();
        Tracer.showClassificationDatasetHtml("Ring and gauss [x1, x2]", ringAndGauss, 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x2, x3]", ringAndGauss, 1500, 1, 2, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x3, x4]", ringAndGauss, 1500, 2, 3, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x4, x1]", ringAndGauss, 1500, 3, 0, false);

        // Example of vector generator function noize.
        VectorGenerator noisifiedRing = fullRing.noisify(new DiscreteRandomProducer(0.1, 0.2, 0.3, 0.4));
        Tracer.showClassificationDatasetHtml("Noisified ring", noisifiedRing.asDataStream(), 1500, 0, 1, false);

        // Example of complex distribution with "axe" shape.
        VectorGenerator axeBlade = circle.filter(v -> Math.abs(v.get(1)) > 5.)
            .rotate(Math.PI / 4).filter(v -> Math.abs(v.get(0)) > 1.5)
            .rotate(-Math.PI / 2).filter(v -> Math.abs(v.get(0)) > 1.5)
            .rotate(Math.PI / 4).filter(v -> Math.sqrt(v.getLengthSquared()) > 10)
            .map(v -> Math.abs(v.get(0)) > 8 && Math.abs(v.get(1)) < 9 ? v.times(0.5) : v).rotate(Math.PI / 2);

        Tracer.showClassificationDatasetHtml("Axe blade", axeBlade.asDataStream(), 1500, 0, 1, false);
        System.out.flush();
    }
}
