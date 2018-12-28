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

public class VectorGeneratorPrimitivesExample {
    public static void main(String... args) throws IOException {
        VectorGenerator fullRing = VectorGeneratorPrimitives.ring(10, 0, 2 * Math.PI);
        VectorGenerator partOfRing = VectorGeneratorPrimitives.ring(15, -Math.PI / 2, Math.PI);
        VectorGenerator circle = VectorGeneratorPrimitives.circle(14.5);
        VectorGenerator parallelogram = VectorGeneratorPrimitives.parallelogram(VectorUtils.of(10, 15));
        VectorGenerator gauss = VectorGeneratorPrimitives.gauss(VectorUtils.of(0.0, 0.0), VectorUtils.of(10., 15.));

        Tracer.showClassificationDatasetHtml("Full ring", fullRing.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Sector", partOfRing.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Circle", circle.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Paralellogram", parallelogram.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Gauss", gauss.asDataStream(), 1500, 0, 1, false);

        VectorGenerator rotatedParallelogram = parallelogram.rotate(-Math.PI / 8);
        VectorGenerator gaussPlusRing = gauss.plus(fullRing);
        VectorGenerator filteredCircle = circle.filter(v -> Math.abs(v.get(0)) > 5);
        VectorGenerator mappedCircle = circle.map(v -> v.get(1) < 0 ? v : v.times(VectorUtils.of(2, 4)));

        Tracer.showClassificationDatasetHtml("Rotated paralellogram", rotatedParallelogram.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Gauss plus ring", gaussPlusRing.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Filtered circle", filteredCircle.asDataStream(), 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Mapped circle", mappedCircle.asDataStream(), 1500, 0, 1, false);

        VectorGenerator axeBlade = circle.filter(v -> Math.abs(v.get(1)) > 5.)
            .rotate(Math.PI / 4).filter(v -> Math.abs(v.get(0)) > 1.5)
            .rotate(-Math.PI / 2).filter(v -> Math.abs(v.get(0)) > 1.5)
            .rotate(Math.PI / 4).filter(v -> Math.sqrt(v.getLengthSquared()) > 10)
            .map(v -> {
                if(Math.abs(v.get(0)) > 8 && Math.abs(v.get(1)) < 9)
                    return v.times(0.5);
                else
                    return v;
            }).rotate(Math.PI / 2);

        Tracer.showClassificationDatasetHtml("Axe blade", axeBlade.asDataStream(), 1500, 0, 1, false);

        DataStreamGenerator ringAndGauss = fullRing.concat(gauss).asDataStream();
        Tracer.showClassificationDatasetHtml("Ring and gauss [x1, x2]", ringAndGauss, 1500, 0, 1, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x2, x3]", ringAndGauss, 1500, 1, 2, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x3, x4]", ringAndGauss, 1500, 2, 3, false);
        Tracer.showClassificationDatasetHtml("Ring and gauss [x4, x1]", ringAndGauss, 1500, 3, 0, false);

        VectorGenerator noisifiedRing = fullRing.noisify(new DiscreteRandomProducer(0.1, 0.2, 0.3, 0.4));
        Tracer.showClassificationDatasetHtml("Noisified ring", noisifiedRing.asDataStream(), 1500, 0, 1, false);
    }
}
