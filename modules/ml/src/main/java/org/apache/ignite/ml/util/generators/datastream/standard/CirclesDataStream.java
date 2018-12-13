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

package org.apache.ignite.ml.util.generators.datastream.standard;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.datastream.RandomVectorsGenerator;
import org.apache.ignite.ml.util.generators.function.FunctionWithNoize;
import org.apache.ignite.ml.util.generators.function.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.variable.DiscreteRandomProducer;
import org.apache.ignite.ml.util.generators.variable.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.variable.UniformRandomProducer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.Utils;

public class CirclesDataStream implements DataStreamGenerator {
    private final int countOfCircles;
    private final double minRadius;
    private final double distanceBetweenCircles;

    public CirclesDataStream(int countOfCircles, double minRadius, double distanceBetweenCircles) {
        this.countOfCircles = countOfCircles;
        this.minRadius = minRadius;
        this.distanceBetweenCircles = distanceBetweenCircles;
    }

    @Override
    public Stream<LabeledVector<Vector, Double>> labeled() {
        DiscreteRandomProducer selector = new DiscreteRandomProducer(Stream.generate(() -> 1.0 / countOfCircles)
            .mapToDouble(x -> x).limit(countOfCircles).toArray());
        List<ParametricVectorGenerator> circleFamilies = new ArrayList<>();
        for (int i = 0; i < countOfCircles; i++) {
            final double radius = minRadius + distanceBetweenCircles * i;
            final double variance = 0.1 * (i + 1);

            GaussRandomProducer randomProducer = new GaussRandomProducer(0, variance);
            circleFamilies.add(new ParametricVectorGenerator(
                new FunctionWithNoize<>(t -> radius * Math.sin(t), randomProducer),
                new FunctionWithNoize<>(t -> radius * Math.cos(t), randomProducer)
            ));
        }

        return Utils.asStream(new RandomVectorsGenerator(circleFamilies, selector, new UniformRandomProducer(-10, 10)))
            .map(v -> new LabeledVector<>(v.vector(), (double)v.distributionFamilyId()));
    }
}
