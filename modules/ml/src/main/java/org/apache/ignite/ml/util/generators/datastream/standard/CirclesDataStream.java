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

import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.function.FunctionWithNoize;
import org.apache.ignite.ml.util.generators.primitives.variable.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.variable.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

public class CirclesDataStream implements DataStreamGenerator {
    private final int countOfCircles;
    private final double minRadius;
    private final double distanceBetweenCircles;

    public CirclesDataStream(int countOfCircles, double minRadius, double distanceBetweenCircles) {
        A.ensure(countOfCircles > 0, "countOfCircles > 0");
        A.ensure(minRadius > 0, "minRadius > 0");
        A.ensure(distanceBetweenCircles > 0, "distanceBetweenCircles > 0");

        this.countOfCircles = countOfCircles;
        this.minRadius = minRadius;
        this.distanceBetweenCircles = distanceBetweenCircles;
    }

    @Override
    public Stream<LabeledVector<Vector, Double>> labeled() {
        VectorGeneratorsFamily.Builder builder = new VectorGeneratorsFamily.Builder();
        for (int i = 0; i < countOfCircles; i++) {
            final double radius = minRadius + distanceBetweenCircles * i;
            final double variance = 0.1 * (i + 1);

            GaussRandomProducer randomProducer = new GaussRandomProducer(0, variance);
            builder = builder.with(new ParametricVectorGenerator(new UniformRandomProducer(-10, 10),
                new FunctionWithNoize<>(t -> radius * Math.sin(t), randomProducer),
                new FunctionWithNoize<>(t -> radius * Math.cos(t), randomProducer)
            ), 1.0);
        }

        return builder.build().asDataStream().labeled();
    }
}
