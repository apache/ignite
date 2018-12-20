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

package org.apache.ignite.ml.util.generators.primitives.vector;

import java.util.stream.DoubleStream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.generators.primitives.variable.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.variable.RandomProducer;
import org.apache.ignite.ml.util.generators.primitives.variable.UniformRandomProducer;

public class VectorGeneratorPrimitives {
    public static VectorGenerator gauss(Vector mean, Vector variances, Long seed) {
        A.notEmpty(mean.asArray(), "mean.size() != 0");
        A.ensure(mean.size() == variances.size(), "mean.size() == variances.size()");

        RandomProducer[] producers = new RandomProducer[mean.size()];
        for (int i = 0; i < producers.length; i++)
            producers[i] = new GaussRandomProducer(mean.get(i), variances.get(i), seed >>= 2);
        return RandomProducer.vectorize(producers);
    }

    public static VectorGenerator gauss(Vector mean, Vector variances) {
        return gauss(mean, variances, System.currentTimeMillis());
    }

    public static VectorGenerator ring(double radius, double fromAngle, double toAngle) {
        return new ParametricVectorGenerator(
            new UniformRandomProducer(fromAngle, toAngle),
            t -> radius * Math.sin(t),
            t -> radius * Math.cos(t)
        );
    }

    public static VectorGenerator parallelogram(Vector dimensions, long seed) {
        A.notEmpty(dimensions.asArray(), "dimensions.size() != 0");

        UniformRandomProducer[] producers = new UniformRandomProducer[dimensions.size()];
        for(int i = 0; i < producers.length; i++)
            producers[i] = new UniformRandomProducer(-dimensions.get(i), dimensions.get(i), seed >>= 2);

        return RandomProducer.vectorize(producers);
    }

    public static VectorGenerator parallelogram(Vector dimensions) {
        return parallelogram(dimensions, System.currentTimeMillis());
    }

    public static VectorGenerator circle(double radius, long seed) {
        return new UniformRandomProducer(-radius, radius, seed)
            .vectorize(2)
            .filter(v -> Math.sqrt(v.getLengthSquared()) <= radius);
    }

    public static VectorGenerator circle(double radius) {
        return circle(radius, System.currentTimeMillis());
    }

    public static VectorGenerator zero(int size) {
        return constant(VectorUtils.of(DoubleStream.of(0.).limit(size).toArray()));
    }

    public static VectorGenerator constant(Vector v) {
        return () -> v;
    }
}
