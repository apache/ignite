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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.gauss;

public class GaussianMixtureDataStream implements DataStreamGenerator {
    private final List<IgniteFunction<Long, VectorGenerator>> components;
    private long seed;

    private GaussianMixtureDataStream(List<IgniteFunction<Long, VectorGenerator>> components, long seed) {
        this.components = components;
        this.seed = seed;
    }

    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        VectorGeneratorsFamily.Builder builder = new VectorGeneratorsFamily.Builder();
        for (int i = 0; i < components.size(); i++) {
            builder = builder.add(components.get(i).apply(seed), 1.0);
            seed >>= 2;
        }

        return builder.build().asDataStream().labeled();
    }

    public static class Builder {
        private List<IgniteFunction<Long, VectorGenerator>> components = new ArrayList<>();

        public Builder add(Vector mean, Vector variance) {
            components.add(seed -> gauss(mean, variance, seed));
            return this;
        }

        public GaussianMixtureDataStream build() {
            return build(System.currentTimeMillis());
        }

        public GaussianMixtureDataStream build(long seed) {
            A.notEmpty(components, "this.means.size()");
            return new GaussianMixtureDataStream(components, seed);
        }
    }
}
