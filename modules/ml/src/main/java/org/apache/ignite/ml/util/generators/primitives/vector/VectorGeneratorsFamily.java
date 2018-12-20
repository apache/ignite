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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.variable.DiscreteRandomProducer;

public class VectorGeneratorsFamily implements VectorGenerator {
    private final List<VectorGenerator> family;
    private final DiscreteRandomProducer selector;

    private VectorGeneratorsFamily(List<VectorGenerator> family, DiscreteRandomProducer selector) {
        A.notEmpty(family, "family");
        A.ensure(family.size() == selector.size(), "family.size() == selector.size()");

        this.family = family;
        this.selector = selector;
    }

    @Override public Vector get() {
        return family.get(selector.getInt()).get();
    }

    public VectorWithDistributionId getWithId() {
        int id = selector.getInt();
        return new VectorWithDistributionId(family.get(id).get(), id);
    }

    @Override public DataStreamGenerator asDataStream() {
        VectorGeneratorsFamily gen = this;
        return new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Vector, Double>> labeled() {
                return Stream.generate(gen::getWithId)
                    .map(v -> new LabeledVector<>(v.vector, (double)v.distributionId));
            }
        };
    }

    public static class Builder {
        private final List<VectorGenerator> family = new ArrayList<>();
        private final List<Double> weights = new ArrayList<>();
        private IgniteFunction<VectorGenerator, VectorGenerator> mapper = x -> x;

        public Builder with(VectorGenerator generator, double weight) {
            A.ensure(weight > 0, "weight > 0");

            family.add(generator);
            weights.add(weight);
            return this;
        }

        public Builder with(VectorGenerator generator) {
            return with(generator, 1);
        }

        public Builder map(IgniteFunction<VectorGenerator, VectorGenerator> f) {
            final IgniteFunction<VectorGenerator, VectorGenerator> old = mapper;
            mapper = x -> f.apply(old.apply(x));
            return this;
        }

        public VectorGeneratorsFamily build() {
            A.notEmpty(family, "family.size != 0");
            double sumOfWeigts = weights.stream().mapToDouble(x -> x).sum();
            double[] probs = weights.stream().mapToDouble(w -> w / sumOfWeigts).toArray();

            List<VectorGenerator> mappedFamilily = family.stream().map(mapper).collect(Collectors.toList());
            return new VectorGeneratorsFamily(mappedFamilily, new DiscreteRandomProducer(probs));
        }
    }

    private static class VectorWithDistributionId {
        private final Vector vector;
        private final int distributionId;

        public VectorWithDistributionId(Vector vector, int distributionId) {
            this.vector = vector;
            this.distributionId = distributionId;
        }

        public Vector vector() {
            return vector;
        }

        public int distributionId() {
            return distributionId;
        }

        public VectorWithDistributionId map(IgniteFunction<Vector, Vector> f) {
            return new VectorWithDistributionId(f.apply(vector), distributionId);
        }
    }
}
