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
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.DiscreteRandomProducer;

/**
 * Represents a distribution family of district vector generators.
 */
public class VectorGeneratorsFamily implements VectorGenerator {
    /** Family of generators. */
    private final List<VectorGenerator> family;

    /** Randomized selector of vector generator from family. */
    private final DiscreteRandomProducer selector;

    /**
     * Creates an instance of VectorGeneratorsFamily.
     *
     * @param family Family of generators.
     * @param selector Randomized selector of generator from family.
     */
    private VectorGeneratorsFamily(List<VectorGenerator> family, DiscreteRandomProducer selector) {
        this.family = family;
        this.selector = selector;
    }

    /** {@inheritDoc} */
    @Override public Vector get() {
        return family.get(selector.getInt()).get();
    }

    /**
     * @return Pseudo random vector with parent distribution id.
     */
    public VectorWithDistributionId getWithId() {
        int id = selector.getInt();
        return new VectorWithDistributionId(family.get(id).get(), id);
    }

    /**
     * Creates data stream where label of vector == id of distribution from family.
     *
     * @return Data stream generator.
     */
    @Override public DataStreamGenerator asDataStream() {
        VectorGeneratorsFamily gen = this;
        return new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(gen::getWithId)
                    .map(v -> new LabeledVector<>(v.vector, (double)v.distributionId));
            }
        };
    }

    /**
     * Helper for distribution family building.
     */
    public static class Builder {
        /** Family. */
        private final List<VectorGenerator> family = new ArrayList<>();

        /** Weights of generators. */
        private final List<Double> weights = new ArrayList<>();

        /**
         * Mapper for generators in family.
         * It as applied before create an instance of VectorGeneratorsFamily
         */
        private IgniteFunction<VectorGenerator, VectorGenerator> mapper = x -> x;

        /**
         * Add generator to family with weight proportional to it selection probability.
         *
         * @param generator Generator.
         * @param weight Weight.
         * @return This builder.
         */
        public Builder add(VectorGenerator generator, double weight) {
            A.ensure(weight > 0, "weight > 0");

            family.add(generator);
            weights.add(weight);
            return this;
        }

        /**
         * Adds generator to family with weight = 1.
         *
         * @param generator Generator.
         * @return This builder.
         */
        public Builder add(VectorGenerator generator) {
            return add(generator, 1);
        }

        /**
         * Adds map function for all generators in family.
         *
         * @param mapper Mapper.
         * @return This builder.
         */
        public Builder map(IgniteFunction<VectorGenerator, VectorGenerator> mapper) {
            final IgniteFunction<VectorGenerator, VectorGenerator> old = this.mapper;
            this.mapper = x -> mapper.apply(old.apply(x));
            return this;
        }

        /**
         * Builds VectorGeneratorsFamily instance.
         *
         * @return Vector generators family.
         */
        public VectorGeneratorsFamily build() {
            return build(System.currentTimeMillis());
        }

        /**
         * Builds VectorGeneratorsFamily instance.
         *
         * @param seed Seed.
         * @return Vector generators family.
         */
        public VectorGeneratorsFamily build(long seed) {
            A.notEmpty(family, "family.size != 0");
            double sumOfWeigts = weights.stream().mapToDouble(x -> x).sum();
            double[] probs = weights.stream().mapToDouble(w -> w / sumOfWeigts).toArray();

            List<VectorGenerator> mappedFamily = family.stream().map(mapper).collect(Collectors.toList());
            return new VectorGeneratorsFamily(mappedFamily, new DiscreteRandomProducer(seed, probs));
        }
    }

    /**
     * Container for vector and distribution id.
     */
    public static class VectorWithDistributionId {
        /** Vector. */
        private final Vector vector;

        /** Distribution id. */
        private final int distributionId;

        /**
         * Creates an instance of VectorWithDistributionId.
         *
         * @param vector Vector.
         * @param distributionId Distribution id.
         */
        public VectorWithDistributionId(Vector vector, int distributionId) {
            this.vector = vector;
            this.distributionId = distributionId;
        }

        /**
         * @return Vector.
         */
        public Vector vector() {
            return vector;
        }

        /**
         * @return Distribution id.
         */
        public int distributionId() {
            return distributionId;
        }
    }
}
