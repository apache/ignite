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

package org.apache.ignite.ml.util.generators.standard;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.gauss;

/**
 * Data stream generator representing gaussian mixture.
 */
public class GaussianMixtureDataStream implements DataStreamGenerator {
    /** Gaussian component generators. */
    private final List<IgniteFunction<Long, VectorGenerator>> componentGenerators;

    /** Seed. */
    private long seed;

    /**
     * Create an instance of GaussianMixtureDataStream.
     *
     * @param componentGenerators Component generators.
     * @param seed Seed.
     */
    private GaussianMixtureDataStream(List<IgniteFunction<Long, VectorGenerator>> componentGenerators, long seed) {
        this.componentGenerators = componentGenerators;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Double>> labeled() {
        VectorGeneratorsFamily.Builder builder = new VectorGeneratorsFamily.Builder();
        for (int i = 0; i < componentGenerators.size(); i++) {
            builder = builder.add(componentGenerators.get(i).apply(seed), 1.0);
            seed *= 2;
        }

        return builder.build().asDataStream().labeled();
    }

    /**
     * Builder for gaussian mixture.
     */
    public static class Builder {
        /** Gaussian component generators. */
        private List<IgniteFunction<Long, VectorGenerator>> componentGenerators = new ArrayList<>();

        /**
         * Adds multidimensional gaussian component.
         *
         * @param mean Mean value.
         * @param variance Variance for each component.
         */
        public Builder add(Vector mean, Vector variance) {
            componentGenerators.add(seed -> gauss(mean, variance, seed));
            return this;
        }

        /**
         * @return GaussianMixtureDataStream instance.
         */
        public GaussianMixtureDataStream build() {
            return build(System.currentTimeMillis());
        }

        /**
         * @param seed Seed.
         * @return GaussianMixtureDataStream instance.
         */
        public GaussianMixtureDataStream build(long seed) {
            A.notEmpty(componentGenerators, "this.means.size()");
            return new GaussianMixtureDataStream(componentGenerators, seed);
        }
    }
}
