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

package org.apache.ignite.ml.util.generators.variable.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.Utils;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.variable.DiscreteRandomProducer;

public class VectorGeneratorsFamily implements VectorGenerator, DataStreamGenerator, Iterator<VectorGeneratorsFamily.VectorWithDistributionId> {
    private final List<VectorGenerator> family;
    private final DiscreteRandomProducer selector;

    public VectorGeneratorsFamily(VectorGenerator family) {
        this.family = Collections.singletonList(family);
        this.selector = new DiscreteRandomProducer(1.0);
    }

    private VectorGeneratorsFamily(List<VectorGenerator> family, DiscreteRandomProducer selector) {
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

    public Stream<VectorWithDistributionId> asStream() {
        return Utils.asStream(this);
    }

    @Override public boolean hasNext() {
        return true;
    }

    @Override public VectorWithDistributionId next() {
        return getWithId();
    }

    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        return asStream().map(v -> new LabeledVector<>(v.vector, (double) v.distributionId));
    }

    public static class Builder {
        private final List<VectorGenerator> family = new ArrayList<>();
        private final List<Double> weights = new ArrayList<>();

        public Builder with(VectorGenerator generator, double weight) {
            family.add(generator);
            weights.add(weight);
            return this;
        }

        public VectorGeneratorsFamily build() {
            A.notEmpty(family, "family.size != 0");
            double sumOfWeigts = weights.stream().mapToDouble(x -> x).sum();
            double[] probs = weights.stream().mapToDouble(w -> w / sumOfWeigts).toArray();
            return new VectorGeneratorsFamily(family, new DiscreteRandomProducer(probs));
        }
    }

    public static class VectorWithDistributionId {
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
