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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

public class GaussianMixtureDataStream implements DataStreamGenerator {
    public static class Builder {
        private final List<Vector> means = new ArrayList<>();
        private final List<Vector> variances = new ArrayList<>();

        public Builder add(Vector mean, Vector variance) {
            A.ensure(variance.size() >= 0, "variance.size() >= 0");

            means.add(mean);
            variances.add(variance);
            return this;
        }

        public GaussianMixtureDataStream build() {
            A.notEmpty(means, "this.means.size()");

            Vector[] means = new Vector[this.means.size()];
            Vector[] variances = new Vector[this.variances.size()];
            for(int i = 0; i<this.means.size(); i++) {
                means[i] = this.means.get(i);
                variances[i] = this.variances.get(i);
            }

            return new GaussianMixtureDataStream(means, variances);
        }
    }

    private final Vector[] points;
    private final Vector[] variances;

    private GaussianMixtureDataStream(Vector[] points, Vector[] variances) {
        this.points = points;
        this.variances = variances;
    }

    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        VectorGeneratorsFamily.Builder builder = new VectorGeneratorsFamily.Builder();
        long seed = System.currentTimeMillis();
        for (int i = 0; i < points.length; i++) {
            VectorGenerator gauss = VectorGeneratorPrimitives.gauss(points[i], variances[i], seed);
            builder = builder.with(gauss, 1.0);
            seed >>= 2;
        }

        return builder.build().asDataStream().labeled();
    }
}
