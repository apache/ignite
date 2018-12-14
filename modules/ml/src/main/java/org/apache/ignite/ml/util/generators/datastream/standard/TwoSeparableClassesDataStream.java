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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.variable.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

public class TwoSeparableClassesDataStream implements DataStreamGenerator {
    private final double margin;
    private final double minCordValue;
    private final double maxCordValue;

    public TwoSeparableClassesDataStream(double margin, double variance) {
        this.margin = margin;
        this.minCordValue = -variance;
        this.maxCordValue = variance;
    }

    @Override
    public Stream<LabeledVector<Vector, Double>> labeled() {
        UniformRandomProducer urv = new UniformRandomProducer(minCordValue - Math.abs(margin), maxCordValue + Math.abs(margin));

        VectorGenerator vectorGenerator = new ParametricVectorGenerator(UniformRandomProducer.zero(), t -> urv.get(), t -> -urv.get());
        VectorGeneratorsFamily vectorsGenerator = new VectorGeneratorsFamily(vectorGenerator);
        return vectorsGenerator.asStream()
            .map(v -> new LabeledVector<>(applyMargin(v.vector()), isFirstClass(v.vector()) ? 1.0 : -1.0))
            .filter(v -> between(v.features().get(0), minCordValue, maxCordValue))
            .filter(v -> between(v.features().get(1), minCordValue, maxCordValue));
    }

    private boolean between(double x, double min, double max) {
        return x >= min && x <= max;
    }

    private boolean isFirstClass(Vector v) {
        return v.get(0) - v.get(1) > 0;
    }

    private Vector applyMargin(Vector v) {
        Vector copy = v.copy();
        copy.set(0, copy.get(0) + Math.signum(v.get(0) - v.get(1)) * margin);
        copy.set(1, copy.get(1) - Math.signum(v.get(0) - v.get(1)) * margin);
        return copy;
    }
}
