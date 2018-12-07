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

package org.apache.ignite.ml.util.generators.dataset;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.ml.util.generators.function.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.variable.UniformRandomProducer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.Utils;

public class TwoSeparableClassesDatasetGenerator {
    private final double margin;
    private final boolean mixClasses;

    public TwoSeparableClassesDatasetGenerator(double margin) {
        this.margin = Math.abs(margin);
        this.mixClasses = margin < 0.0;
    }

    public Stream<LabeledVector<Vector, Double>> labeled() {
        UniformRandomProducer urv = new UniformRandomProducer(-10, 10);
        UniformRandomProducer mixGenerator = new UniformRandomProducer(-margin, margin);

        List<ParametricVectorGenerator> families = Collections.singletonList(
            new ParametricVectorGenerator(t -> urv.get(), t -> -urv.get())
        );

        return Utils.asStream(new RandomVectorsGenerator(families, null, new UniformRandomProducer(-10, 10)))
            .filter(v -> isFirstClass(v.vector()) || isSecondClass(v.vector()))
            .map(v -> new LabeledVector<>(moveVector(mixGenerator, v.vector()), isFirstClass(v.vector()) ? 1.0 : -1.0));
    }

    private boolean isFirstClass(Vector v) {
        return v.get(0) - v.get(1) > margin;
    }

    private boolean isSecondClass(Vector v) {
        return v.get(0) - v.get(1) < -margin;
    }

    private Vector moveVector(UniformRandomProducer mixGenerator, Vector v) {
        if(mixClasses) {
            Vector copy = v.copy();
            for (int i = 0; i < v.size(); i++)
                copy.set(i, v.get(i) + mixGenerator.get());
            return copy;
        }

        return v;
    }

    public Stream<Vector> unlabeled() {
        return labeled().map(DatasetRow::features);
    }
}
