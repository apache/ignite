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

import java.util.Iterator;
import org.apache.ignite.ml.util.generators.variable.RandomProducer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class LabeledRandomVectorsGenerator implements Iterator<LabeledRandomVectorsGenerator.LabeledRandomVector> {
    private final RandomVectorsGenerator vectorsStream;
    private final IgniteFunction<Vector, Double> classifier;
    private final RandomProducer noizeAfterClassification;

    public LabeledRandomVectorsGenerator(RandomVectorsGenerator vectorsStream,
        IgniteFunction<Vector, Double> classifier) {

        this(vectorsStream, classifier, () -> 0.0);
    }

    public LabeledRandomVectorsGenerator(RandomVectorsGenerator vectorsStream,
        IgniteFunction<Vector, Double> classifier,
        RandomProducer noizeAfterClassification) {

        this.vectorsStream = vectorsStream;
        this.classifier = classifier;
        this.noizeAfterClassification = noizeAfterClassification;
    }

    @Override public LabeledRandomVector next() {
        RandomVectorsGenerator.VectorWithDistributionFamily vector = vectorsStream.next();
        Double label = classifier.apply(vector.vector());
        vector = vector.map(v -> v.plus(noizeAfterClassification.get()));

        return new LabeledRandomVector(vector, label);
    }

    @Override public boolean hasNext() {
        return true;
    }

    public static class LabeledRandomVector {
        private final RandomVectorsGenerator.VectorWithDistributionFamily vector;
        private final Double label;

        public LabeledRandomVector(RandomVectorsGenerator.VectorWithDistributionFamily vector, Double label) {
            this.vector = vector;
            this.label = label;
        }

        public Vector vector() {
            return vector.vector();
        }

        public int distributionFamilyId() {
            return vector.distributionFamilyId();
        }

        public Double label() {
            return label;
        }
    }
}
