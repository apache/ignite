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

package org.apache.ignite.ml.composition.boosting.convergence;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Before;

/** */
public abstract class ConvergenceCheckerTest {
    /** Not converged model. */
    protected ModelsComposition notConvergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double predict(Vector features) {
            return 2.1 * features.get(0);
        }
    };

    /** Converged model. */
    protected ModelsComposition convergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double predict(Vector features) {
            return 2 * (features.get(0) + 1);
        }
    };

    /** Features extractor. */
    protected LabeledDummyVectorizer<Integer, Double> vectorizer = new LabeledDummyVectorizer<>();

    /** Data. */
    protected Map<Integer, LabeledVector<Double>> data;

    /** */
    @Before
    public void setUp() {
        data = new HashMap<>();
        for (int i = 0; i < 10; i++)
            data.put(i, VectorUtils.of(i, i + 1).labeled((double)(2 * (i + 1))));
    }

    /** */
    public ConvergenceChecker<Integer, LabeledVector<Double>> createChecker(ConvergenceCheckerFactory factory,
        LocalDatasetBuilder<Integer, LabeledVector<Double>> datasetBuilder) {

        return factory.create(data.size(),
            x -> x,
            new Loss() {
                @Override public double error(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }

                @Override public double gradient(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }
            },
            datasetBuilder, vectorizer
        );
    }
}
