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

package org.apache.ignite.ml.naivebayes.compound;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.ml.naivebayes.compound.Data.LABEL_1;
import static org.apache.ignite.ml.naivebayes.compound.Data.LABEL_2;
import static org.apache.ignite.ml.naivebayes.compound.Data.binarizedDataThresholds;
import static org.apache.ignite.ml.naivebayes.compound.Data.classProbabilities;
import static org.apache.ignite.ml.naivebayes.compound.Data.data;
import static org.junit.Assert.assertEquals;

/** Integration tests for Compound naive Bayes algorithm with different datasets. */
public class CompoundNaiveBayesTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Test. */
    @Test
    public void testLearnsAndPredictCorrectly() {
        CompoundNaiveBayesTrainer trainer = new CompoundNaiveBayesTrainer()
            .withPriorProbabilities(classProbabilities)
            .withGaussianNaiveBayesTrainer(new GaussianNaiveBayesTrainer())
            .withGaussianFeatureIdsToSkip(asList(3, 4, 5, 6, 7))
            .withDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                .setBucketThresholds(binarizedDataThresholds))
            .withDiscreteFeatureIdsToSkip(asList(0, 1, 2));

        CompoundNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
                new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        Vector observation1 = VectorUtils.of(5.92, 165, 10, 1, 1, 0, 0, 0);
        assertEquals(LABEL_1, mdl.predict(observation1), PRECISION);

        Vector observation2 = VectorUtils.of(6, 130, 8, 1, 0, 1, 1, 0);
        assertEquals(LABEL_2, mdl.predict(observation2), PRECISION);
    }
}
