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

package org.apache.ignite.ml.naivebayes.discrete;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for Bernoulli naive Bayes algorithm with different datasets.
 */
public class DiscreteNaiveBayesTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Example from book Barber D. Bayesian reasoning and machine learning. Chapter 10. */
    @Test
    public void testLearnsAndPredictCorrently() {
        double english = 1.;
        double scottish = 2.;

        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {0, 0, 1, 1, 1, english});
        data.put(1, new double[] {1, 0, 1, 1, 0, english});
        data.put(2, new double[] {1, 1, 0, 0, 1, english});
        data.put(3, new double[] {1, 1, 0, 0, 0, english});
        data.put(4, new double[] {0, 1, 0, 0, 1, english});
        data.put(5, new double[] {0, 0, 0, 1, 0, english});
        data.put(6, new double[] {1, 0, 0, 1, 1, scottish});
        data.put(7, new double[] {1, 1, 0, 0, 1, scottish});
        data.put(8, new double[] {1, 1, 1, 1, 0, scottish});
        data.put(9, new double[] {1, 1, 0, 1, 0, scottish});
        data.put(10, new double[] {1, 1, 0, 1, 1, scottish});
        data.put(11, new double[] {1, 0, 1, 1, 0, scottish});
        data.put(12, new double[] {1, 0, 1, 0, 0, scottish});
        double[][] thresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};

        DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer().setBucketThresholds(thresholds);

        DiscreteNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        Vector observation = VectorUtils.of(1, 0, 1, 1, 0);

        Assert.assertEquals(scottish, mdl.predict(observation), PRECISION);
    }
}
