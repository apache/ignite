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

package org.apache.ignite.ml.naivebayes.gaussian;

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
 * Complex tests for naive Bayes algorithm with different datasets.
 */
public class GaussianNaiveBayesTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /**
     * An example data set from wikipedia article about Naive Bayes https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Sex_classification
     */
    @Test
    public void wikipediaSexClassificationDataset() {
        Map<Integer, double[]> data = new HashMap<>();
        double male = 0.;
        double female = 1.;
        data.put(0, new double[] {male, 6, 180, 12});
        data.put(2, new double[] {male, 5.92, 190, 11});
        data.put(3, new double[] {male, 5.58, 170, 12});
        data.put(4, new double[] {male, 5.92, 165, 10});
        data.put(5, new double[] {female, 5, 100, 6});
        data.put(6, new double[] {female, 5.5, 150, 8});
        data.put(7, new double[] {female, 5.42, 130, 7});
        data.put(8, new double[] {female, 5.75, 150, 9});
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );
        Vector observation = VectorUtils.of(6, 130, 8);

        Assert.assertEquals(female, model.predict(observation), PRECISION);
    }

    /** Dataset from Gaussian NB example in the scikit-learn documentation */
    @Test
    public void scikitLearnExample() {
        Map<Integer, double[]> data = new HashMap<>();
        double one = 1.;
        double two = 2.;
        data.put(0, new double[] {one, -1, 1});
        data.put(2, new double[] {one, -2, -1});
        data.put(3, new double[] {one, -3, -2});
        data.put(4, new double[] {two, 1, 1});
        data.put(5, new double[] {two, 2, 1});
        data.put(6, new double[] {two, 3, 2});
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );
        Vector observation = VectorUtils.of(-0.8, -1);

        Assert.assertEquals(one, model.predict(observation), PRECISION);
    }

}
