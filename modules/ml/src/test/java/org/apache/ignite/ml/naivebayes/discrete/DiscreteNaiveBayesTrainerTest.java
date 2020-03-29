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
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link DiscreteNaiveBayesTrainer} */
public class DiscreteNaiveBayesTrainerTest extends TrainerTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** */
    private static final double LABEL_1 = 1.;

    /** */
    private static final double LABEL_2 = 2.;

    /** Binary data. */
    private static final Map<Integer, double[]> binarizedData = new HashMap<>();

    /** Data. */
    private static final Map<Integer, double[]> data = new HashMap<>();

    /** */
    private static final double[][] binarizedDatathresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};

    /** */
    private static final double[][] thresholds = new double[][] {{4, 8}, {.5}, {.3, .4, .5}, {250, 500, 750}};

    static {
        binarizedData.put(0, new double[] {0, 0, 1, 1, 1, LABEL_1});
        binarizedData.put(1, new double[] {1, 0, 1, 1, 0, LABEL_1});
        binarizedData.put(2, new double[] {1, 1, 0, 0, 1, LABEL_1});
        binarizedData.put(3, new double[] {1, 1, 0, 0, 0, LABEL_1});
        binarizedData.put(4, new double[] {0, 1, 0, 0, 1, LABEL_1});
        binarizedData.put(5, new double[] {0, 0, 0, 1, 0, LABEL_1});

        binarizedData.put(6, new double[] {1, 0, 0, 1, 1, LABEL_2});
        binarizedData.put(7, new double[] {1, 1, 0, 0, 1, LABEL_2});
        binarizedData.put(8, new double[] {1, 1, 1, 1, 0, LABEL_2});
        binarizedData.put(9, new double[] {1, 1, 0, 1, 0, LABEL_2});
        binarizedData.put(10, new double[] {1, 1, 0, 1, 1, LABEL_2});
        binarizedData.put(11, new double[] {1, 0, 1, 1, 0, LABEL_2});
        binarizedData.put(12, new double[] {1, 0, 1, 0, 0, LABEL_2});

        data.put(0, new double[] {2, 0, .34, 123, LABEL_1});
        data.put(1, new double[] {8, 0, .37, 561, LABEL_1});
        data.put(2, new double[] {5, 1, .01, 678, LABEL_1});
        data.put(3, new double[] {2, 1, .32, 453, LABEL_1});
        data.put(4, new double[] {7, 1, .67, 980, LABEL_1});
        data.put(5, new double[] {2, 1, .69, 912, LABEL_1});
        data.put(6, new double[] {8, 0, .43, 453, LABEL_1});
        data.put(7, new double[] {2, 0, .45, 752, LABEL_1});
        data.put(8, new double[] {7, 1, .01, 132, LABEL_2});
        data.put(9, new double[] {2, 1, .68, 169, LABEL_2});
        data.put(10, new double[] {8, 0, .43, 453, LABEL_2});
        data.put(11, new double[] {2, 1, .45, 748, LABEL_2});
    }

    /** Trainer under test. */
    private DiscreteNaiveBayesTrainer trainer;

    /** Initialization {@code DiscreteNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new DiscreteNaiveBayesTrainer().setBucketThresholds(binarizedDatathresholds);
    }

    /** Test. */
    @Test
    public void testReturnsCorrectLabelProbalities() {

        DiscreteNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(binarizedData, parts),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        double[] expectedProbabilities = {6. / binarizedData.size(), 7. / binarizedData.size()};
        Assert.assertArrayEquals(expectedProbabilities, mdl.getClsProbabilities(), PRECISION);
    }

    /** Test. */
    @Test
    public void testReturnsEquivalentProbalitiesWhenSetEquiprobableClasses_() {
        DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer()
            .setBucketThresholds(binarizedDatathresholds)
            .withEquiprobableClasses();

        DiscreteNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(binarizedData, parts),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        Assert.assertArrayEquals(new double[] {.5, .5}, mdl.getClsProbabilities(), PRECISION);
    }

    /** Test. */
    @Test
    public void testReturnsPresetProbalitiesWhenSetPriorProbabilities() {
        double[] priorProbabilities = new double[] {.35, .65};
        DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer()
            .setBucketThresholds(binarizedDatathresholds)
            .setPriorProbabilities(priorProbabilities);

        DiscreteNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(binarizedData, parts),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        Assert.assertArrayEquals(priorProbabilities, mdl.getClsProbabilities(), PRECISION);
    }

    /** Test. */
    @Test
    public void testReturnsCorrectPriorProbabilities() {
        double[][][] expectedPriorProbabilites = new double[][][] {
            {{.5, .5}, {.5, .5}, {2. / 3., 1. / 3.}, {.5, .5}, {.5, .5}},
            {{0, 1}, {3. / 7, 4. / 7}, {4. / 7, 3. / 7}, {2. / 7, 5. / 7}, {4. / 7, 3. / 7,}}
        };

        DiscreteNaiveBayesModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(binarizedData, parts),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        for (int i = 0; i < expectedPriorProbabilites.length; i++) {
            for (int j = 0; j < expectedPriorProbabilites[i].length; j++)
                Assert.assertArrayEquals(expectedPriorProbabilites[i][j], mdl.getProbabilities()[i][j], PRECISION);
        }
    }

    /** Test. */
    @Test
    public void testReturnsCorrectPriorProbabilitiesWithDefferentThresholds() {
        double[][][] expectedPriorProbabilites = new double[][][] {
            {
                {4. / 8, 2. / 8, 2. / 8},
                {.5, .5},
                {1. / 8, 3. / 8, 2. / 8, 2. / 8},
                {1. / 8, 2. / 8, 2. / 8, 3. / 8}},
            {
                {2. / 4, 1. / 4, 1. / 4},
                {1. / 4, 3. / 4},
                {1. / 4, 0, 2. / 4, 1. / 4},
                {2. / 4, 1. / 4, 1. / 4, 0}}
        };

        DiscreteNaiveBayesModel mdl = trainer
            .setBucketThresholds(thresholds)
            .fit(
                new LocalDatasetBuilder<>(data, parts),
                new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
            );

        for (int i = 0; i < expectedPriorProbabilites.length; i++) {
            for (int j = 0; j < expectedPriorProbabilites[i].length; j++)
                Assert.assertArrayEquals(expectedPriorProbabilites[i][j], mdl.getProbabilities()[i][j], PRECISION);
        }
    }

}
