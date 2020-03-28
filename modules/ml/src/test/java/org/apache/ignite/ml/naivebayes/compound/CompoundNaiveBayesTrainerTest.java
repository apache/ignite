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

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.ml.naivebayes.compound.Data.binarizedDataThresholds;
import static org.apache.ignite.ml.naivebayes.compound.Data.classProbabilities;
import static org.apache.ignite.ml.naivebayes.compound.Data.data;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test for {@link CompoundNaiveBayesTrainer} */
public class CompoundNaiveBayesTrainerTest extends TrainerTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Trainer under test. */
    private CompoundNaiveBayesTrainer trainer;

    /** Initialization {@code CompoundNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new CompoundNaiveBayesTrainer()
            .withPriorProbabilities(classProbabilities)
            .withGaussianNaiveBayesTrainer(new GaussianNaiveBayesTrainer())
                .withGaussianFeatureIdsToSkip(asList(3, 4, 5, 6, 7))
            .withDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                .setBucketThresholds(binarizedDataThresholds))
                .withDiscreteFeatureIdsToSkip(asList(0, 1, 2));
    }

    /** Test. */
    @Test
    public void test() {
        CompoundNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
                new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        assertDiscreteModel(model.getDiscreteModel());
        assertGaussianModel(model.getGaussianModel());
    }

    /** Discrete model assertions. */
    private void assertDiscreteModel(DiscreteNaiveBayesModel model) {
        double[][][] expectedProbabilities = new double[][][] {
            {
                {.25, .75},
                {.5, .5},
                {.5, .5},
                {.5, .5},
                {.5, .5}
            },
            {
                {.0, 1},
                {.25, .75},
                {.75, .25},
                {.25, .75},
                {.5, .5}
            }
        };

        for (int i = 0; i < expectedProbabilities.length; i++) {
            for (int j = 0; j < expectedProbabilities[i].length; j++)
                assertArrayEquals(expectedProbabilities[i][j], model.getProbabilities()[i][j], PRECISION);
        }
        assertArrayEquals(new double[] {.5, .5}, model.getClsProbabilities(), PRECISION);
    }

    /** Gaussian model assertions. */
    private void assertGaussianModel(GaussianNaiveBayesModel model) {
        double[] priorProbabilities = new double[] {.5, .5};

        assertEquals(priorProbabilities[0], model.getClassProbabilities()[0], PRECISION);
        assertEquals(priorProbabilities[1], model.getClassProbabilities()[1], PRECISION);
        assertArrayEquals(new double[] {5.855, 176.25, 11.25}, model.getMeans()[0], PRECISION);
        assertArrayEquals(new double[] {5.4175, 132.5, 7.5}, model.getMeans()[1], PRECISION);
        double[] expectedVars = {0.026274999999999, 92.1875, 0.6875};
        assertArrayEquals(expectedVars, model.getVariances()[0], PRECISION);
    }
}
