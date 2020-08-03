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

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.ml.naivebayes.compound.Data.LABEL_2;
import static org.apache.ignite.ml.naivebayes.compound.Data.binarizedDataThresholds;
import static org.apache.ignite.ml.naivebayes.compound.Data.classProbabilities;
import static org.apache.ignite.ml.naivebayes.compound.Data.labels;
import static org.apache.ignite.ml.naivebayes.compound.Data.means;
import static org.apache.ignite.ml.naivebayes.compound.Data.probabilities;
import static org.apache.ignite.ml.naivebayes.compound.Data.variances;
import static org.junit.Assert.assertEquals;

/** Tests for {@link CompoundNaiveBayesModel} */
public class CompoundNaiveBayesModelTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Test. */
    @Test
    public void testPredictOnlyGauss() {
        GaussianNaiveBayesModel gaussianModel =
            new GaussianNaiveBayesModel(means, variances, classProbabilities, labels, null);

        Vector observation = VectorUtils.of(6, 130, 8);

        CompoundNaiveBayesModel model = new CompoundNaiveBayesModel()
            .withPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withGaussianModel(gaussianModel);

        assertEquals(LABEL_2, model.predict(observation), PRECISION);
    }

    /** Test. */
    @Test
    public void testPredictOnlyDiscrete() {
        DiscreteNaiveBayesModel discreteModel =
            new DiscreteNaiveBayesModel(probabilities, classProbabilities, labels, binarizedDataThresholds, null);

        Vector observation = VectorUtils.of(1, 0, 1, 1, 0);

        CompoundNaiveBayesModel model = new CompoundNaiveBayesModel()
            .withPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withDiscreteModel(discreteModel);

        assertEquals(LABEL_2, model.predict(observation), PRECISION);
    }

    /** Test. */
    @Test
    public void testPredictGaussAndDiscrete() {
        DiscreteNaiveBayesModel discreteMdl =
            new DiscreteNaiveBayesModel(probabilities, classProbabilities, labels, binarizedDataThresholds, null);

        GaussianNaiveBayesModel gaussianMdl =
            new GaussianNaiveBayesModel(means, variances, classProbabilities, labels, null);

        CompoundNaiveBayesModel mdl = new CompoundNaiveBayesModel()
            .withPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withGaussianModel(gaussianMdl)
            .withGaussianFeatureIdsToSkip(asList(3, 4, 5, 6, 7))
            .withDiscreteModel(discreteMdl)
            .withDiscreteFeatureIdsToSkip( asList(0, 1, 2));

        Vector observation = VectorUtils.of(6, 130, 8, 1, 0, 1, 1, 0);

        assertEquals(LABEL_2, mdl.predict(observation), PRECISION);
    }
}
