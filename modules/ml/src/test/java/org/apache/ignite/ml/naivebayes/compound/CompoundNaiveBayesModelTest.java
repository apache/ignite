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
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesSumsHolder;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link CompoundNaiveBayesModel} */
public class CompoundNaiveBayesModelTest {

    @Test /** */
    public void testPredictOnlyGaus() {
        double first = 1;
        double second = 2;
        double[] labels = {first, second};
        double[] classProbabilities = new double[] {.5, .5};

        double[][] means = new double[][] {
            {5.855, 176.25, 11.25},
            {5.4175, 132.5, 7.5},
        };
        double[][] variances = new double[][] {
            {3.5033E-2, 1.2292E2, 9.1667E-1},
            {9.7225E-2, 5.5833E2, 1.6667},
        };
        GaussianNaiveBayesModel gaussianModel =
            new GaussianNaiveBayesModel(means, variances, classProbabilities, labels, null);

        Vector observation = VectorUtils.of(6, 130, 8);

        CompoundNaiveBayesModel model = CompoundNaiveBayesModel.builder()
            .wirhPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withGaussianModel(gaussianModel)
            .build();

        assertEquals(second, model.predict(observation), 0.0001);
    }

    @Test /** */
    public void testPredictOnlyDiscrete() {
        double first = 1;
        double second = 2;
        double[] labels = {first, second};
        double[] classProbabilities = new double[] {6. / 13, 7. / 13};

        double[][][] probabilities = new double[][][] {
            {{.5, .5}, {.2, .3, .5}, {2. / 3., 1. / 3.}, {.4, .1, .5}, {.5, .5}},
            {{0, 1}, {1. / 7, 2. / 7, 4. / 7}, {4. / 7, 3. / 7}, {2. / 7, 3. / 7, 2. / 7}, {4. / 7, 3. / 7,}}
        };
        double[][] thresholds = new double[][] {{.5}, {.2, .7}, {.5}, {.5, 1.5}, {.5}};
        DiscreteNaiveBayesModel discreteModel =
            new DiscreteNaiveBayesModel(probabilities, classProbabilities, labels, thresholds, new DiscreteNaiveBayesSumsHolder());

        Vector observation = VectorUtils.of(2, 0, 1, 2, 0);

        CompoundNaiveBayesModel model = CompoundNaiveBayesModel.builder()
            .wirhPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withDiscreteModel(discreteModel)
            .build();

        assertEquals(second, model.predict(observation), 0.0001);
    }

    @Test /** */
    public void testPredictGausAndDiscrete() {

    }
}
