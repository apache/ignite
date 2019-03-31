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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesSumsHolder;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link CompoundNaiveBayesModel} */
public class CompoundNaiveBayesModelTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** The first label*/
    private static final double LABEL_1 = 1.;

    /** The second label*/
    private static final double LABEL_2 = 2.;

    private static final Map<Integer, double[]> data = new HashMap<>();

    static {
        data.put(0, new double[] {6, 180, 12, 0, 0, 1, 1, 1, LABEL_1});
        data.put(1, new double[] {5.92, 190, 11, 1, 0, 1, 1, 0, LABEL_1});
        data.put(2, new double[] {5.58, 170, 12, 1, 1, 0, 0, 1, LABEL_1});
        data.put(3, new double[] {5.92, 165, 10, 1, 1, 0, 0, 0, LABEL_1});
//        data.put(4, new double[] {0, 1, 0, 0, 1, LABEL_1});
//        data.put(5, new double[] {0, 0, 0, 1, 0, LABEL_1});

        data.put(6, new double[] {5, 100, 6, 1, 0, 0, 1, 1, LABEL_2});
        data.put(7, new double[] {5.5, 150, 8, 1, 1, 0, 0, 1, LABEL_2});
        data.put(8, new double[] {5.42, 130, 7, 1, 1, 1, 1, 0, LABEL_2});
        data.put(9, new double[] {5.75, 150, 9, 1, 1, 0, 1, 0, LABEL_2});
//        data.put(10, new double[] {1, 1, 0, 1, 1, LABEL_2});
//        data.put(11, new double[] {1, 0, 1, 1, 0, LABEL_2});
//        data.put(12, new double[] {1, 0, 1, 0, 0, LABEL_2});

    }

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

        assertEquals(second, model.predict(observation), PRECISION);
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

        assertEquals(second, model.predict(observation), PRECISION);
    }

    @Test /** */
    public void testPredictGausAndDiscrete() {
        double[][][] probabilities = new double[][][] {
            {{-1}, {-1}, {-1}, {.5, .5}, {.2, .3, .5}, {2. / 3., 1. / 3.}, {.4, .1, .5}, {.5, .5}},
            {{-1}, {-1}, {-1}, {0, 1}, {1. / 7, 2. / 7, 4. / 7}, {4. / 7, 3. / 7}, {2. / 7, 3. / 7, 2. / 7}, {4. / 7, 3. / 7,}}
        };
        double[] classProbabilities = new double[] {.5, .5};
        double[] labels = {LABEL_1, LABEL_2};

        double[][] thresholds = new double[][] {{-1}, {-1}, {-1}, {.5}, {.5}, {.5}, {.5}, {.5}};
        DiscreteNaiveBayesModel discreteModel = new DiscreteNaiveBayesModel(probabilities, classProbabilities, labels, thresholds, null);

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

        CompoundNaiveBayesModel model = CompoundNaiveBayesModel.builder()
            .wirhPriorProbabilities(classProbabilities)
            .withLabels(labels)
            .withGaussianModel(gaussianModel)
            .withGaussianSkipFuture(f -> f >= 2)
            .withDiscreteModel(discreteModel)
            .withDiscreteSkipFuture(f -> f <= 2)
            .build();

        Vector observation = VectorUtils.of(6, 130, 8, 2, 0, 1, 2, 0);

        assertEquals(LABEL_2, model.predict(observation), PRECISION);
    }
}
