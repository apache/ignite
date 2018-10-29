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
package org.apache.ignite.ml.naivebayes.bernoulli;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link BernoulliNaiveBayesTrainer} */
public class BernoulliNaiveBayesTrainerTest extends TrainerTest {

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;
    /** */
    private static final double LABEL_1 = 1.;
    /** */
    private static final double LABEL_2 = 2.;

    /** Data. */
    private static final Map<Integer, double[]> data = new HashMap<>();

    static {
        data.put(0, new double[] {0, 0, 1, 1, 1, LABEL_1});
        data.put(1, new double[] {1, 0, 1, 1, 0, LABEL_1});
        data.put(2, new double[] {1, 1, 0, 0, 1, LABEL_1});
        data.put(3, new double[] {1, 1, 0, 0, 0, LABEL_1});
        data.put(4, new double[] {0, 1, 0, 0, 1, LABEL_1});
        data.put(5, new double[] {0, 0, 0, 1, 0, LABEL_1});

        data.put(6, new double[] {1, 0, 0, 1, 1, LABEL_2});
        data.put(7, new double[] {1, 1, 0, 0, 1, LABEL_2});
        data.put(8, new double[] {1, 1, 1, 1, 0, LABEL_2});
        data.put(9, new double[] {1, 1, 0, 1, 0, LABEL_2});
        data.put(10, new double[] {1, 1, 0, 1, 1, LABEL_2});
        data.put(11, new double[] {1, 0, 1, 1, 0, LABEL_2});
        data.put(12, new double[] {1, 0, 1, 0, 0, LABEL_2});
    }

    /** */
    private BernoulliNaiveBayesTrainer trainer;

    /** Initialization {@code BernoulliNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new BernoulliNaiveBayesTrainer();
    }

    /** */
    @Test
    public void testReturnsCorrectLabelProbalities() {

        BernoulliNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[v.length - 1]
        );

        double[] expectedProbabilities = {6. / data.size(), 7. / data.size()};
        Assert.assertArrayEquals(expectedProbabilities, model.getClassProbabilities(), PRECISION);
    }

    /** */
    @Test
    public void testReturnsEquivalentProbalitiesWhenSetEquiprobableClasses_() {
        BernoulliNaiveBayesTrainer trainer = new BernoulliNaiveBayesTrainer()
            .withEquiprobableClasses();

        BernoulliNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[v.length - 1]
        );

        Assert.assertArrayEquals(new double[] {.5, .5}, model.getClassProbabilities(), PRECISION);
    }

    /** */
    @Test
    public void testReturnsPresetProbalitiesWhenSetPriorProbabilities() {
        double[] priorProbabilities = new double[] {.35, .65};
        BernoulliNaiveBayesTrainer trainer = new BernoulliNaiveBayesTrainer()
            .setPriorProbabilities(priorProbabilities);

        BernoulliNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[v.length - 1]
        );

        Assert.assertArrayEquals(priorProbabilities, model.getClassProbabilities(), PRECISION);
    }

    /** */
    @Test
    public void testReturnsCorrectPriorProbabilities() {
        double[][] expectedPriorProbabilites = new double[][] {
            {.5, .5, 1. / 3, .5, .5},
            {1, 4. / 7, 3. / 7, 5. / 7, 3. / 7}
        };

        BernoulliNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[v.length - 1]
        );

        Assert.assertArrayEquals(expectedPriorProbabilites[0], model.getProbabilities()[0], PRECISION);
        Assert.assertArrayEquals(expectedPriorProbabilites[1], model.getProbabilities()[1], PRECISION);
    }

}
