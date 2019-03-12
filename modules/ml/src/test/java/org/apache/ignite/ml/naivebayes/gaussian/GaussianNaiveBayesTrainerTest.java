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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link GaussianNaiveBayesTrainer}.
 */
public class GaussianNaiveBayesTrainerTest extends TrainerTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;
    /** */
    private static final double LABEL_1 = 1.;
    /** */
    private static final double LABEL_2 = 2.;

    /** Data. */
    private static final Map<Integer, double[]> data = new HashMap<>();
    /** */
    private static final Map<Integer, double[]> singleLabeldata1 = new HashMap<>();
    /** */
    private static final Map<Integer, double[]> singleLabeldata2 = new HashMap<>();

    static {
        data.put(0, new double[] {1.0, -1.0, LABEL_1});
        data.put(1, new double[] {-1.0, 2.0, LABEL_1});
        data.put(2, new double[] {6.0, 1.0, LABEL_1});
        data.put(3, new double[] {-3.0, 2.0, LABEL_2});
        data.put(4, new double[] {-5.0, -2.0, LABEL_2});

        singleLabeldata1.put(0, new double[] {1.0, -1.0, LABEL_1});
        singleLabeldata1.put(1, new double[] {-1.0, 2.0, LABEL_1});
        singleLabeldata1.put(2, new double[] {6.0, 1.0, LABEL_1});

        singleLabeldata2.put(0, new double[] {-3.0, 2.0, LABEL_2});
        singleLabeldata2.put(1, new double[] {-5.0, -2.0, LABEL_2});
    }

    /** Trainer. */
    private GaussianNaiveBayesTrainer trainer;

    /** Initialization {@code GaussianNaiveBayesTrainer}.*/
    @Before
    public void createTrainer() {
        trainer = new GaussianNaiveBayesTrainer();
    }

    /** */
    @Test
    public void testWithLinearlySeparableData() {
        Map<Integer, double[]> cacheMock = new HashMap<>();
        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        GaussianNaiveBayesModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(10, 100)), PRECISION);
    }

    /** */
    @Test
    public void testReturnsCorrectLabelProbalities() {

        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertEquals(3. / data.size(), model.getClassProbabilities()[0], PRECISION);
        Assert.assertEquals(2. / data.size(), model.getClassProbabilities()[1], PRECISION);
    }

    /** */
    @Test
    public void testReturnsEquivalentProbalitiesWhenSetEquiprobableClasses_() {
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer()
            .withEquiprobableClasses();

        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertEquals(.5, model.getClassProbabilities()[0], PRECISION);
        Assert.assertEquals(.5, model.getClassProbabilities()[1], PRECISION);
    }

    /** */
    @Test
    public void testReturnsPresetProbalitiesWhenSetPriorProbabilities() {
        double[] priorProbabilities = new double[] {.35, .65};
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer()
            .setPriorProbabilities(priorProbabilities);

        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertEquals(priorProbabilities[0], model.getClassProbabilities()[0], PRECISION);
        Assert.assertEquals(priorProbabilities[1], model.getClassProbabilities()[1], PRECISION);
    }

    /** */
    @Test
    public void testReturnsCorrectMeans() {

        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(singleLabeldata1, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertArrayEquals(new double[] {2.0, 2. / 3.}, model.getMeans()[0], PRECISION);
    }

    /** */
    @Test
    public void testReturnsCorrectVariances() {

        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(singleLabeldata1, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        double[] expectedVars = {8.666666666666666, 1.5555555555555556};
        Assert.assertArrayEquals(expectedVars, model.getVariances()[0], PRECISION);
    }

    /** */
    @Test
    public void testUpdatigModel() {
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(singleLabeldata1, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        GaussianNaiveBayesModel updatedModel = trainer.updateModel(model,
            new LocalDatasetBuilder<>(singleLabeldata2, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertEquals(3. / data.size(), updatedModel.getClassProbabilities()[0], PRECISION);
        Assert.assertEquals(2. / data.size(), updatedModel.getClassProbabilities()[1], PRECISION);
    }
}
