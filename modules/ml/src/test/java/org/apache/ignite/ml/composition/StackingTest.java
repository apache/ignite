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

package org.apache.ignite.ml.composition;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.stacking.StackedDatasetTrainer;
import org.apache.ignite.ml.composition.stacking.StackedModel;
import org.apache.ignite.ml.composition.stacking.StackedVectorDatasetTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.SmoothParametrized;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.trainers.AdaptableDatasetModel;
import org.apache.ignite.ml.trainers.AdaptableDatasetTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Tests stacked trainers.
 */
public class StackingTest extends TrainerTest {
    /** Rule to check exceptions. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Tests simple stack training.
     */
    @Test
    public void testSimpleStack() {
        StackedDatasetTrainer<Vector, Vector, Double, LinearRegressionModel, Double> trainer =
            new StackedDatasetTrainer<>();

        UpdatesStrategy<SmoothParametrized, SimpleGDParameterUpdate> updatesStgy = new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.2),
            SimpleGDParameterUpdate.SUM_LOCAL,
            SimpleGDParameterUpdate.AVG
        );

        MLPArchitecture arch = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        MLPTrainer<SimpleGDParameterUpdate> trainer1 = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            updatesStgy,
            3000,
            10,
            50,
            123L
        );

        // Convert model trainer to produce Vector -> Vector model
        DatasetTrainer<AdaptableDatasetModel<Vector, Vector, Matrix, Matrix, MultilayerPerceptron>, Double> mlpTrainer =
            AdaptableDatasetTrainer.of(trainer1)
                .beforeTrainedModel((Vector v) -> new DenseMatrix(v.asArray(), 1))
                .afterTrainedModel((Matrix mtx) -> mtx.getRow(0))
                .withConvertedLabels(VectorUtils::num2Arr);

        final double factor = 3;

        StackedModel<Vector, Vector, Double, LinearRegressionModel> mdl = trainer
            .withAggregatorTrainer(new LinearRegressionLSQRTrainer().withConvertedLabels(x -> x * factor))
            .addTrainer(mlpTrainer)
            .withAggregatorInputMerger(VectorUtils::concat)
            .withSubmodelOutput2VectorConverter(IgniteFunction.identity())
            .withVector2SubmodelInputConverter(IgniteFunction.identity())
            .withOriginalFeaturesKept(IgniteFunction.identity())
            .withEnvironmentBuilder(TestUtils.testEnvBuilder())
            .fit(getCacheMock(xor), parts, new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST));

        assertEquals(0.0 * factor, mdl.predict(VectorUtils.of(0.0, 0.0)), 0.3);
        assertEquals(1.0 * factor, mdl.predict(VectorUtils.of(0.0, 1.0)), 0.3);
        assertEquals(1.0 * factor, mdl.predict(VectorUtils.of(1.0, 0.0)), 0.3);
        assertEquals(0.0 * factor, mdl.predict(VectorUtils.of(1.0, 1.0)), 0.3);
    }

    /**
     * Tests simple stack training.
     */
    @Test
    public void testSimpleVectorStack() {
        StackedVectorDatasetTrainer<Double, LinearRegressionModel, Double> trainer =
            new StackedVectorDatasetTrainer<>();

        UpdatesStrategy<SmoothParametrized, SimpleGDParameterUpdate> updatesStgy = new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.2),
            SimpleGDParameterUpdate.SUM_LOCAL,
            SimpleGDParameterUpdate.AVG
        );

        MLPArchitecture arch = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        DatasetTrainer<MultilayerPerceptron, Double> mlpTrainer = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            updatesStgy,
            3000,
            10,
            50,
            123L
        ).withConvertedLabels(VectorUtils::num2Arr);

        final double factor = 3;

        StackedModel<Vector, Vector, Double, LinearRegressionModel> mdl = trainer
            .withAggregatorTrainer(new LinearRegressionLSQRTrainer().withConvertedLabels(x -> x * factor))
            .addMatrix2MatrixTrainer(mlpTrainer)
            .withEnvironmentBuilder(TestUtils.testEnvBuilder())
            .fit(getCacheMock(xor), parts, new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST));

        assertEquals(0.0 * factor, mdl.predict(VectorUtils.of(0.0, 0.0)), 0.3);
        assertEquals(1.0 * factor, mdl.predict(VectorUtils.of(0.0, 1.0)), 0.3);
        assertEquals(1.0 * factor, mdl.predict(VectorUtils.of(1.0, 0.0)), 0.3);
        assertEquals(0.0 * factor, mdl.predict(VectorUtils.of(1.0, 1.0)), 0.3);
    }

    /**
     * Tests that if there is no any way for input of first layer to propagate to second layer,
     * exception will be thrown.
     */
    @Test
    public void testINoWaysOfPropagation() {
        StackedDatasetTrainer<Void, Void, Void, IgniteModel<Void, Void>, Void> trainer =
            new StackedDatasetTrainer<>();
        thrown.expect(IllegalStateException.class);
        trainer.fit(null, (IgniteCache<Object, Object>)null, null);
    }
}
