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

package org.apache.ignite.ml.composition.boosting;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.convergence.simple.ConvergenceCheckerStubFactory;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeConditionalNode;
import org.apache.ignite.ml.tree.boosting.GDBBinaryClassifierOnTreesTrainer;
import org.apache.ignite.ml.tree.boosting.GDBRegressionOnTreesTrainer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class GDBTrainerTest extends TrainerTest {
    /** */
    @Test
    public void testFitRegression() {
        int size = 100;
        double[] xs = new double[size];
        double[] ys = new double[size];
        double from = -5.0;
        double to = 5.0;
        double step = Math.abs(from - to) / size;

        Map<Integer, double[]> learningSample = new HashMap<>();
        for (int i = 0; i < size; i++) {
            xs[i] = from + step * i;
            ys[i] = 2 * xs[i];
            learningSample.put(i, new double[] {xs[i], ys[i]});
        }

        GDBTrainer trainer = new GDBRegressionOnTreesTrainer(1.0, 2000, 3, 0.0)
            .withUsingIdx(true);

        Model<Vector, Double> mdl = trainer.fit(
            learningSample, 1,
            (k, v) -> VectorUtils.of(v[0]),
            (k, v) -> v[1]
        );

        double mse = 0.0;
        for (int j = 0; j < size; j++) {
            double x = xs[j];
            double y = ys[j];
            double p = mdl.apply(VectorUtils.of(x));
            mse += Math.pow(y - p, 2);
        }
        mse /= size;

        assertEquals(0.0, mse, 0.0001);

        ModelsComposition composition = (ModelsComposition)mdl;
        assertTrue(composition.toString().length() > 0);
        assertTrue(composition.toString(true).length() > 0);
        assertTrue(composition.toString(false).length() > 0);

        composition.getModels().forEach(m -> assertTrue(m instanceof DecisionTreeConditionalNode));

        assertEquals(2000, composition.getModels().size());
        assertTrue(composition.getPredictionsAggregator() instanceof WeightedPredictionsAggregator);

        trainer = trainer.withCheckConvergenceStgyFactory(new MeanAbsValueConvergenceCheckerFactory(0.1));
        assertTrue(trainer.fit(
            learningSample, 1,
            (k, v) -> VectorUtils.of(v[0]),
            (k, v) -> v[1]
        ).getModels().size() < 2000);
    }

    /** */
    @Test
    public void testFitClassifier() {
        testClassifier((trainer, learningSample) -> trainer.fit(
            learningSample, 1,
            (k, v) -> VectorUtils.of(v[0]),
            (k, v) -> v[1]
        ));
    }

    /** */
    @Test
    public void testFitClassifierWithLearningStrategy() {
        testClassifier((trainer, learningSample) -> trainer.fit(
            new LocalDatasetBuilder<>(learningSample, 1),
            (k, v) -> VectorUtils.of(v[0]),
            (k, v) -> v[1]
        ));
    }

    /** */
    private void testClassifier(BiFunction<GDBTrainer, Map<Integer, double[]>,
        Model<Vector, Double>> fitter) {
        int sampleSize = 100;
        double[] xs = new double[sampleSize];
        double[] ys = new double[sampleSize];

        for (int i = 0; i < sampleSize; i++) {
            xs[i] = i;
            ys[i] = ((int)(xs[i] / 10.0) % 2) == 0 ? -1.0 : 1.0;
        }

        Map<Integer, double[]> learningSample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++)
            learningSample.put(i, new double[] {xs[i], ys[i]});

        GDBTrainer trainer = new GDBBinaryClassifierOnTreesTrainer(0.3, 500, 3, 0.0)
            .withUsingIdx(true)
            .withCheckConvergenceStgyFactory(new MeanAbsValueConvergenceCheckerFactory(0.3));

        Model<Vector, Double> mdl = fitter.apply(trainer, learningSample);

        int errorsCnt = 0;
        for (int j = 0; j < sampleSize; j++) {
            double x = xs[j];
            double y = ys[j];
            double p = mdl.apply(VectorUtils.of(x));
            if (p != y)
                errorsCnt++;
        }

        assertEquals(0, errorsCnt);

        assertTrue(mdl instanceof ModelsComposition);
        ModelsComposition composition = (ModelsComposition)mdl;
        composition.getModels().forEach(m -> assertTrue(m instanceof DecisionTreeConditionalNode));

        assertTrue(composition.getModels().size() < 500);
        assertTrue(composition.getPredictionsAggregator() instanceof WeightedPredictionsAggregator);

        trainer = trainer.withCheckConvergenceStgyFactory(new ConvergenceCheckerStubFactory());
        assertEquals(500, ((ModelsComposition)fitter.apply(trainer, learningSample)).getModels().size());
    }

    /** */
    @Test
    public void testUpdate() {
        int sampleSize = 100;
        double[] xs = new double[sampleSize];
        double[] ys = new double[sampleSize];

        for (int i = 0; i < sampleSize; i++) {
            xs[i] = i;
            ys[i] = ((int)(xs[i] / 10.0) % 2) == 0 ? -1.0 : 1.0;
        }

        Map<Integer, double[]> learningSample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++)
            learningSample.put(i, new double[] {xs[i], ys[i]});
        IgniteBiFunction<Integer, double[], Vector> fExtr = (k, v) -> VectorUtils.of(v[0]);
        IgniteBiFunction<Integer, double[], Double> lExtr = (k, v) -> v[1];

        GDBTrainer classifTrainer = new GDBBinaryClassifierOnTreesTrainer(0.3, 500, 3, 0.0)
            .withUsingIdx(true)
            .withCheckConvergenceStgyFactory(new MeanAbsValueConvergenceCheckerFactory(0.3));
        GDBTrainer regressTrainer = new GDBRegressionOnTreesTrainer(0.3, 500, 3, 0.0)
            .withUsingIdx(true)
            .withCheckConvergenceStgyFactory(new MeanAbsValueConvergenceCheckerFactory(0.3));

        testUpdate(learningSample, fExtr, lExtr, classifTrainer);
        testUpdate(learningSample, fExtr, lExtr, regressTrainer);
    }

    /** */
    private void testUpdate(Map<Integer, double[]> dataset, IgniteBiFunction<Integer, double[], Vector> fExtr,
        IgniteBiFunction<Integer, double[], Double> lExtr, GDBTrainer trainer) {

        ModelsComposition originalMdl = trainer.fit(dataset, 1, fExtr, lExtr);
        ModelsComposition updatedOnSameDataset = trainer.update(originalMdl, dataset, 1, fExtr, lExtr);

        LocalDatasetBuilder<Integer, double[]> epmtyDataset = new LocalDatasetBuilder<>(new HashMap<>(), 1);
        ModelsComposition updatedOnEmptyDataset = trainer.updateModel(originalMdl, epmtyDataset, fExtr, lExtr);

        dataset.forEach((k,v) -> {
            Vector features = fExtr.apply(k, v);

            Double originalAnswer = originalMdl.apply(features);
            Double updatedMdlAnswer1 = updatedOnSameDataset.apply(features);
            Double updatedMdlAnswer2 = updatedOnEmptyDataset.apply(features);

            assertEquals(originalAnswer, updatedMdlAnswer1, 0.01);
            assertEquals(originalAnswer, updatedMdlAnswer2, 0.01);
        });
    }
}
