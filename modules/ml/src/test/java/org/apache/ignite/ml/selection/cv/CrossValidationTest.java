/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.selection.cv;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.paramgrid.RandomStrategy;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetrics;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.junit.Test;

import static org.apache.ignite.ml.common.TrainerTest.twoLinearlySeparableClasses;
import static org.junit.Assert.*;

/**
 * Tests for {@link CrossValidation}.
 */
public class CrossValidationTest {
    /** */
    @Test
    public void testScoreWithGoodDataset() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] {i > 500 ? 1.0 : 0.0, i});

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        DebugCrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new DebugCrossValidation<>();

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        int folds = 4;

        scoreCalculator
            .withUpstreamMap(data)
            .withAmountOfParts(1)
            .withTrainer(trainer)
            .withMetric(new Accuracy<>())
            .withPreprocessor(vectorizer)
            .withAmountOfFolds(folds)
            .isRunningOnPipeline(false);

        verifyScores(folds, scoreCalculator.scoreByFolds());
    }

    /** */
    @Test
    public void testScoreWithGoodDatasetAndBinaryMetrics() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] {i > 500 ? 1.0 : 0.0, i});

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        DebugCrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new DebugCrossValidation<>();

        int folds = 4;

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        scoreCalculator
            .withUpstreamMap(data)
            .withAmountOfParts(1)
            .withTrainer(trainer)
            .withMetric(metrics)
            .withPreprocessor(vectorizer)
            .withAmountOfFolds(folds)
            .isRunningOnPipeline(false);

        verifyScores(folds, scoreCalculator.scoreByFolds());
    }

    /** */
    @Test
    public void testBasicFunctionality() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            data.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(14)
            .withSeed(123L);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]> scoreCalculator =
            new DebugCrossValidation<>();

        int folds = 4;

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        scoreCalculator
            .withUpstreamMap(data)
            .withAmountOfParts(1)
            .withTrainer(trainer)
            .withMetric(metrics)
            .withPreprocessor(vectorizer)
            .withAmountOfFolds(folds)
            .isRunningOnPipeline(false);

        double[] scores = scoreCalculator.scoreByFolds();

        assertEquals(0.8389830508474576, scores[0], 1e-6);
        assertEquals(0.9402985074626866, scores[1], 1e-6);
        assertEquals(0.8809523809523809, scores[2], 1e-6);
        assertEquals(0.9921259842519685, scores[3], 1e-6);
    }

    /** */
    @Test
    public void testGridSearch() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            data.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(14)
            .withSeed(123L);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        ParamGrid paramGrid = new ParamGrid()
            .addHyperParam("maxIterations", trainer::withMaxIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("locIterations", trainer::withLocIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("batchSize", trainer::withBatchSize, new Double[]{1.0, 2.0, 4.0, 8.0, 16.0});

        DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]> scoreCalculator =
            (DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>) new DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>()
                .withUpstreamMap(data)
                .withAmountOfParts(1)
                .withTrainer(trainer)
                .withMetric(metrics)
                .withPreprocessor(vectorizer)
                .withAmountOfFolds(4)
                .isRunningOnPipeline(false)
                .withParamGrid(paramGrid);

        CrossValidationResult crossValidationRes = scoreCalculator.tuneHyperParamterers();


        assertArrayEquals(crossValidationRes.getBestScore(), new double[]{0.9745762711864406, 1.0, 0.8968253968253969, 0.8661417322834646}, 1e-6);
        assertEquals(crossValidationRes.getBestAvgScore(), 0.9343858500738256, 1e-6);
        assertEquals(crossValidationRes.getScoringBoard().size(), 80);
    }

    /** */
    @Test
    public void testRandomSearch() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            data.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(14)
            .withSeed(123L);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        ParamGrid paramGrid = new ParamGrid()
            .withParameterSearchStrategy(
                new RandomStrategy()
                    .withMaxTries(10)
                    .withSeed(1234L)
                    .withSatisfactoryFitness(0.9)
            )
            .addHyperParam("maxIterations", trainer::withMaxIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("locIterations", trainer::withLocIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("batchSize", trainer::withBatchSize, new Double[]{1.0, 2.0, 4.0, 8.0, 16.0});

        DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]> scoreCalculator =
            (DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>) new DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>()
                .withUpstreamMap(data)
                .withAmountOfParts(1)
                .withTrainer(trainer)
                .withMetric(metrics)
                .withPreprocessor(vectorizer)
                .withAmountOfFolds(4)
                .isRunningOnPipeline(false)
                .withParamGrid(paramGrid);

        CrossValidationResult crossValidationRes = scoreCalculator.tuneHyperParamterers();

        assertEquals(crossValidationRes.getBestAvgScore(), 0.9343858500738256, 1e-6);
        assertEquals(crossValidationRes.getScoringBoard().size(), 10);
    }

    /**
     *
     */
    @Test
    public void testRandomSearchWithPipeline() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            data.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(14)
            .withSeed(123L);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        ParamGrid paramGrid = new ParamGrid()
            .withParameterSearchStrategy(
                new RandomStrategy()
                    .withMaxTries(10)
                    .withSeed(1234L)
                    .withSatisfactoryFitness(0.9)
            )
            .addHyperParam("maxIterations", trainer::withMaxIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("locIterations", trainer::withLocIterations, new Double[]{10.0, 100.0, 1000.0, 10000.0})
            .addHyperParam("batchSize", trainer::withBatchSize, new Double[]{1.0, 2.0, 4.0, 8.0, 16.0});


        Pipeline<Integer, double[], Integer, Double> pipeline = new Pipeline<Integer, double[], Integer, Double>()
            .addVectorizer(vectorizer)
            .addTrainer(trainer);

        DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]> scoreCalculator =
            (DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>) new DebugCrossValidation<LogisticRegressionModel, Double, Integer, double[]>()
                .withUpstreamMap(data)
                .withAmountOfParts(1)
                .withPipeline(pipeline)
                .withMetric(metrics)
                .withPreprocessor(vectorizer)
                .withAmountOfFolds(4)
                .isRunningOnPipeline(true)
                .withParamGrid(paramGrid);

        CrossValidationResult crossValidationRes = scoreCalculator.tuneHyperParamterers();

        assertEquals(crossValidationRes.getBestAvgScore(), 0.9343858500738256, 1e-6);
        assertEquals(crossValidationRes.getScoringBoard().size(), 10);
    }

    /** */
    @Test
    public void testScoreWithBadDataset() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] { i, i % 2 == 0 ? 1.0 : 0.0});

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        DebugCrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new DebugCrossValidation<>();

        int folds = 4;

        scoreCalculator
            .withUpstreamMap(data)
            .withAmountOfParts(1)
            .withTrainer(trainer)
            .withMetric(new Accuracy<>())

            .withPreprocessor(vectorizer)
            .withAmountOfFolds(folds)
            .isRunningOnPipeline(false);


        double[] scores = scoreCalculator.scoreByFolds();

        assertEquals(folds, scores.length);

        for (int i = 0; i < folds; i++)
            assertTrue(scores[i] < 0.6);
    }

    /** */
    private void verifyScores(int folds, double[] scores) {
        assertEquals(folds, scores.length);

        for (int i = 0; i < folds; i++)
            assertEquals(1, scores[i], 1e-1);
    }
}




