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

package org.apache.ignite.examples.ml.tutorial;

import java.io.FileNotFoundException;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.cv.CrossValidationResult;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * To choose the best hyperparameters the cross-validation with {@link ParamGrid} will be used in this example.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it defines how to split the data to train and test sets and configures preprocessors that extract
 * features from an upstream data and perform other desired changes over the extracted data.</p>
 * <p>
 * Then, it tunes hyperparams with K-fold Cross-Validation on the split training set and trains the model based on
 * the processed data using decision tree classification and the obtained hyperparams.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 * <p>
 * The purpose of cross-validation is model checking, not model building.</p>
 * <p>
 * We train {@code k} different models.</p>
 * <p>
 * They differ in that {@code 1/(k-1)}th of the training data is exchanged against other cases.</p>
 * <p>
 * These models are sometimes called surrogate models because the (average) performance measured for these models
 * is taken as a surrogate of the performance of the model trained on all cases.</p>
 * <p>
 * All scenarios are described there: https://sebastianraschka.com/faq/docs/evaluate-a-model.html</p>
 */
public class Step_8_CV_with_Param_Grid_and_metrics_and_pipeline {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 8 (cross-validation with param grid) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                // Defines first preprocessor that extracts features from an upstream data.
                // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare" .
                IgniteBiFunction<Integer, Object[], Vector> featureExtractor = (k, v) -> {
                    double[] data = new double[]{
                        (double) v[0],
                        (double) v[4],
                        (double) v[5],
                        (double) v[6],
                        (double) v[8],
                    };
                    data[0] = Double.isNaN(data[0]) ? 0 : data[0];
                    data[1] = Double.isNaN(data[1]) ? 0 : data[1];
                    data[2] = Double.isNaN(data[2]) ? 0 : data[2];
                    data[3] = Double.isNaN(data[3]) ? 0 : data[3];
                    data[4] = Double.isNaN(data[4]) ? 0 : data[4];

                    return VectorUtils.of(data);
                };


                IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                TrainTestSplit<Integer, Object[]> split = new TrainTestDatasetSplitter<Integer, Object[]>()
                    .split(0.75);

                Pipeline<Integer, Object[], Vector> pipeline = new Pipeline<Integer, Object[], Vector>()
                    .addFeatureExtractor(featureExtractor)
                    .addLabelExtractor(lbExtractor)
                    .addPreprocessingTrainer(new ImputerTrainer<Integer, Object[]>())
                    .addPreprocessingTrainer(new MinMaxScalerTrainer<Integer, Object[]>())
                    .addTrainer(new DecisionTreeClassificationTrainer(5, 0));

                // Tune hyperparams with K-fold Cross-Validation on the split training set.

                CrossValidation<DecisionTreeNode, Double, Integer, Object[]> scoreCalculator
                    = new CrossValidation<>();

                ParamGrid paramGrid = new ParamGrid()
                    .addHyperParam("maxDeep", new Double[]{1.0, 2.0, 3.0, 4.0, 5.0, 10.0})
                    .addHyperParam("minImpurityDecrease", new Double[]{0.0, 0.25, 0.5});

                BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
                    .withNegativeClsLb(0.0)
                    .withPositiveClsLb(1.0)
                    .withMetric(BinaryClassificationMetricValues::accuracy);

                CrossValidationResult crossValidationRes = scoreCalculator.score(
                    pipeline,
                    metrics,
                    ignite,
                    dataCache,
                    split.getTrainFilter(),
                    lbExtractor,
                    3,
                    paramGrid
                );

                System.out.println("Train with maxDeep: " + crossValidationRes.getBest("maxDeep")
                    + " and minImpurityDecrease: " + crossValidationRes.getBest("minImpurityDecrease"));

                System.out.println(crossValidationRes);

                System.out.println("Best score: " + Arrays.toString(crossValidationRes.getBestScore()));
                System.out.println("Best hyper params: " + crossValidationRes.getBestHyperParams());
                System.out.println("Best average score: " + crossValidationRes.getBestAvgScore());

                crossValidationRes.getScoringBoard().forEach((hyperParams, score)
                    -> System.out.println("Score " + Arrays.toString(score) + " for hyper params " + hyperParams));

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
