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
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderTrainer;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidationScoreCalculator;
import org.apache.ignite.ml.selection.score.AccuracyScoreCalculator;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.thread.IgniteThread;

/**
 * Maybe the another algorithm can give us the higher accuracy?
 *
 * Let's win with the LogisticRegressionSGDTrainer!
 */
public class Step_9_Change_algorithm {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_9_Change_algorithm.class.getSimpleName(), () -> {
                try {
                    IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                    // Defines first preprocessor that extracts features from an upstream data.
                    // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare"
                    IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                        = (k, v) -> new Object[]{v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

                    TrainTestSplit<Integer, Object[]> split = new TrainTestDatasetSplitter<Integer, Object[]>()
                        .split(0.75);

                    IgniteBiFunction<Integer, Object[], double[]> strEncoderPreprocessor = new StringEncoderTrainer<Integer, Object[]>()
                        .encodeFeature(1)
                        .encodeFeature(6) // <--- Changed index here
                        .fit(ignite,
                            dataCache,
                            featureExtractor
                    );

                    IgniteBiFunction<Integer, Object[], double[]> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                        .fit(ignite,
                            dataCache,
                            strEncoderPreprocessor
                        );

                    IgniteBiFunction<Integer, Object[], double[]> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Object[]>()
                        .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                    // Tune hyperparams with K-fold Cross-Validation on the splitted training set.
                    int[] pSet = new int[]{1, 2};
                    int[] maxIterationsSet = new int[]{ 100, 1000};
                    int[] batchSizeSet = new int[]{100, 10};
                    int[] locIterationsSet = new int[]{10, 100};
                    double[] learningRateSet = new double[]{0.1, 0.2, 0.5};


                    int bestP = 1;
                    int bestMaxIterations = 100;
                    int bestBatchSize = 10;
                    int bestLocIterations = 10;
                    double bestLearningRate = 0.0;
                    double avg = Double.MIN_VALUE;

                    for(int p: pSet){
                        for(int maxIterations: maxIterationsSet) {
                            for (int batchSize : batchSizeSet) {
                                for (int locIterations : locIterationsSet) {
                                    for (double learningRate : learningRateSet) {

                                        IgniteBiFunction<Integer, Object[], double[]> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                                            .withP(p)
                                            .fit(
                                                ignite,
                                                dataCache,
                                                minMaxScalerPreprocessor
                                            );

                                        LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
                                            new SimpleGDUpdateCalculator(learningRate),
                                            SimpleGDParameterUpdate::sumLocal,
                                            SimpleGDParameterUpdate::avg
                                        ), maxIterations, batchSize, locIterations, 123L);

                                        CrossValidationScoreCalculator<LogisticRegressionModel, Double, Integer, Object[]> scoreCalculator
                                            = new CrossValidationScoreCalculator<>();

                                        double[] scores = scoreCalculator.score(
                                            trainer,
                                            new AccuracyScoreCalculator<>(),
                                            ignite,
                                            dataCache,
                                            split.getTrainFilter(),
                                            normalizationPreprocessor,
                                            (k, v) -> (double)v[1],
                                            3
                                        );

                                        System.out.println("Scores are: " + Arrays.toString(scores));

                                        final double currAvg = Arrays.stream(scores).average().orElse(Double.MIN_VALUE);

                                        if (currAvg > avg) {
                                            avg = currAvg;
                                            bestP = p;
                                            bestMaxIterations = maxIterations;
                                            bestBatchSize = batchSize;
                                            bestLearningRate = learningRate;
                                            bestLocIterations = locIterations;
                                        }

                                        System.out.println("Avg is: " + currAvg
                                            + " with p: " + p
                                            + " with maxIterations: " + maxIterations
                                            + " with batchSize: " + batchSize
                                            + " with learningRate: " + learningRate
                                            + " with locIterations: " + locIterations
                                        );
                                    }
                                }
                            }
                        }
                    }

                    System.out.println("Train "
                        + " with p: " + bestP
                        + " with maxIterations: " + bestMaxIterations
                        + " with batchSize: " + bestBatchSize
                        + " with learningRate: " + bestLearningRate
                        + " with locIterations: " + bestLocIterations
                    );

                    IgniteBiFunction<Integer, Object[], double[]> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                        .withP(bestP)
                        .fit(
                            ignite,
                            dataCache,
                            minMaxScalerPreprocessor
                        );

                    LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(bestLearningRate),
                        SimpleGDParameterUpdate::sumLocal,
                        SimpleGDParameterUpdate::avg
                    ), bestMaxIterations,  bestBatchSize, bestLocIterations, 123L);

                    System.out.println(">>> Perform the training to get the model.");
                    LogisticRegressionModel bestMdl = trainer.fit(
                        ignite,
                        dataCache,
                        split.getTrainFilter(),
                        normalizationPreprocessor,
                        (k, v) -> (double)v[1]
                    );


                    System.out.println("----------------------------------------------------------------");
                    System.out.println("| Prediction\t| Ground Truth\t| Name\t|");
                    System.out.println("----------------------------------------------------------------");

                    int amountOfErrors = 0;
                    int totalAmount = 0;

                    // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                    int[][] confusionMtx = {{0, 0}, {0, 0}};

                    ScanQuery<Integer, Object[]> qry = new ScanQuery<>();
                    qry.setFilter(split.getTestFilter());

                    try (QueryCursor<Cache.Entry<Integer, Object[]>> observations = dataCache.query(qry)) {
                        for (Cache.Entry<Integer, Object[]> observation : observations) {

                            Object[] val = observation.getValue();
                            double groundTruth = (double)val[1];
                            String name = (String)val[2];

                            double prediction = bestMdl.apply(new DenseLocalOnHeapVector(
                                normalizationPreprocessor.apply(observation.getKey(), val)));

                            totalAmount++;
                            if (groundTruth != prediction)
                                amountOfErrors++;

                            int idx1 = (int)prediction;
                            int idx2 = (int)groundTruth;

                            confusionMtx[idx1][idx2]++;

                            System.out.printf("| %.4f\t\t| %.4f\t\t\t\t\t\t| %s\t\t\t\t\t\t\t\t\t\t|\n", prediction, groundTruth, name);
                        }

                        System.out.println("---------------------------------");

                        System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                        double accuracy = 1 - amountOfErrors / (double)totalAmount;
                        System.out.println("\n>>> Accuracy " + accuracy);
                        System.out.println("\n>>> Test Error " + (1 - accuracy));

                        System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));
                        System.out.println(">>> ---------------------------------");
                    }
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
}
