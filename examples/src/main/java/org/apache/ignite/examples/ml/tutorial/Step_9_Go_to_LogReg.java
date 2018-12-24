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
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;

/**
 * Change classification algorithm that was used in {@link Step_8_CV_with_Param_Grid} from decision tree to logistic
 * regression ({@link LogisticRegressionSGDTrainer}) because sometimes this can give the higher accuracy.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it defines how to split the data to train and test sets and configures preprocessors that extract
 * features from an upstream data and perform other desired changes over the extracted data.</p>
 * <p>
 * Then, it tunes hyperparams with K-fold Cross-Validation on the split training set and trains the model based on
 * the processed data using logistic regression and the obtained hyperparams.</p>
 * <p>
 * Finally, this example uses {@link BinaryClassificationEvaluator} functionality to compute metrics from predictions.</p>
 */
public class Step_9_Go_to_LogReg {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 9 (logistic regression) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                // Defines first preprocessor that extracts features from an upstream data.
                // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare"
                IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                    = (k, v) -> new Object[]{v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

                IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                TrainTestSplit<Integer, Object[]> split = new TrainTestDatasetSplitter<Integer, Object[]>()
                    .split(0.75);

                IgniteBiFunction<Integer, Object[], Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(1)
                    .withEncodedFeature(6) // <--- Changed index here
                    .fit(ignite,
                        dataCache,
                        featureExtractor
                );

                IgniteBiFunction<Integer, Object[], Vector> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                    .fit(ignite,
                        dataCache,
                        strEncoderPreprocessor
                    );

                IgniteBiFunction<Integer, Object[], Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Object[]>()
                    .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                // Tune hyperparams with K-fold Cross-Validation on the split training set.
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
                                    IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor
                                        = new NormalizationTrainer<Integer, Object[]>()
                                        .withP(p)
                                        .fit(
                                            ignite,
                                            dataCache,
                                            minMaxScalerPreprocessor
                                        );

                                    LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                                        .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(learningRate),
                                            SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
                                        .withMaxIterations(maxIterations)
                                        .withLocIterations(locIterations)
                                        .withBatchSize(batchSize)
                                        .withSeed(123L);

                                    CrossValidation<LogisticRegressionModel, Double, Integer, Object[]>
                                        scoreCalculator = new CrossValidation<>();

                                    double[] scores = scoreCalculator.score(
                                        trainer,
                                        new Accuracy<>(),
                                        ignite,
                                        dataCache,
                                        split.getTrainFilter(),
                                        normalizationPreprocessor,
                                        lbExtractor,
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

                IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                    .withP(bestP)
                    .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                    .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(bestLearningRate),
                        SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
                    .withMaxIterations(bestMaxIterations)
                    .withLocIterations(bestLocIterations)
                    .withBatchSize(bestBatchSize)
                    .withSeed(123L);

                System.out.println(">>> Perform the training to get the model.");
                LogisticRegressionModel bestMdl = trainer.fit(
                    ignite,
                    dataCache,
                    split.getTrainFilter(),
                    normalizationPreprocessor,
                    lbExtractor
                );

                System.out.println("\n>>> Trained model: " + bestMdl);

                double accuracy = BinaryClassificationEvaluator.evaluate(
                    dataCache,
                    split.getTestFilter(),
                    bestMdl,
                    normalizationPreprocessor,
                    lbExtractor,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 9 (logistic regression) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
