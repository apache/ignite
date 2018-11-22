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

package org.apache.ignite.examples.ml.regression.logistic.multiclass;

import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassModel;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassTrainer;

/**
 * Run Logistic Regression multi-class classification trainer ({@link LogRegressionMultiClassModel}) over distributed
 * dataset to build two models: one with minmaxscaling and one without minmaxscaling.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (preprocessed
 * <a href="https://archive.ics.uci.edu/ml/datasets/Glass+Identification">Glass dataset</a>).</p>
 * <p>
 * After that it trains two logistic regression models based on the specified data - one model is with minmaxscaling
 * and one without minmaxscaling.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained models to predict the target value,
 * compares prediction to expected outcome (ground truth), and builds
 * <a href="https://en.wikipedia.org/wiki/Confusion_matrix">confusion matrices</a>.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class LogRegressionMultiClassClassificationExampleWithOneVsRest {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Logistic Regression Multi-class classification model over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.GLASS_IDENTIFICATION);




            LogRegressionMultiClassTrainer<?> trainer = new LogRegressionMultiClassTrainer<>()
                .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate::sumLocal,
                        SimpleGDParameterUpdate::avg
                    ))
                .withAmountOfIterations(100000)
                .withAmountOfLocIterations(10)
                .withBatchSize(100)
                .withSeed(123L);

            LogRegressionMultiClassModel mdl = trainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size()),
                (k, v) -> v.get(0)
            );

            System.out.println(">>> SVM Multi-class model");
            System.out.println(mdl.toString());

            MinMaxScalerTrainer<Integer, Vector> normalizationTrainer = new MinMaxScalerTrainer<>();

            IgniteBiFunction<Integer, Vector, Vector> preprocessor = normalizationTrainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size())
            );

            LogRegressionMultiClassModel mdlWithNormalization = trainer.fit(
                ignite,
                dataCache,
                preprocessor,
                (k, v) -> v.get(0)
            );

            System.out.println(">>> Logistic Regression Multi-class model with normalization");
            System.out.println(mdlWithNormalization.toString());

            System.out.println(">>> ----------------------------------------------------------------");
            System.out.println(">>> | Prediction\t| Prediction with Normalization\t| Ground Truth\t|");
            System.out.println(">>> ----------------------------------------------------------------");

            int amountOfErrors = 0;
            int amountOfErrorsWithNormalization = 0;
            int totalAmount = 0;

            // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
            int[][] confusionMtx = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
            int[][] confusionMtxWithNormalization = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);

                    double prediction = mdl.apply(inputs);
                    double predictionWithNormalization = mdlWithNormalization.apply(inputs);

                    totalAmount++;

                    // Collect data for model
                    if(groundTruth != prediction)
                        amountOfErrors++;

                    int idx1 = (int)prediction == 1 ? 0 : ((int)prediction == 3 ? 1 : 2);
                    int idx2 = (int)groundTruth == 1 ? 0 : ((int)groundTruth == 3 ? 1 : 2);

                    confusionMtx[idx1][idx2]++;

                    // Collect data for model with normalization
                    if(groundTruth != predictionWithNormalization)
                        amountOfErrorsWithNormalization++;

                    idx1 = (int)predictionWithNormalization == 1 ? 0 : ((int)predictionWithNormalization == 3 ? 1 : 2);
                    idx2 = (int)groundTruth == 1 ? 0 : ((int)groundTruth == 3 ? 1 : 2);

                    confusionMtxWithNormalization[idx1][idx2]++;

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t\t\t\t\t| %.4f\t\t|\n", prediction, predictionWithNormalization, groundTruth);
                }
                System.out.println(">>> ----------------------------------------------------------------");
                System.out.println("\n>>> -----------------Logistic Regression model-------------");
                System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                System.out.println("\n>>> Accuracy " + (1 - amountOfErrors / (double)totalAmount));
                System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));

                System.out.println("\n>>> -----------------Logistic Regression model with Normalization-------------");
                System.out.println("\n>>> Absolute amount of errors " + amountOfErrorsWithNormalization);
                System.out.println("\n>>> Accuracy " + (1 - amountOfErrorsWithNormalization / (double)totalAmount));
                System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtxWithNormalization));

                System.out.println(">>> Logistic Regression Multi-class classification model over cached dataset usage example completed.");
            }
        }
    }
}
