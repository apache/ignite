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

package org.apache.ignite.examples.ml.regression.linear;

import java.io.FileNotFoundException;
import java.util.function.BiFunction;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Example of using Linear Regression model in Apache Ignite for house prices prediction.
 *
 * Description of model can be found in: https://en.wikipedia.org/wiki/Linear_regression .
 * Original dataset can be downloaded from: https://archive.ics.uci.edu/ml/machine-learning-databases/housing/ .
 * Copy of dataset are stored in: modules/ml/src/main/resources/datasets/boston_housing_dataset.txt .
 * Score for regression estimation: R^2 (coefficient of determination).
 * Description of score evaluation can be found in: https://stattrek.com/statistics/dictionary.aspx?definition=coefficient_of_determination .
 */
public class BostonHousePricesPredictionExample {
    /** Runs example. */
    public static void main(String[] args) throws FileNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                System.out.println(">>> Fill dataset cache.");
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.BOSTON_HOUSE_PRICES);
                DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                    .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));

                // This vectorizer works with values in cache of Vector class.
                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST); // FIRST means "label are stored at first coordinate of vector"

                // Splits dataset to train and test samples with 80/20 proportion.
                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>().split(0.8);

                System.out.println(">>> Start traininig.");
                LinearRegressionModel model = trainer.fit(
                    ignite, dataCache,
                    split.getTrainFilter(),
                    vectorizer
                );

                System.out.println(">>> Perform scoring.");
                double u = 0.0; // Parameters for R^2 score evaluation.
                double v = 0.0;
                double meanPrice = computeMeanPrice(dataCache, split.getTestFilter(), vectorizer);
                ScanQuery<Integer, Vector> qry = new ScanQuery<>(split.getTestFilter());
                try (QueryCursor<Cache.Entry<Integer, Vector>> cursor = dataCache.query(qry)) {
                    for (Cache.Entry<Integer, Vector> entry : cursor) {
                        LabeledVector<Double> vec = vectorizer.apply(entry.getKey(), entry.getValue());

                        double realPrice = vec.label();
                        double predictedPrice = model.predict(vec.features());

                        u += Math.pow(realPrice - predictedPrice, 2);
                        v += Math.pow(realPrice - meanPrice, 2);
                    }
                }

                double score = 1 - u / v;
                System.out.println(">>> Model: " + toString(model));
                System.out.println(">>> R^2 score: " + score);
            } finally {
                dataCache.destroy();
            }
        }
    }

    /**
     * Computes mean value of label over dataset.
     *
     * @param cache Cache with dataset.
     * @param filter Filter of testing data.
     * @param vectorizer Vectorizer for feature-value extraction.
     * @return Mean value of label over dataset.
     */
    private static double computeMeanPrice(IgniteCache<Integer, Vector> cache,
        IgniteBiPredicate<Integer, Vector> filter,
        Vectorizer<Integer, Vector, Integer, Double> vectorizer) {

        long countOfExamples = 0;
        double sumOfPrices = 0.0;
        try (QueryCursor<Cache.Entry<Integer, Vector>> cursor = cache.query(new ScanQuery<>(filter))) {
            for (Cache.Entry<Integer, Vector> ent : cursor) {
                sumOfPrices += vectorizer.apply(ent.getKey(), ent.getValue()).label();
                countOfExamples += 1;
            }
        }

        return sumOfPrices / Math.max(countOfExamples, 1);
    }

    /**
     * Prepare pretty string for model.
     * @param model Model.
     * @return String representation of model.
     */
    private static String toString(LinearRegressionModel model) {
        BiFunction<Integer, Double, String> formatter = (idx, val) -> String.format("%.2f*f%d", val, idx);

        Vector weights = model.getWeights();
        StringBuilder sb = new StringBuilder(formatter.apply(0, weights.get(0)));

        for (int fid = 1; fid < weights.size(); fid++) {
            double w = weights.get(fid);
            sb.append(" ").append(w > 0 ? "+" : "-").append(" ")
                .append(formatter.apply(fid, Math.abs(w)));
        }

        double intercept = model.getIntercept();
        sb.append(" ").append(intercept > 0 ? "+" : "-").append(" ")
            .append(String.format("%.2f", Math.abs(intercept)));
        return sb.toString();
    }
}
