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

package org.apache.ignite.examples.ml.tree;

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;

/**
 * Example of using classification algorithms for fraud detection problem.
 * <p>
 * Description of models can be found in: https://en.wikipedia.org/wiki/Logistic_regression and
 * https://en.wikipedia.org/wiki/Decision_tree_learning . Original dataset can be downloaded from:
 * https://www.kaggle.com/mlg-ulb/creditcardfraud/ . Copy of dataset are stored in:
 * modules/ml/src/main/resources/datasets/fraud_detection.csv . Score for clusterizer estimation: accuracy, recall,
 * precision, f1-score . Description of entropy can be found in: https://en.wikipedia.org/wiki/Evaluation_of_binary_classifiers
 * .
 */
public class FraudDetectionExample {
    /** Run example. */
    public static void main(String[] args) throws IOException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                System.out.println(">>> Fill dataset cache.");
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.FRAUD_DETECTION);

                // This vectorizer works with values in cache of Vector class.
                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.LAST); // LAST means "label are stored at last coordinate of vector"

                // Splits dataset to train and test samples with 80/20 proportion.
                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>().split(0.8);

                System.out.println(">>> Perform logistic regression.");
                trainAndEstimateModel(ignite, dataCache,
                    new LogisticRegressionSGDTrainer()
                        .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0)),
                    vectorizer, split
                );

                System.out.println("\n\n>>> Perform decision tree classifier.");
                trainAndEstimateModel(ignite, dataCache,
                    new DecisionTreeClassificationTrainer()
                        .withMaxDeep(10.)
                        .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0)),
                    vectorizer, split
                );
            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Train model and estimate it.
     *
     * @param ignite     Ignite
     * @param dataCache  Data set cache.
     * @param trainer    Trainer.
     * @param vectorizer Upstream vectorizer.
     * @param splitter   Train test splitter.
     */
    private static void trainAndEstimateModel(Ignite ignite,
        IgniteCache<Integer, Vector> dataCache,
        DatasetTrainer<? extends IgniteModel<Vector, Double>, Double> trainer,
        Vectorizer<Integer, Vector, Integer, Double> vectorizer, TrainTestSplit<Integer, Vector> splitter) {
        System.out.println(">>> Start training.");
        IgniteModel<Vector, Double> mdl = trainer.fit(
            ignite, dataCache,
            splitter.getTrainFilter(),
            vectorizer
        );

        System.out.println(">>> Perform scoring.");
        System.out.println(Evaluator.evaluateBinaryClassification(dataCache, splitter.getTestFilter(), mdl, vectorizer));
    }
}
