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

package org.apache.ignite.examples.ml.knn;

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;

/**
 * Example of using Knn model in Apache Ignite for iris class predicion.
 * <p>
 * Description of model can be found in: https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm . Original dataset
 * can be downloaded from: https://archive.ics.uci.edu/ml/datasets/Wholesale+customers . Copy of dataset are stored in:
 * https://archive.ics.uci.edu/ml/datasets/iris . Score for classifier estimation: accuracy . Description of score can
 * be found in: https://stattrek.com/statistics/dictionary.aspx?definition=accuracy .
 */
public class IrisClassificationExample {
    /**
     * Runs example.
     */
    public static void main(String[] args) throws IOException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                System.out.println(">>> Fill dataset cache.");
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.IRIS);
                KNNClassificationTrainer trainer = ((KNNClassificationTrainer)new KNNClassificationTrainer()
                    .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0)))
                    .withK(3)
                    .withDistanceMeasure(new EuclideanDistance())
                    .withWeighted(true);

                // This vectorizer works with values in cache of Vector class.
                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST); // FIRST means "label are stored at first coordinate of vector"

                // Splits dataset to train and test samples with 60/40 proportion.
                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>().split(0.6);

                System.out.println(">>> Start traininig.");
                KNNClassificationModel mdl = trainer.fit(
                    ignite, dataCache,
                    split.getTrainFilter(),
                    vectorizer
                );

                System.out.println(">>> Perform scoring.");
                double accuracy = Evaluator.evaluate(
                    dataCache,
                    split.getTestFilter(),
                    mdl,
                    vectorizer,
                    new Accuracy<>()
                );

                System.out.println(">> Model accuracy: " + accuracy);
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
}
