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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.composition.bagging.BaggedModel;
import org.apache.ignite.ml.composition.bagging.BaggedTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.TrainerTransformers;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;

/**
 * {@link MinMaxScalerTrainer} and {@link NormalizationTrainer} are used in this example due to different values
 * distribution in columns and rows.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it defines preprocessors that extract features from an upstream data and perform other desired changes
 * over the extracted data, including the scaling.</p>
 * <p>
 * Then, it trains the model based on the processed data using decision tree classification.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 */
public class Step_10_Bagging {
    /**
     * Run example.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 10 (Bagging) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Vector> dataCache = TitanicUtils.readPassengers(ignite);

                // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare".
                final Vectorizer<Integer, Vector, Integer, Double> vectorizer
                    = new DummyVectorizer<Integer>(0, 3, 4, 5, 6, 8, 10).labeled(1);

                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>()
                    .split(0.75);

                Preprocessor<Integer, Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Vector>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(1)
                    .withEncodedFeature(6) // <--- Changed index here.
                    .fit(ignite,
                        dataCache,
                        vectorizer
                    );

                Preprocessor<Integer, Vector> imputingPreprocessor = new ImputerTrainer<Integer, Vector>()
                    .fit(ignite,
                        dataCache,
                        strEncoderPreprocessor
                    );

                Preprocessor<Integer, Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Vector>()
                    .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                Preprocessor<Integer, Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Vector>()
                    .withP(1)
                    .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                BaggedTrainer<Double> baggedTrainer = TrainerTransformers.makeBagged(
                    trainer,
                    10,
                    0.6,
                    4,
                    3,
                    new OnMajorityPredictionsAggregator())
                    .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1));

                BaggedModel mdl = baggedTrainer.fit(
                    ignite,
                    dataCache,
                    split.getTrainFilter(),
                    normalizationPreprocessor
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    split.getTestFilter(),
                    mdl,
                    normalizationPreprocessor,
                    MetricName.ACCURACY
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 10 (Bagging) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
