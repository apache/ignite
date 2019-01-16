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
import org.apache.ignite.ml.composition.stacking.StackedModel;
import org.apache.ignite.ml.composition.stacking.StackedVectorDatasetTrainer;
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
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
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
 * Finally, this example uses {@link BinaryClassificationEvaluator} functionality to compute metrics from predictions.</p>
 */
public class Step_10_Scaling_With_Stacking {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 5 (scaling) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                // Defines first preprocessor that extracts features from an upstream data.
                // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare".
                IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                    = (k, v) -> new Object[] {v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

                IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double)v[1];

                IgniteBiFunction<Integer, Object[], Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(1)
                    .withEncodedFeature(6) // <--- Changed index here.
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

                IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                    .withP(1)
                    .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);
                DecisionTreeClassificationTrainer trainer1 = new DecisionTreeClassificationTrainer(3, 0);
                DecisionTreeClassificationTrainer trainer2 = new DecisionTreeClassificationTrainer(4, 0);

                LogisticRegressionSGDTrainer aggregator = new LogisticRegressionSGDTrainer()
                    .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg));

                StackedModel<Vector, Vector, Double, LogisticRegressionModel> mdl =
                    new StackedVectorDatasetTrainer<>(aggregator)
                    .addTrainerWithDoubleOutput(trainer)
                    .addTrainerWithDoubleOutput(trainer1)
                    .addTrainerWithDoubleOutput(trainer2)
                    .fit(
                        ignite,
                        dataCache,
                        normalizationPreprocessor,
                        lbExtractor
                    );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = BinaryClassificationEvaluator.evaluate(
                    dataCache,
                    mdl,
                    normalizationPreprocessor,
                    lbExtractor,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 5 (scaling) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
