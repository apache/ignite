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
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.pipeline.PipelineMdl;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.thread.IgniteThread;

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
public class Step_5_Scaling_with_Pipeline {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> Tutorial step 5 (scaling) via Pipeline example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_5_Scaling_with_Pipeline.class.getSimpleName(), () -> {
                try {
                    IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                    // Defines first preprocessor that extracts features from an upstream data.
                    // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare".
                    IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                        = (k, v) -> new Object[]{v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

                    IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                    PipelineMdl<Integer, Object[]> mdl = new Pipeline<Integer, Object[], Object[]>()
                        .addFeatureExtractor(featureExtractor)
                        .addLabelExtractor(lbExtractor)
                        .addPreprocessor(new EncoderTrainer<Integer, Object[]>()
                            .withEncoderType(EncoderType.STRING_ENCODER)
                            .withEncodedFeature(1)
                            .withEncodedFeature(6))
                        .addPreprocessor(new ImputerTrainer<Integer, Object[]>())
                        .addPreprocessor(new MinMaxScalerTrainer<Integer, Object[]>())
                        .addPreprocessor(new NormalizationTrainer<Integer, Object[]>()
                            .withP(1))
                        .addTrainer(new DecisionTreeClassificationTrainer(5, 0))
                        .fit(ignite, dataCache);

                    System.out.println("\n>>> Trained model: " + mdl);

                    double accuracy = Evaluator.evaluate(
                        dataCache,
                        mdl,
                        mdl.getFeatureExtractor(),
                        mdl.getLabelExtractor(),
                        new Accuracy<>()
                    );

                    System.out.println("\n>>> Accuracy " + accuracy);
                    System.out.println("\n>>> Test Error " + (1 - accuracy));

                    System.out.println(">>> Tutorial step 5 (scaling) via Pipeline example completed.");
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
