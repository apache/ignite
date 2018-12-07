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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Usage of {@link DecisionTreeClassificationTrainer} to predict death in the disaster.
 * <p>
 * Extract 3 features "pclass", "sibsp", "parch" to use in prediction.</p>
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it trains the model based on the specified data using decision tree classification.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 */
public class Step_1_Read_and_Learn {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 1 (read and learn) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                IgniteBiFunction<Integer, Object[], Vector> featureExtractor
                    = (k, v) -> VectorUtils.of((double) v[0], (double) v[5], (double) v[6]);

                IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                DecisionTreeNode mdl = trainer.fit(
                    ignite,
                    dataCache,
                    featureExtractor, // "pclass", "sibsp", "parch"
                    lbExtractor
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    featureExtractor,
                    lbExtractor,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 1 (read and learn) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
