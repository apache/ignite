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

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;

/**
 * Run linear regression model based on <a href="http://web.stanford.edu/group/SOL/software/lsqr/">LSQR algorithm</a>
 * ({@link LinearRegressionLSQRTrainer}) over cached dataset.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with simple test data.</p>
 * <p>
 * After that it trains the linear regression model based on the specified data.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict the target value
 * and compares prediction to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class LinearRegressionLSQRTrainerExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Linear regression model over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.MORTALITY_DATA);

                System.out.println(">>> Create new linear regression trainer object.");
                LinearRegressionLSQRTrainer trainer = new LinearRegressionLSQRTrainer();

                System.out.println(">>> Perform the training to get the model.");

                // This object is used to extract features and vectors from upstream entities which are
                // essentially tuples of the form (key, value) (in our case (Integer, Vector)).
                // Key part of tuple in our example is ignored.
                // Label is extracted from 0th entry of the value (which is a Vector)
                // and features are all remaining vector part. Alternatively we could use
                // DatasetTrainer#fit(Ignite, IgniteCache, IgniteBiFunction, IgniteBiFunction) method call
                // where there is a separate lambda for extracting label from (key, value) and a separate labmda for
                // extracting features.

                LinearRegressionModel mdl = trainer.fit(ignite, dataCache, new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST));

                double rmse = Evaluator.evaluate(
                    dataCache, mdl,
                    new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST),
                    MetricName.RMSE
                );

                System.out.println("\n>>> Rmse = " + rmse);

                System.out.println(">>> Linear regression model over cache based dataset usage example completed.");
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
