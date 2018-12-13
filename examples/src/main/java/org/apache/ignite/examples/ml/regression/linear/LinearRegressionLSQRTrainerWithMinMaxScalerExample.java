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
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerPreprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Run linear regression model based on <a href="http://web.stanford.edu/group/SOL/software/lsqr/">LSQR algorithm</a>
 * ({@link LinearRegressionLSQRTrainer}) over cached dataset that was created using
 * a minmaxscaling preprocessor ({@link MinMaxScalerTrainer}, {@link MinMaxScalerPreprocessor}).
 * <p>
 * Code in this example launches Ignite grid, fills the cache with simple test data, and defines minmaxscaling
 * trainer and preprocessor.</p>
 * <p>
 * After that it trains the linear regression model based on the specified data that has been processed
 * using minmaxscaling.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict predict the target
 * value and compares prediction to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class LinearRegressionLSQRTrainerWithMinMaxScalerExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Linear regression model with Min Max Scaling preprocessor over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.MORTALITY_DATA);

            System.out.println(">>> Create new MinMaxScaler trainer object.");
            MinMaxScalerTrainer<Integer, Vector> minMaxScalerTrainer = new MinMaxScalerTrainer<>();

            System.out.println(">>> Perform the training to get the MinMaxScaler preprocessor.");
            IgniteBiFunction<Integer, Vector, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size())
            );

            System.out.println(">>> Create new linear regression trainer object.");
            LinearRegressionLSQRTrainer trainer = new LinearRegressionLSQRTrainer();

            System.out.println(">>> Perform the training to get the model.");
            LinearRegressionModel mdl = trainer.fit(ignite, dataCache, preprocessor, (k, v) -> v.get(0));

            System.out.println(">>> Linear regression model: " + mdl);

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> | Prediction\t| Ground Truth\t|");
            System.out.println(">>> ---------------------------------");

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Integer key = observation.getKey();
                    Vector val = observation.getValue();
                    double groundTruth = val.get(0);

                    double prediction = mdl.apply(preprocessor.apply(key, val));

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                }
            }

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> Linear regression model with MinMaxScaler preprocessor over cache based dataset usage example completed.");
        }
    }
}
