/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.ml.selection.split;

import java.io.FileNotFoundException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Run linear regression model over dataset split on train and test subsets ({@link TrainTestDatasetSplitter}).
 * <p>
 * Code in this example launches Ignite grid and fills the cache with simple test data.</p>
 * <p>
 * After that it creates dataset splitter and trains the linear regression model based on the specified data using
 * this splitter.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict the target value
 * and compares prediction to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data and split parameters used in this example and re-run it to explore this functionality
 * further.</p>
 */
public class TrainTestDatasetSplitterExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Linear regression model over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.MORTALITY_DATA);

            System.out.println(">>> Create new linear regression trainer object.");
            LinearRegressionLSQRTrainer trainer = new LinearRegressionLSQRTrainer();

            System.out.println(">>> Create new training dataset splitter object.");
            TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>()
                .split(0.75);

            System.out.println(">>> Perform the training to get the model.");
            LinearRegressionModel mdl = trainer.fit(
                ignite,
                dataCache,
                split.getTrainFilter(),
                (k, v) -> v.copyOfRange(1, v.size()),
                (k, v) -> v.get(0)
            );

            System.out.println(">>> Linear regression model: " + mdl);

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> | Prediction\t| Ground Truth\t|");
            System.out.println(">>> ---------------------------------");

            ScanQuery<Integer, Vector> qry = new ScanQuery<>();
            qry.setFilter(split.getTestFilter());

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(qry)) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);

                    double prediction = mdl.predict(inputs);

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                }
            }

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> Linear regression model over cache based dataset usage example completed.");
        }
    }
}
