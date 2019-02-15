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

package org.apache.ignite.examples.ml.regression.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

import java.io.FileNotFoundException;

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

            System.out.println(">>> Perform the training to get the model.");

             // This object is used to extract features and vectors from upstream entities which are
             // essentialy tuples of the form (key, value) (in our case (Integer, Vector)).
             // Key part of tuple in our example is ignored.
             // Label is extracted from 0th entry of the value (which is a Vector)
             // and features are all remaining vector part. Alternatively we could use
             // DatasetTrainer#fit(Ignite, IgniteCache, IgniteBiFunction, IgniteBiFunction) method call
             // where there is a separate lambda for extracting label from (key, value) and a separate labmda for
             // extracting features.
            FeatureLabelExtractor<Integer, Vector, Double> extractor =
                (k, v) -> new LabeledVector<>(v.copyOfRange(1, v.size()), v.get(0));

            LinearRegressionModel mdl = trainer.fit(
                ignite,
                dataCache,
                extractor
            );

            double rmse = Evaluator.evaluate(
                dataCache,
                mdl,
                CompositionUtils.asFeatureExtractor(extractor),
                CompositionUtils.asLabelExtractor(extractor),
                new RegressionMetrics()
            );

            System.out.println("\n>>> Rmse = " + rmse);

            System.out.println(">>> Linear regression model over cache based dataset usage example completed.");
        }
    }
}
