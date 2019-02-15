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

package org.apache.ignite.examples.ml.tree.boosting;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueConvergenceCheckerFactory;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.boosting.GDBRegressionOnTreesTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Example represents a solution for the task of regression learning based on
 * Gradient Boosting on trees implementation. It shows an initialization of {@link GDBRegressionOnTreesTrainer},
 * initialization of Ignite Cache, learning step and comparing of predicted and real values.
 * <p>
 * In this example dataset is created automatically by parabolic function {@code f(x) = x^2}.</p>
 */
public class GDBOnTreesRegressionTrainerExample {
    /**
     * Run example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String... args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> GDB regression trainer example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create cache with training data.
            CacheConfiguration<Integer, double[]> trainingSetCfg = createCacheConfiguration();
            IgniteCache<Integer, double[]> trainingSet = fillTrainingData(ignite, trainingSetCfg);

            // Create regression trainer.
            DatasetTrainer<ModelsComposition, Double> trainer = new GDBRegressionOnTreesTrainer(1.0, 2000, 1, 0.)
                .withCheckConvergenceStgyFactory(new MeanAbsValueConvergenceCheckerFactory(0.001));

            // Train decision tree model.
            Model<Vector, Double> mdl = trainer.fit(
                ignite,
                trainingSet,
                (k, v) -> VectorUtils.of(v[0]),
                (k, v) -> v[1]
            );

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> | Prediction\t| Valid answer \t|");
            System.out.println(">>> ---------------------------------");

            // Calculate score.
            for (int x = -5; x < 5; x++) {
                double predicted = mdl.apply(VectorUtils.of(x));

                System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", predicted, Math.pow(x, 2));
            }

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> GDB regression trainer example completed.");
        }
    }

    /**
     * Create cache configuration.
     */
    @NotNull private static CacheConfiguration<Integer, double[]> createCacheConfiguration() {
        CacheConfiguration<Integer, double[]> trainingSetCfg = new CacheConfiguration<>();
        trainingSetCfg.setName("TRAINING_SET");
        trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        return trainingSetCfg;
    }

    /**
     * Fill parabolic training data.
     *
     * @param ignite Ignite instance.
     * @param trainingSetCfg Training set config.
     */
    @NotNull private static IgniteCache<Integer, double[]> fillTrainingData(Ignite ignite,
        CacheConfiguration<Integer, double[]> trainingSetCfg) {
        IgniteCache<Integer, double[]> trainingSet = ignite.createCache(trainingSetCfg);
        for(int i = -50; i <= 50; i++) {
            double x = ((double)i) / 10.0;
            double y = Math.pow(x, 2);
            trainingSet.put(i, new double[] {x, y});
        }
        return trainingSet;
    }
}
