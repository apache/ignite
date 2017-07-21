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

package org.apache.ignite.examples.computegrid.montecarlo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteReducer;

/**
 * Monte-Carlo example. Demonstrates distributed credit risk calculation.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public final class CreditRiskExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println("Credit risk example started.");

            // Create portfolio.
            Credit[] portfolio = new Credit[5000];

            Random rnd = new Random();

            // Generate some test portfolio items.
            for (int i = 0; i < portfolio.length; i++) {
                portfolio[i] = new Credit(
                    50000 * rnd.nextDouble(), // Credit amount.
                    rnd.nextInt(1000), // Credit term in days.
                    rnd.nextDouble() / 10, // APR.
                    rnd.nextDouble() / 20 + 0.02 // EDF.
                );
            }

            // Forecast horizon in days.
            int horizon = 365;

            // Number of Monte-Carlo iterations.
            int iter = 10000;

            // Percentile.
            double percentile = 0.95;

            // Mark the stopwatch.
            long start = System.currentTimeMillis();

            // Calculate credit risk and print it out.
            // As you can see the ignite enabling is completely hidden from the caller
            // and it is fully transparent to him. In fact, the caller is never directly
            // aware if method was executed just locally or on the 100s of cluster nodes.
            // Credit risk crdRisk is the minimal amount that creditor has to have
            // available to cover possible defaults.

            double crdRisk = ignite.compute().call(jobs(ignite.cluster().nodes().size(), portfolio, horizon, iter, percentile),
                new IgniteReducer<Double, Double>() {
                    /** Collected values sum. */
                    private double sum;

                    /** Collected values count. */
                    private int cnt;

                    /** {@inheritDoc} */
                    @Override public synchronized boolean collect(Double e) {
                        sum += e;
                        cnt++;

                        return true;
                    }

                    /** {@inheritDoc} */
                    @Override public synchronized Double reduce() {
                        return sum / cnt;
                    }
                });

            System.out.println();
            System.out.println("Credit risk [crdRisk=" + crdRisk + ", duration=" +
                (System.currentTimeMillis() - start) + "ms]");
        }
        // We specifically don't do any error handling here to
        // simplify the example. Real application may want to
        // add error handling and application specific recovery.
    }

    /**
     * Creates closures for calculating credit risks.
     *
     * @param clusterSize Size of the cluster.
     * @param portfolio Portfolio.
     * @param horizon Forecast horizon in days.
     * @param iter Number of Monte-Carlo iterations.
     * @param percentile Percentile.
     * @return Collection of closures.
     */
    private static Collection<IgniteCallable<Double>> jobs(int clusterSize, final Credit[] portfolio,
        final int horizon, int iter, final double percentile) {
        // Number of iterations should be done by each node.
        int iterPerNode = Math.round(iter / (float)clusterSize);

        // Number of iterations for the last/the only node.
        int lastNodeIter = iter - (clusterSize - 1) * iterPerNode;

        Collection<IgniteCallable<Double>> clos = new ArrayList<>(clusterSize);

        // Note that for the purpose of this example we perform a simple homogeneous
        // (non weighted) split assuming that all computing resources in this split
        // will be identical. In real life scenarios when heterogeneous environment
        // is used a split that is weighted by, for example, CPU benchmarks of each
        // node in the split will be more efficient. It is fairly easy addition and
        // Ignite comes with convenient Spring-compatible benchmark that can be
        // used for weighted splits.
        for (int i = 0; i < clusterSize; i++) {
            final int nodeIter = i == clusterSize - 1 ? lastNodeIter : iterPerNode;

            clos.add(new IgniteCallable<Double>() {
                /** {@inheritDoc} */
                @Override public Double call() {
                    return new CreditRiskManager().calculateCreditRiskMonteCarlo(
                        portfolio, horizon, nodeIter, percentile);
                }
            });
        }

        return clos;
    }
}