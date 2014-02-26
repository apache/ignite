// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute.montecarlo;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.examples.compute.*;

import java.util.*;

/**
 * Monte-Carlo example based on GridGain 3.0 API.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-compute.xml</pre>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 * <p>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * all of the nodes will participate in task execution (check node
 * output).
 *
 * @author @java.author
 * @version @java.version
 */
public final class CreditRiskExample {
    /**
     * @param args Command arguments.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
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
            // As you can see the grid enabling is completely hidden from the caller
            // and it is fully transparent to him. In fact, the caller is never directly
            // aware if method was executed just locally or on the 100s of grid nodes.
            // Credit risk crdRisk is the minimal amount that creditor has to have
            // available to cover possible defaults.

            double crdRisk = g.compute().call(jobs(g.nodes().size(), portfolio, horizon, iter, percentile),
                new GridReducer<Double, Double>() {
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
                }).get();

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
     * @param gridSize Size of the grid.
     * @param portfolio Portfolio.
     * @param horizon Forecast horizon in days.
     * @param iter Number of Monte-Carlo iterations.
     * @param percentile Percentile.
     * @return Collection of closures.
     */
    private static Collection<GridCallable<Double>> jobs(int gridSize, final Credit[] portfolio,
        final int horizon, int iter, final double percentile) {
        // Number of iterations should be done by each node.
        int iterPerNode = Math.round(iter / (float)gridSize);

        // Number of iterations for the last/the only node.
        int lastNodeIter = iter - (gridSize - 1) * iterPerNode;

        Collection<GridCallable<Double>> clos = new ArrayList<>(gridSize);

        // Note that for the purpose of this example we perform a simple homogeneous
        // (non weighted) split assuming that all computing resources in this split
        // will be identical. In real life scenarios when heterogeneous environment
        // is used a split that is weighted by, for example, CPU benchmarks of each
        // node in the split will be more efficient. It is fairly easy addition and
        // GridGain comes with convenient Spring-compatible benchmark that can be
        // used for weighted splits.
        for (int i = 0; i < gridSize; i++) {
            final int nodeIter = i == gridSize - 1 ? lastNodeIter : iterPerNode;

            clos.add(new GridCallable<Double>() {
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
