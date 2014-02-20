// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.primenumbers;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.GridClosureCallMode.*;

/**
 * Prime Number calculation example based on GridGain 3.0 API.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-default.xml</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * all of the nodes will participate in task execution (check node
 * output).
 * <p>
 * Note that when running this example on a multi-core box, simply
 * starting additional grid node on the same box will speed up
 * prime number calculation by a factor of 2.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridPrimeExample {
    /**
     * Starts up grid and checks all provided values for prime.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Values we want to check for prime.
        long[] checkVals = {
            32452841, 32452843, 32452847, 32452849, 236887699, 217645199
        };

        System.out.println(">>>");
        System.out.println(">>> Starting to check the following numbers for primes: " + Arrays.toString(checkVals));

        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            long start = System.currentTimeMillis();

            for (long checkVal : checkVals) {
                Long divisor = g.compute().reduce(
                    SPREAD,
                    closures(g.nodes().size(), checkVal),
                    new GridReducer<Long, Long>() {
                        /** Last divisor value. */
                        private Long divisor;

                        /** {@inheritDoc} */
                        @Override public boolean collect(Long e) {
                            // If divisor is found then stop collecting.
                            return (divisor = e) == null;
                        }

                        /** {@inheritDoc} */
                        @Override public Long apply() {
                            return divisor;
                        }
                    }
                ).get();

                if (divisor == null)
                    System.out.println(">>> Value '" + checkVal + "' is a prime number");
                else
                    System.out.println(">>> Value '" + checkVal + "' is divisible by '" + divisor + '\'');
            }

            long totalTime = System.currentTimeMillis() - start;

            System.out.println(">>> Total time to calculate all primes (milliseconds): " + totalTime);
            System.out.println(">>>");
        }
    }

    /**
     * Creates closures for checking passed in value for prime.
     * <p>
     * Every closure gets a range of divisors to check. The lower and
     * upper boundaries of this range are passed into closure.
     * Closures invoke {@link GridPrimeChecker} to check if the value
     * passed in is divisible by any of the divisors in the range.
     * Refer to {@link GridPrimeChecker} for algorithm specifics (it is
     * very unsophisticated).
     *
     * @param gridSize Size of the grid.
     * @param val Value to check.
     * @return Collection of closures.
     */
    @SuppressWarnings({"TooBroadScope"})
    private static Collection<GridOutClosure<Long>> closures(int gridSize, final long val) {
        Collection<GridOutClosure<Long>> cls = new ArrayList<>(gridSize);

        long taskMinRange = 2;

        long numbersPerTask = val / gridSize < 10 ? 10 : val / gridSize;

        long jobMinRange;
        long jobMaxRange = 0;

        // In this loop we create as many grid jobs as
        // there are nodes in the grid.
        for (int i = 0; jobMaxRange < val; i++) {
            jobMinRange = i * numbersPerTask + taskMinRange;
            jobMaxRange = (i + 1) * numbersPerTask + taskMinRange - 1;

            if (jobMaxRange > val)
                jobMaxRange = val;

            final long min = jobMinRange;
            final long max = jobMaxRange;

            cls.add(new GridOutClosure<Long>() {
                /**
                 * Check if the value passed in is divisible by
                 * any of the divisors in the range. If so,
                 * return the first divisor found, otherwise
                 * return {@code null}.
                 *
                 * @return First divisor found or {@code null} if no
                 *      divisor was found.
                 */
                @Nullable @Override public Long apply() {
                    // Return first divisor found or null if no
                    // divisor was found.
                    return GridPrimeChecker.checkPrime(val, min, max);
                }
            });
        }

        return cls;
    }
}
