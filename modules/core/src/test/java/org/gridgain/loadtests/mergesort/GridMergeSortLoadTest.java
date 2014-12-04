/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.mergesort;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;

/**
 * Merge-sort load test.
 */
public class GridMergeSortLoadTest {
    /** Default size of real array to be sorted. */
    private static final int ARR_SIZE = 100000;

    /** A fixed seed that gives equal arrays each run. */
    public static final int FIXED_SEED = 23;

    /**
     * Entry point for this test. Generates an input array of random integers,
     * starts the grid, and launches the job. The method then waits for job completion.
     *
     * @param args Program arguments.
     *      <ul>
     *          <li>
     *              <b>1-st argument:</b> absolute or relative path to the configuration
     *              file for the grid (optional).
     *          </li>
     *          <li>
     *              <b>2-nd argument:</b> size of the generated array (optional, default: {@code 100000}).
     *          </li>
     *          <li>
     *              <b>3-nd argument:</b> size of the generated array for "warm up" (optional, default: {@code 10000}).
     *          </li>
     *      </ul>
     * @throws GridException In case of error.
     * @throws IOException In case of file output error.
     */
    public static void main(String[] args) throws GridException, IOException {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            String outputFileName = args.length >= 1 ? args[0] : null;

            try (Ignite g = G.start(args.length >= 2 ? args[1] : "modules/core/src/test/config/load/merge-sort-base.xml")) {
                int arrRealSize = args.length > 1 ? Integer.parseInt(args[1]) : ARR_SIZE;

                int arrWarmupSize = args.length > 2 ? Integer.parseInt(args[2]) : ARR_SIZE;

                X.println("Test is being executed on the gird of size " + g.cluster().nodes().size() + ".");

                X.println("Performing warm up sorting of int[" + arrWarmupSize + "]...");

                sort(g, arrWarmupSize);

                X.println("Cleaning up after warm-up...");

                // Run GC on all nodes.
                g.compute().broadcast(new GridAbsClosure() {
                    @Override public void apply() {
                        System.gc();
                    }
                });

                X.println("Performing measured sorting of int[" + arrRealSize + "]...");

                long execTime = sort(g, arrRealSize);

                if (outputFileName != null)
                    GridLoadTestUtils.appendLineToFile(
                        outputFileName,
                        "%s,%d",
                        GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                        execTime / 1000);
            }
        }
        finally {
            fileLock.close();
        }
    }

    /**
     * Generates a random array and performs merge sort benchmark.
     *
     * @param g Grid to run sorting on.
     * @param size Size of the generated array, which we sort.
     * @return Sort execution time in milliseconds.
     * @throws GridException If failed.
     */
    private static long sort(Ignite g, int size) throws GridException {
        int[] bigArr = generateRandomArray(size);

        X.println("Array is generated.");

        long startTime = System.currentTimeMillis();

        g.compute().execute(new GridMergeSortLoadTask(), bigArr);

        long execTime = System.currentTimeMillis() - startTime;

        X.println("Sorting is finished. Execution time: " + execTime + "ms");

        return execTime;
    }

    /**
     * Generates an int array of random elements of specified size.
     *
     * @param size Size of an array.
     * @return The generated array.
     */
    private static int[] generateRandomArray(int size) {
        int[] ret = new int[size];

        Random rnd = new Random(FIXED_SEED);

        for (int i = 0; i < ret.length; i++)
            ret[i] = rnd.nextInt(100);

        return ret;
    }
}
