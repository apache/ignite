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

package org.apache.ignite.loadtests.mergesort;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;

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
     * @throws IgniteCheckedException In case of error.
     * @throws IOException In case of file output error.
     */
    public static void main(String[] args) throws IgniteCheckedException, IOException {
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
     */
    private static long sort(Ignite g, int size) {
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