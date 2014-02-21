// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.continuation.mergesort;

import org.gridgain.grid.*;

import java.util.*;

/**
 * This example demonstrates how to use continuation feature of GridGain by
 * performing the distributed version of Merge Sort algorithm. Continuations
 * functionality is exposed via {@link org.gridgain.grid.compute.GridComputeJobContext#holdcc()} and
 * {@link org.gridgain.grid.compute.GridComputeJobContext#callcc()} method calls in {@link GridMergeSortTask}.
 * <p>
 * Merge Sort algorithm (http://en.wikipedia.org/wiki/Merge_sort) is one of
 * the most popular and efficient algorithms for sorting arrays.
 * This example shows, how Merge Sort can be distributed amongst many nodes
 * in the grid to sort a large amount of data. You may run it on any number
 * of nodes (including {@code 1}). If there are more than {@code 1} node - the task will
 * be spread amongst the nodes in your topology.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-default.xml</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMergeSortContinuationExample {
    /**
     * Entry point for this example. Generates an input array of random integers,
     * starts the grid, and launches the job. The method then waits for job completion.
     *
     * @param args Program arguments.
     *      <ul>
     *          <li>
     *              <b>1-st argument:</b> absolute or relative path to the configuration
     *              file for the grid (optional).
     *          </li>
     *          <li>
     *              <b>2-nd argument:</b> size of the generated array (optional, default: {@code 30}).
     *          </li>
     *      </ul>
     * @throws GridException In case of error.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            int arrSize = args.length == 2 ? Integer.parseInt(args[1]) : 30;

            int[] bigArr = generateRandomArray(arrSize);

            System.out.println("Unsorted array: " + arrayToString(bigArr));

            long startTime = System.currentTimeMillis();

            int[] res = g.compute().execute(new GridMergeSortTask(), bigArr).get();

            System.out.println("Sorted array: " + arrayToString(res));
            System.out.println("Execution time: " + (System.currentTimeMillis() - startTime) + "ms");
        }
    }

    /**
     * Generates an int array of random elements of specified size.
     *
     * @param size Size of an array.
     * @return The generated array.
     */
    private static int[] generateRandomArray(int size) {
        int[] ret = new int[size];

        Random rnd = new Random();

        for (int i = 0; i < ret.length; i++)
            ret[i] = rnd.nextInt(100);

        return ret;
    }

    /**
     * Returns a short String representation of an array.
     *
     * @param arr An array to output.
     * @return Conversion result: a string representation.
     */
    public static String arrayToString(int[] arr) {
        StringBuilder sb = new StringBuilder();

        sb.append("[size=").append(arr.length).append(", elements=");

        int end = arr.length < 50 ? arr.length : 50;

        for(int i = 0; i < end; i++)
            sb.append(arr[i]).append(i + 1 == end ? "" : ", ");

        if (arr.length >= 50)
            sb.append(", ...");

        sb.append(']');

        return sb.toString();
    }
}
