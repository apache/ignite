// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates usage of continuous mapper. With continuous mapper
 * it is possible to continue mapping jobs asynchronously even after
 * initial {@link org.gridgain.grid.compute.GridComputeTask#map(List, Object)} method completes.
 * <p>
 * String "Hello Continuous Mapper" is passed as an argument for execution
 * of {@link GridContinuousMapperTask}. As an outcome, participating
 * nodes will print out a single word from the passed in string and return
 * number of characters in that word. However, to demonstrate continuous
 * mapping, next word will be mapped to a node only after the result from
 * previous word has been received.
 * <p>
 * Grid task {@link GridContinuousMapperTask} handles actual splitting
 * into sub-jobs, their continuous mapping and remote execution, as well
 * as calculation of the total character count.
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
 * <p>
 * <h1 class="header">XML Configuration</h1>
 * If no specific configuration is provided, GridGain will start with
 * all defaults. For information about GridGain default configuration
 * refer to {@link GridGain} documentation. If you would like to
 * try out different configurations you should pass a path to Spring
 * configuration file as 1st command line argument into this example.
 * The path can be relative to {@code GRIDGAIN_HOME} environment variable.
 * You should also pass the same configuration file to all other
 * grid nodes by executing startup script as follows (you will need
 * to change the actual file name):
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/specific-config-file.xml</pre>
 * <p>
 * GridGain examples come with multiple configuration files you can try.
 * All configuration files are located under {@code GRIDGAIN_HOME/examples/config}
 * folder.
 *
 * @author @java.author
 * @version @java.version
 */
public class ComputeContinuousMapperExample {
    /**
     * @param args Command line arguments (none required).
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            GridComputeTaskFuture<Integer> fut = g.compute().execute(
                GridContinuousMapperTask.class, "Hello Continuous Mapper");

            // Wait for task completion.
            int phraseLen = fut.get();

            System.out.println(">>>");
            System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
            System.out.println(">>>");
        }
    }

    /**
     * Counts number of characters in the given word.
     *
     * @param word Word to count characters in.
     * @return Number of characters in the given word.
     */
    static int charCount(String word) {
        System.out.println(">>>");
        System.out.println(">>> Printing '" + word + "' from grid job at time: " + new Date());
        System.out.println(">>>");

        return word.length();
    }

    /**
     * This task demonstrates how continuous mapper is used. The passed in phrase
     * is split into multiple words and next word is sent out for processing only
     * when the result for the previous word was received.
     * <p>
     * Note that annotation {@link org.gridgain.grid.compute.GridComputeTaskNoResultCache} is optional and tells GridGain
     * not to accumulate results from individual jobs. In this example we increment
     * total character count directly in {@link #result(org.gridgain.grid.compute.GridComputeJobResult, List)} method,
     * and therefore don't need to accumulate them be be processed at reduction step.
     *
     * @author @java.author
     * @version @java.version
     */
    @GridComputeTaskNoResultCache
    private static class GridContinuousMapperTask extends GridComputeTaskAdapter<String, Integer> {
        /** This field will be injected with task continuous mapper. */
        @GridTaskContinuousMapperResource
        private GridComputeTaskContinuousMapper mapper;

        /** Word queue. */
        private final Queue<String> words = new ConcurrentLinkedQueue<>();

        /** Total character count. */
        private final AtomicInteger totalChrCnt = new AtomicInteger(0);

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> grid, String phrase) throws GridException {
            if (phrase == null || phrase.isEmpty())
                throw new GridException("Phrase is empty.");

            // Populate word queue.
            Collections.addAll(words, phrase.split(" "));

            // Sends first word.
            sendWord();

            // Since we have sent at least one job, we are allowed to return
            // 'null' from map method.
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
            // If there is an error, fail-over to another node.
            if (res.getException() != null)
                return super.result(res, rcvd);

            // Add result to total character count.
            totalChrCnt.addAndGet(res.<Integer>getData());

            sendWord();

            // If next word was sent, keep waiting, otherwise work queue is empty and we reduce.
            return GridComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            return totalChrCnt.get();
        }

        /**
         * Sends next queued word to the next node implicitly selected by load balancer.
         *
         * @throws GridException If sending of a word failed.
         */
        private void sendWord() throws GridException {
            // Remove first word from the queue.
            String word = words.poll();

            if (word != null) {
                // Map next word.
                mapper.send(new GridComputeJobAdapter(word) {
                    @Override public Object execute() {
                        String word = argument(0);

                        int cnt = word.length();

                        // Sleep for some time so it will be visually noticeable that
                        // jobs are executed sequentially.
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }

                        return cnt;
                    }
                });
            }
        }
    }
}
