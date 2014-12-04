/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates usage of continuous mapper. With continuous mapper
 * it is possible to continue mapping jobs asynchronously even after
 * initial {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method completes.
 * <p>
 * String "Hello Continuous Mapper" is passed as an argument for execution
 * of {@link GridContinuousMapperTask}. As an outcome, participating
 * nodes will print out a single word from the passed in string and return
 * number of characters in that word. However, to demonstrate continuous
 * mapping, next word will be mapped to a node only after the result from
 * previous word has been received.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeContinuousMapperExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        System.out.println();
        System.out.println(">>> Compute continuous mapper example started.");

        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            int phraseLen = g.compute().execute(GridContinuousMapperTask.class, "Hello Continuous Mapper");

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
        }
    }

    /**
     * This task demonstrates how continuous mapper is used. The passed in phrase
     * is split into multiple words and next word is sent out for processing only
     * when the result for the previous word was received.
     * <p>
     * Note that annotation {@link org.apache.ignite.compute.ComputeTaskNoResultCache} is optional and tells GridGain
     * not to accumulate results from individual jobs. In this example we increment
     * total character count directly in {@link #result(org.apache.ignite.compute.ComputeJobResult, List)} method,
     * and therefore don't need to accumulate them be be processed at reduction step.
     */
    @ComputeTaskNoResultCache
    private static class GridContinuousMapperTask extends ComputeTaskAdapter<String, Integer> {
        /** This field will be injected with task continuous mapper. */
        @GridTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** Word queue. */
        private final Queue<String> words = new ConcurrentLinkedQueue<>();

        /** Total character count. */
        private final AtomicInteger totalChrCnt = new AtomicInteger(0);

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> grid, String phrase)
            throws GridException {
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
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws GridException {
            // If there is an error, fail-over to another node.
            if (res.getException() != null)
                return super.result(res, rcvd);

            // Add result to total character count.
            totalChrCnt.addAndGet(res.<Integer>getData());

            sendWord();

            // If next word was sent, keep waiting, otherwise work queue is empty and we reduce.
            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws GridException {
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
                mapper.send(new ComputeJobAdapter(word) {
                    @Override public Object execute() {
                        String word = argument(0);

                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' from grid job at time: " + new Date());

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
