package org.gridgain.examples.hpc.session;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with the use
 * of task session.
 * <p>
 * String "Hello World" is passed as an argument to
 * {@link GridCompute#execute(GridComputeTask, Object, long)} along with a
 * {@link org.gridgain.grid.compute.GridComputeTask} instance, that is distributed amongst the nodes. Task
 * on each of the participating nodes will do the following:
 * <ol>
 * <li>
 *      Set it's argument as a session attribute.
 * </li>
 * <li>
 *      Wait for other jobs to set their arguments as a session attributes.
 * </li>
 * <li>
 *      Concatenate all session attributes (which in this case are all job arguments)
 *      into one string and print it out.
 * </li>
 * </ol>
 * One of the potential outcomes could look as following:
 * <pre class="snippet">
 * All session attributes [ Hello World ]
 * >>>
 * >>> Printing 'World' on this node.
 * >>>
 * </pre>
 * <p>
 * Grid task {@link org.gridgain.grid.compute.GridComputeTaskSplitAdapter} handles actual splitting
 * into sub-jobs, remote execution, and result reduction (see {@link org.gridgain.grid.compute.GridComputeTask}).
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
public class GridSessionExample {
    /**
     * Method, that simply prints out the argument passed in and returns it's length.
     *
     * @param phrase Phrase string to print.
     * @return Number of characters in the phrase.
     */
    public static int sayIt(CharSequence phrase) {
        // Simply print out the argument.
        System.out.println(">>>");
        System.out.println(">>> Printing '" + phrase + "' on this node.");
        System.out.println(">>>");

        return phrase.length();
    }

    /**
     * Execute {@code HelloWorld} example with session attributes.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridComputeTask<String, Integer> task = new GridSessionExampleTask();

        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            GridComputeTaskFuture<Integer> f = g.compute().execute(task, "Hello World", 0);

            int phraseLen = f.get();

            System.out.println(">>>");
            System.out.println(">>> Finished executing \"Hello World\" example with task session.");
            System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
            System.out.println(">>> You should see print out of 'Hello' on one node and 'World' on another node.");
            System.out.println(">>> Afterwards all nodes should print out all attributes added to session.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            System.out.println(">>>");
        }
    }

    /**
     * Example task.
     * <p>
     * Note, that task is annotated with {@code @GridTaskSessionAttributesEnabled} since
     * it uses session attributes.
     */
    @GridComputeTaskSessionFullSupport
    private static class GridSessionExampleTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /** Grid task session is injected here. */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /**
         * Splits the passed in phrase into words and creates a job for every
         * word. Every job will print out full phrase, the word and return
         * number of letters in that word.
         *
         * @param gridSize Number of nodes in the grid.
         * @param arg Task execution argument.
         * @return Created grid jobs for remote execution.
         */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) {
            String[] words = arg.split(" ");

            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(words.length);

            for (String word : words) {
                jobs.add(new GridComputeJobAdapter(word) {
                    /** Job context will be injected. */
                    @GridJobContextResource
                    private GridComputeJobContext jobCtx;

                    /**
                     * Prints out all session attributes concatenated into string
                     * and runs {@link GridSessionExample#sayIt(CharSequence)}
                     * method passing a word from split arg.
                     */
                    @Override public Object execute() throws GridException {
                        String word = argument(0);

                        // Set session attribute with value of this job's word.
                        ses.setAttribute(jobCtx.getJobId(), word);

                        try {
                            // Wait for all other jobs within this task to set their attributes on
                            // the session.
                            for (GridComputeJobSibling sibling : ses.getJobSiblings()) {
                                // Waits for attribute with sibling's job ID as a key.
                                if (ses.waitForAttribute(sibling.getJobId()) == null)
                                    throw new GridException("Failed to get session attribute from job: " +
                                        sibling.getJobId());
                            }
                        }
                        catch (InterruptedException e) {
                            throw new GridException("Got interrupted while waiting for session attributes.", e);
                        }

                        // Create a string containing all attributes set by all jobs
                        // within this task (in this case an argument from every job).
                        StringBuilder msg = new StringBuilder();

                        // Formatting.
                        msg.append("All session attributes [ ");

                        for (Object jobArg : ses.getAttributes().values())
                            msg.append(jobArg).append(' ');

                        // Formatting.
                        msg.append(']');

                        // For the purpose of example, we simply log session attributes.
                        System.out.println(msg.toString());

                        return sayIt(word);
                    }
                });
            }

            return jobs;
        }

        /**
         * Sums up all characters from all jobs and returns a
         * total number of characters in the initial phrase.
         *
         * @param results Job results.
         * @return Number of letters for the word passed into
         *      {@link GridSessionExample#sayIt(CharSequence)}} method.
         */
        @Override public Integer reduce(List<GridComputeJobResult> results) {
            int sum = 0;

            for (GridComputeJobResult res : results)
                sum += res.<Integer>getData();

            return results.size() - 1 + sum;
        }
    }
}
