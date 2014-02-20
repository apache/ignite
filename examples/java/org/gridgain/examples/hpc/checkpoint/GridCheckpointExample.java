// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.checkpoint;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

/**
 * Demonstrates the usage of checkpoints in GridGain.
 * <p>
 * Example may take configuration file as the only parameter. By default it
 * uses sharedfs checkpoint implementation.
 * <p>
 * String "Hello World" is passed as an argument to
 * {@link #sayIt(CharSequence)} method, which prints the phrase and returns it's length. The task, created
 * within the main method, responsible for split and
 * reduce logic will do the following:
 * <ol>
 * <li>Save checkpoint with key '{@code fail}' and value '{@code true}'.</li>
 * <li>Pass the passed in string as an argument into remote job for execution.</li>
 * <li>
 *   The job will check the value of checkpoint with key '{@code fail}'. If it
 *   is {@code true}, then it will set it to {@code false} and throw
 *   exception to simulate a failure. If it is {@code false}, then
 *   it will run the {@link GridCheckpointExample#sayIt(CharSequence)} method.
 * </li>
 * </ol>
 * Note that when job throws an exception it will be treated as a failure, and the task
 * will return {@link org.gridgain.grid.compute.GridComputeJobResultPolicy#FAILOVER} policy. This will
 * cause the job to automatically failover to another node for execution.
 * The new job will simply print out the argument passed in.
 * <p>
 * The possible outcome will look as following:
 * <pre class="snippet">
 * NODE #1 (failure occurred on this node)
 * Exception:
 * ----------
 * [18:04:15] (omg) Failed to execute job [jobId=...]
 * class org.gridgain.grid.GridException: Example job exception.
 * at org.gridgain.examples1.hpc.checkpoint.GridCheckpointExample$1$1.execute(GridCheckpointExample.java:238)
 * at org.gridgain.examples1.hpc.checkpoint.GridCheckpointExample$1$1.execute(GridCheckpointExample.java:208)
 * at org.gridgain.grid.kernal.processors.job.GridJobWorker$2.call(GridJobWorker.java:448)
 * at org.gridgain.grid.util.GridUtils.wrapThreadLoader(GridUtils.java:5195)
 * at org.gridgain.grid.kernal.processors.job.GridJobWorker.execute0(GridJobWorker.java:446)
 * at org.gridgain.grid.kernal.processors.job.GridJobWorker.body(GridJobWorker.java:399)
 * at org.gridgain.grid.util.worker.GridWorker$1.run(GridWorker.java:145)
 * at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:441)
 * at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
 * at java.util.concurrent.FutureTask.run(FutureTask.java:138)
 * at org.gridgain.grid.util.worker.GridWorker.run(GridWorker.java:192)
 * at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
 * at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
 * at java.lang.Thread.run(Thread.java:662)
 *
 * NODE #2 (job was failed over to this node)
 * [02-Aug-2012 18:04:15][WARN ][gridgain-#117%null%][GridAlwaysFailoverSpi] Failed over job to a new node [newNode=...]
 * >>>
 * >>> Printing 'Hello World' on this node.
 * >>>
 * </pre>
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you need to start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-checkpoint.xml</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 * <p>
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCheckpointExample {
    /** Path to configuration file. */
    private static final String CONFIG = "examples/config/example-checkpoint.xml";

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
     * Executes example with checkpoint.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(CONFIG)) {
            GridComputeTask<String, Integer> task = new CheckPointExampleTask();

            GridComputeTaskFuture<Integer> f = g.compute().execute(task, "Hello World", 0);

            int phraseLen = f.get();

            System.out.println(">>>");

            if (phraseLen < 0) {
                System.out.println(">>> \"Hello World\" checkpoint example finished with wrong result.");
                System.out.println(">>> Checkpoint was not found. Make sure that Checkpoint SPI on all nodes ");
                System.out.println(">>> has the same configuration (the 'directoryPath' configuration parameter for");
                System.out.println(">>> GridSharedFsCheckpointSpi on all nodes should point to the same location).");
            }
            else {
                System.out.println(">>> Finished executing \"Hello World\" example with checkpoints.");
                System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
                System.out.println(">>> You should see exception stack trace from failed job on one node.");
                System.out.println(">>> Failed job will be failed over to another node.");
                System.out.println(">>> You should see print out of 'Hello World' on another node.");
                System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            }

            System.out.println(">>>");
        }
    }

    /**
     * Example task.
     */
    @GridComputeTaskSessionFullSupport
    private static class CheckPointExampleTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /** Injected task session. */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /**
         * Creates job which throws an exception and it will be treated as a failure.
         * This will cause the job to automatically failover to another node for execution.
         * The new job will simply print out the argument passed in.
         *
         * @param gridSize Number of nodes in the grid.
         * @param phrase Task execution argument.
         * @return Created grid jobs for remote execution.
         * @throws GridException If split failed.
         */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String phrase)
            throws GridException {
            // Make reasonably unique checkpoint key.
            final String cpKey = getClass().getName() + phrase;

            taskSes.saveCheckpoint(cpKey, true);

            return Collections.singletonList(new GridComputeJobAdapter(phrase) {
                /** Injected distributed task session. */
                @GridTaskSessionResource
                private GridComputeTaskSession jobSes;

                /** Injected grid logger. */
                @GridLoggerResource
                private GridLogger log;

                /**
                 * The job will check the checkpoint with key '{@code fail}' and if
                 * it's {@code true} it will throw exception to simulate a failure.
                 * Otherwise, it will execute the grid-enabled method.
                 */
                @Override public Serializable execute() throws GridException {
                    Serializable cp = jobSes.loadCheckpoint(cpKey);

                    if (cp == null) {
                        log.warning("Checkpoint was not found. Make sure that Checkpoint SPI on all nodes " +
                            "has the same configuration. The 'directoryPath' configuration parameter for " +
                            "GridSharedFsCheckpointSpi on all nodes should point to the same location.");

                        return -1;
                    }

                    boolean fail = (Boolean)cp;

                    if (fail) {
                        jobSes.saveCheckpoint(cpKey, false);

                        throw new GridException("Expected Example job exception.");
                    }

                    return sayIt(this.<String>argument(0));
                }
            });
        }

        /**
         * To facilitate example's logic, returns {@link GridComputeJobResultPolicy#FAILOVER}
         * policy in case of any exception.
         *
         * @param res Job result.
         * @param rcvd All previously received results.
         * @return {@inheritDoc}
         */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) {
            return res.getException() != null ? GridComputeJobResultPolicy.FAILOVER : GridComputeJobResultPolicy.WAIT;
        }

        /**
         * Sums up all characters from all jobs and returns a
         * total number of characters in the initial phrase.
         *
         * @param results Job results.
         * @return Number of letters for the phrase.
         */
        @Override public Integer reduce(List<GridComputeJobResult> results) {
            // We only had one job in the split. Therefore, we only have one result.
            return  results.get(0).getData();
        }
    }
}
