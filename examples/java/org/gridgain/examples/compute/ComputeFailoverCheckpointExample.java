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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;

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
 *   it will run the {@link ComputeFailoverCheckpointExample#sayIt(CharSequence)} method.
 * </li>
 * </ol>
 * Note that when job throws an exception it will be treated as a failure, and the task
 * will return {@link GridComputeJobResultPolicy#FAILOVER} policy. This will
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
public class ComputeFailoverCheckpointExample {
    /** Path to configuration file. */
    private static final String CONFIG = "examples/config/example-checkpoint.xml";

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
            GridFuture<Integer> f = g.compute().apply(new CheckPointJob(), "Stage1 Stage2 Stage3");

            // Number of letters.
            int charCnt = f.get();

            System.out.println(">>>");

            if (charCnt < 0) {
                System.out.println(">>> \"Hello World\" checkpoint example finished with wrong result.");
                System.out.println(">>> Checkpoint was not found. Make sure that Checkpoint SPI on all nodes ");
                System.out.println(">>> has the same configuration (the 'directoryPath' configuration parameter for");
                System.out.println(">>> GridSharedFsCheckpointSpi on all nodes should point to the same location).");
            }
            else {
                System.out.println(">>> Finished executing fail-over example with checkpoints.");
                System.out.println(">>> Total number of characters in the phrase is '" + charCnt + "'.");
                System.out.println(">>> You should see exception stack trace from failed job on one node.");
                System.out.println(">>> Failed job will be failed over to another node.");
                System.out.println(">>> You should see print out of 'Hello World' on another node.");
                System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            }

            System.out.println(">>>");
        }
    }

    @GridComputeTaskSessionFullSupport
    private static final class CheckPointJob extends GridClosure<String, Integer>
        implements GridComputeJobMasterLeaveAware {
        /** Injected distributed task session. */
        @GridTaskSessionResource
        private GridComputeTaskSession jobSes;

        /** Injected grid logger. */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private GridBiTuple<Integer, Integer> state;

        /** */
        private String phrase;

        /**
         * The job will check the checkpoint with key '{@code fail}' and if
         * it's {@code true} it will throw exception to simulate a failure.
         * Otherwise, it will execute the grid-enabled method.
         */
        @Override public Integer apply(String phrase) {
            this.phrase = phrase;

            List<String> words = Arrays.asList(phrase.split(" "));

            final String cpKey = checkpointKey();

            try {
                GridBiTuple<Integer, Integer> state = jobSes.loadCheckpoint(cpKey);

                int idx = 0;
                int sum = 0;

                if (state != null) {
                    this.state = state;

                    // Last processed word index and total length.
                    idx = state.get1();
                    sum = state.get2();
                }

                for (int i = idx + 1; i < words.size(); i++) {
                    sum += words.get(i).length();

                    this.state = new GridBiTuple<>(i, sum);

                    // Save checkpoint with scope of task execution.
                    // It will be automatically removed when task completes.
                    jobSes.saveCheckpoint(cpKey, state);

                    // For example purposes, we fail on purpose after every stage.
                    throw new GridRuntimeException("Expected example job exception.");
                }

                return sum;
            }
            catch (GridException e) {
                throw new GridClosureException(e);
            }
        }

        /**
         * Callback for when master node fails or leaves.
         *
         * @param ses Task session, can be used for checkpoint saving.
         * @throws GridException If failed.
         */
        @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
            if (state != null)
                // Save checkpoint with global scope, so another task execution can pick it up.
                ses.saveCheckpoint(checkpointKey(), state, GridComputeTaskSessionScope.GLOBAL_SCOPE, 0);
        }

        /**
         * Make reasonably unique checkpoint key.
         *
         * @return Checkpoint key.
         */
        private String checkpointKey() {
            return getClass().getName() + '-' + phrase;
        }
    }
}
