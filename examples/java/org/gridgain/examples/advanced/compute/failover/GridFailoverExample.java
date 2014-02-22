package org.gridgain.examples.advanced.compute.failover;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

/**
 * Demonstrates the usage of failover logic in GridGain.
 * <p>
 * Example may take configuration file as the only parameter.
 * <p>
 * String "Hello World" is passed as an argument to
 * {@link #sayIt(CharSequence)} method, which prints the phrase and returns it's length. The task, created
 * within the main method, responsible for split and
 * reduce logic will do the following:
 * <ol>
 * <li>Set session attribute '{@code fail=true}'.</li>
 * <li>Pass the passed in string as an argument into remote job for execution.</li>
 * <li>
 *   The job will check the value of '{@code fail}' attribute. If it
 *   is {@code true}, then it will set it to {@code false} and throw
 *   exception to simulate a failure. If it is {@code false}, then
 *   it will execute the job.
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
 * [02-Aug-2012 13:23:31][ERROR][gridgain-#3%null%][GridJobWorker] Failed to execute job
 * [jobId=d382850e041-11cd308d-bed8-4a4e-a2b9-6877dfa4b813, ses=GridJobSessionImpl
 * [ses=GridTaskSessionImpl [taskName=org.gridgain.examples.hpc.failover.GridFailoverExample$GridFailoverExampleTask,
 * dep=GridDeployment [ts=1378153411643, depMode=SHARED, clsLdr=sun.misc.Launcher$AppClassLoader@65fe28a7,
 * clsLdrId=9382850e041-11cd308d-bed8-4a4e-a2b9-6877dfa4b813, userVer=0, loc=true,
 * sampleClsName=org.gridgain.examples.hpc.failover.GridFailoverExample$GridFailoverExampleTask,
 * pendingUndeploy=false, undeployed=false, usage=2],
 * taskClsName=org.gridgain.examples.hpc.failover.GridFailoverExample$GridFailoverExampleTask,
 * sesId=8382850e041-11cd308d-bed8-4a4e-a2b9-6877dfa4b813, startTime=1378153411643, endTime=9223372036854775807,
 * taskNodeId=11cd308d-bed8-4a4e-a2b9-6877dfa4b813, clsLdr=sun.misc.Launcher$AppClassLoader@65fe28a7, closed=false,
 * topSpi=null, cpSpi=null, failSpi=null, loadSpi=null, nodeFilter=null, usage=2, fullSup=true],
 * jobId=d382850e041-11cd308d-bed8-4a4e-a2b9-6877dfa4b813]]
 * class org.gridgain.grid.GridException: Example job exception.
 *     at org.gridgain.examples.hpc.failover.GridFailoverExample$GridFailoverExampleTask$1.execute(GridFailoverExample.java:210)
 *     at org.gridgain.examples.hpc.failover.GridFailoverExample$GridFailoverExampleTask$1.execute(GridFailoverExample.java:181)
 *     at org.gridgain.grid.kernal.processors.job.GridJobWorker$2.call(GridJobWorker.java:473)
 *     at org.gridgain.grid.util.GridUtils.wrapThreadLoader(GridUtils.java:5518)
 *     at org.gridgain.grid.kernal.processors.job.GridJobWorker.execute0(GridJobWorker.java:467)
 *     at org.gridgain.grid.kernal.processors.job.GridJobWorker.body(GridJobWorker.java:420)
 *     at org.gridgain.grid.util.worker.GridWorker.run(GridWorker.java:139)
 *     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
 *     at java.lang.Thread.run(Thread.java:724)
 *
 * TASK MASTER NODE (where task was started and job was created and mapped)
 * [02-Aug-2012 18:04:15][WARN ][gridgain-#117%null%][GridAlwaysFailoverSpi] Failed over job to a new node [newNode=...]
 *
 * NODE #2 (node job has been failed over to)
 * >>>
 * >>> Printing 'Hello World' on this node.
 * >>>
 * </pre>
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should start remote grid instances.
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
public class GridFailoverExample {
    /** Job argument. */
    private static final String JOB_ARG = "Hello World";

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
     * Execute {@code HelloWorld} example with checkpoint.
     *
     * @param args Command line arguments, none required but user may
     *      set configuration file path as the only parameter.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            if (g.forRemotes().nodes().isEmpty()) {
                System.out.println(">>> This example requires one or more remote nodes to be in topology.");
                System.out.println(">>> Please start remote nodes and rerun the example.");
                System.out.println();

                return;
            }

            GridComputeTask<String, Integer> task = new GridFailoverExampleTask();

            GridComputeTaskFuture<Integer> f = g.compute().execute(task, JOB_ARG);

            int phraseLen = f.get();

            System.out.println(">>>");

            if (phraseLen < 0)
                throw new GridException("\"GridFailoverExample\" example finished with wrong result " +
                    "[result=" + phraseLen + ", expected=" + JOB_ARG.length() + ']');
            else {
                System.out.println(">>> Finished executing \"GridFailoverExample\".");
                System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
                System.out.println(">>> You should see exception stack trace from failed job on one node.");
                System.out.println(">>> Failed job should have been failed over to another node.");
                System.out.println(">>> You should see print out of '" + JOB_ARG + "' on another node.");
                System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            }

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
    private static class GridFailoverExampleTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /** Injected task session. */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /**
         * Creates job which throws an exception and it will be treated as a failure.
         * This will cause the job to automatically failover to another node for execution.
         * The new job will simply print out the argument passed in.
         *
         * @param gridSize Number of nodes in the grid.
         * @param words Task execution argument.
         * @return Created grid jobs for remote execution.
         * @throws GridException If split failed.
         */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String words)
        throws GridException {
            // Set initial value for 'fail' attribute to 'true'.
            ses.setAttribute("fail", true);

            // Return just one job here.
            return Collections.singletonList(new GridComputeJobAdapter(words) {
                /*
                * The job will check the 'fail' session attribute and if
                * it's 'true' it will throw exception to simulate a failure.
                * Otherwise, it will execute the grid-enabled method.
                */
                @Override public Serializable execute() throws GridException {
                    boolean fail;

                    try {
                        // Wait and get 'fail' attribute from session when it becomes available.
                        // In our example - we'll get it immediately since we set it up front
                        // in the 'split' method above.
                        fail = ses.waitForAttribute("fail");
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    // First time 'fail' attribute will be 'true' since
                    // that's what we initialized it to during 'split'.
                    if (fail) {
                        // Reset this attribute to 'false'.
                        // Next time we get this attribute we'll get 'false' value.
                        ses.setAttribute("fail", false);

                        // Throw exception to simulate error condition.
                        // The task 'result' method will see this exception
                        // and failover the job.
                        throw new GridException("Example job exception.");
                    }

                    return sayIt(this.<String>argument(0));
                }
            });
        }

        /**
         * To facilitate example's logic, returns {@link org.gridgain.grid.compute.GridComputeJobResultPolicy#FAILOVER}
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
